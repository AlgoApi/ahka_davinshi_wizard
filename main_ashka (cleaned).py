import traceback
import signal
import aiofiles
from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest
from typing import Optional, Tuple
from telethon.tl.types import InputPeerChannel
from telethon.errors import SessionPasswordNeededError, DataInvalidError
from telethon.tl.functions.messages import CreateChatRequest
from telethon.tl.types import MessageEntityMention, MessageEntityTextUrl, MessageEntityMentionName, Message
from config import BB_TIME, MESSAGES_FILE, CHAT_ID_ADMIN, PRELAUNCH_MESSAGE_DELAY, \
    SYNONYMS_FILE, ENVELOPE_TIME_BEFORE_SEND_MESSAGE, MAX_ENVELOPE_MESSAGES_ALL_SESSIONS, ENVELOPE_EMOJI, MAX_LIMIT, \
    CHAT_ID, CHAT_ID_rezerv, BOT_TOKEN
from config import WAKEUP_AFTER_NIGHT
from telethon.tl.types import ReplyInlineMarkup, ReplyKeyboardMarkup
import json, os, random, logging, asyncio, sys, requests, time
from telethon import errors
import datetime
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.tl.types import Channel, Chat, InputPeerChannel, InputPeerChat
import json
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, func, select
import re

# Regex patterns for inline mentions and t.me links
USERNAME_PATTERN = re.compile(r'@([A-Za-z0-9_]{5,32})')
TME_LINK_PATTERN = re.compile(r'(?<=\()(?:https?://)?t\.me/([A-Za-z0-9_]{5,32})(?=\))')

DATABASE_DSN = "postgresql+asyncpg://log:pass@ip:port/db_name"
DATABASE_DSN2 = "postgresql://log:pass@ip:port/db_name"

# ========== Глобальный shutdown_event ==========
shutdown_event = asyncio.Event()

# SQLAlchemy async setup
Base = declarative_base()
engine = create_async_engine(DATABASE_DSN, echo=False)
# Create async session factory (without expiration to avoid refetching objects on commit)
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Models
default_now = func.now()

class Account(Base):
    __tablename__ = 'Accounts'
    Id = Column(Integer, primary_key=True, index=True)
    #AgentId = Column(Integer, ForeignKey('Agents.Id'), nullable=True)
    LastActive = Column(DateTime, default=default_now)
    Phone = Column(String, unique=True, index=True)
    Username = Column(String, unique=True, index=True)
    # Добавляем связи к упоминаниям без изменения структуры БД
    mentions_a = relationship('MentionA', back_populates='Account', lazy='selectin')
    mentions_b = relationship('MentionB', back_populates='Account', lazy='selectin')

class MentionA(Base):
    __tablename__ = 'mentions_a'
    Id = Column(Integer, primary_key=True, index=True)
    AccountId = Column(Integer, ForeignKey('Accounts.Id'), nullable=False)
    Username = Column(String, index=True)
    CreatedAt = Column(DateTime, default=default_now)
    Account = relationship('Account', back_populates='mentions_a')

class MentionB(Base):
    __tablename__ = 'mentions_b'
    Id = Column(Integer, primary_key=True, index=True)
    AccountId = Column(Integer, ForeignKey('Accounts.Id'), nullable=False)
    Username = Column(String, index=True)
    CreatedAt = Column(DateTime, default=default_now)
    Account = relationship('Account', back_populates='mentions_b')


async def init_db():
    """Create tables if they do not exist"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

def _on_exit():
    """SIGINT/SIGTERM handler"""
    shutdown_event.set()

LOG_FILE = './log/main_ashka.log'
SESSIONS_DIR = './sessions_checker/'

import asyncpg

async def get_group_id(pool: asyncpg.Pool, phone_number: str) -> Optional[Tuple[int, int]]:
    """
    Если для phone_number в таблице есть запись — вернёт кортеж (group_id, access_hash),
    иначе — None.
    """
    row = await pool.fetchrow(
        "SELECT group_id, access_hash FROM tg_groups_ashqua WHERE phone_number = $1",
        phone_number
    )
    if not row:
        return None
    return row['group_id'], row['access_hash']

async def set_group_id(pool: asyncpg.Pool, phone_number: str, group_id: int, access_hash: int):
    """
    Вставляет новую запись или обновляет старую по conflict по phone_number,
    сохраняя и group_id, и access_hash.
    """
    await pool.execute("""
        INSERT INTO tg_groups_ashqua (phone_number, group_id, access_hash)
        VALUES ($1, $2, $3)
        ON CONFLICT (phone_number) DO
          UPDATE SET
            group_id     = EXCLUDED.group_id,
            access_hash  = EXCLUDED.access_hash
    """, phone_number, group_id, access_hash)

async def find_group(client: TelegramClient, group_title: str):
    # Получаем последние 100 диалогов
    await asyncio.sleep(2)
    dialogs = await client.get_dialogs(limit=25)
    for dlg in dialogs:
        # dlg.is_group будет True для «малых групп» (Chat) и для «мега‑групп» (Channel с флагом is_channel=True)
        if dlg.title == group_title and dlg.is_group:
            return dlg.entity
    return None

async def get_or_create_group(client: TelegramClient, group_title: str, pool: asyncpg.Pool, phone_number: str):
    """
    Если группа с названием group_title существует, возвращает её entity.
    Если нет, создает новую группу с указанным названием и возвращает созданную группу.

    participant_usernames должен содержать список username участников, которых требуется добавить.
    (Помните, что для создания группы требуется минимум 2 участника.)
    """

    # 1) пробуем сразу по сохранённому group_id
    info_group = await get_group_id(pool, phone_number)
    if info_group and 3 > len(info_group) > 1:
        gid, ah = info_group
        if gid and ah:
            try:
                # 1) Строим InputPeerChannel
                logger.info("Строим InputPeerChannel")
                peer = InputPeerChannel(channel_id=gid, access_hash=ah)
                # 2) Получаем entity группы
                grp = await client.get_entity(peer)
                logger.info(peer)
                logger.info("нашёл группу из бд InputPeerChannel")
                return grp
            except Exception as e:
                logger.error(f"{phone_number} ошибка получения get_entity или InputPeerChannel группы для взаимок с бд, "
                             f"продолжаем дальше\n{e}\n\ngid: {gid}\naccess_hash: {ah}")
                # если вдруг уже удалили/сменили — сбросим и пойдём дальше
                await pool.execute("DELETE FROM tg_groups_ashqua WHERE phone_number = $1", phone_number)
        elif gid:
            try:
                # 1) Строим InputPeerChat
                logger.info("Строим InputPeerChat")
                # 2) Получаем entity группы
                # given gid = chat_id
                full = await client(GetFullChatRequest(chat_id=gid))
                # the “chats” array contains one Chat object
                grp = full.chats[0]
                logger.info("нашёл группу из бд InputPeerChat")
                #logger.info(peer)
                if grp is None:
                    raise RuntimeError(f"full.chats[0] is none")
                if not grp:
                    raise RuntimeError(f"if not full.chats[0]")
                return grp
            except Exception as e:
                logger.error(f"{phone_number} ошибка получения get_entity или InputPeerChat группы для взаимок с бд, "
                             f"продолжаем дальше\n{e}\n\ngid: {gid}\naccess_hash: {ah}")
                # если вдруг уже удалили/сменили — сбросим и пойдём дальше
                await pool.execute("DELETE FROM tg_groups_ashqua WHERE phone_number = $1", phone_number)
        else:
            logger.warning(f"{phone_number} ошибка получения id и access_hash = None группы для взаимок с бд, "
                           f"продолжаем дальше\ninfo_group: {info_group}")
            await pool.execute("DELETE FROM tg_groups_ashqua WHERE phone_number = $1", phone_number)
    else:
        logger.warning(f"{phone_number} ошибка получения info_group (id и access_hash) группы для взаимок с бд, "
                       f"продолжаем дальше\ninfo_group: {info_group}")
        await pool.execute("DELETE FROM tg_groups_ashqua WHERE phone_number = $1", phone_number)

    # 2) ищем в последних группах (быстро) по title
    group = await find_group(client, group_title)
    if group:
        # …сохраняем в БД и возвращаем
        if isinstance(group, Channel):
            ah = group.access_hash
        else:
            # Chat у Telethon действительно не имеет access_hash
            ah = 0
        await set_group_id(pool, phone_number, group.id, ah)
        logger.info("нашёл группу")
        return group

    # Получаем объекты участников
    participants = [await get_or_start_bot(client, "Bop4k_bot")]

    while True:
        try:
            await asyncio.sleep(1)
            result = await client(CreateChatRequest(
                users=participants,
                title=group_title
            ))

            try:
                created = result.updates.chats[0]
                if isinstance(created, Channel):
                    ah = created.access_hash
                else:
                    # Chat у Telethon действительно не имеет access_hash
                    ah = 0
                await set_group_id(pool, phone_number, created.id, ah)
                logger.info("создали новою группу")
                return created
            except Exception:
                # либо нет поля .updates, либо .updates.chats пуст
                pass

            await asyncio.sleep(2)
            new_group = await find_group(client, group_title)
            if new_group:
                # …сохраняем в БД и возвращаем
                if isinstance(new_group, Channel):
                    ah = new_group.access_hash
                else:
                    # Chat у Telethon действительно не имеет access_hash
                    ah = 0
                await set_group_id(pool, phone_number, new_group.id, ah)
                logger.info("создали новою группу")
                return new_group

        except errors.FloodWaitError as e:
            logger.warning("ФЛУД получения новой группы ждём")
            await asyncio.sleep(e.seconds + 1)
            # после сна ещё раз пробуем найти в недавних группах
            group = await find_group(client, group_title)
            if group:
                if isinstance(group, Channel):
                    ah = group.access_hash
                else:
                    # Chat у Telethon действительно не имеет access_hash
                    ah = 0
                # …сохраняем в БД и возвращаем
                await set_group_id(pool, phone_number, group.id, ah)
                return group

        except errors.RPCError:
            logger.error("Ошибка создания и получения новой группы")
            return None

async def get_or_start_bot(client: TelegramClient, bot_username: str):
    """
    Автоматически получает объект бота.
    Если диалог с ботом ещё не начат, отправляет ему команду /start,
    ждёт некоторое время и повторно запрашивает его объект.
    """
    try:
        bot_entity = await client.get_entity(bot_username)
        logger.info(f"Бот {bot_username} уже в контактах.")
        return bot_entity
    except errors.UsernameNotOccupiedError:
        # Если бот не найден, можно попробовать начать с ним диалог
        logger.warning(f"Бот {bot_username} не найден в контактах. Попытка начать диалог.")
    except errors.FloodWaitError as e:
        logger.warning(f"Flood wait error: подождите {e.seconds} секунд.")
        await asyncio.sleep(e.seconds)
        return await get_or_start_bot(client, bot_username)
    except Exception as e:
        logger.error(f"Ошибка при получении бота: {e}")

    # Если бот не найден, попробуем отправить ему команду /start
    try:
        # При отправке сообщения, если диалог отсутствует, он автоматически создаётся
        await client.send_message(bot_username, '/start')
        logger.info("Команда /start отправлена боту.")
        # Небольшая задержка для обработки сообщения ботом и обновления данных
        await asyncio.sleep(1)
        # Повторно пытаемся получить объект бота
        bot_entity = await client.get_entity(bot_username)
        logger.info(f"Бот {bot_username} успешно получен.")
        return bot_entity
    except Exception as e:
        logger.error(f"Не удалось получить бота после отправки /start: {e}")


def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID_ADMIN, "text": text}
    response = requests.post(url, data=data)
    if response.status_code != 200:
        logger.warning(f"Ошибка отправки сообщения в Telegram: {response.status_code}")


class TelegramLogHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        if record.levelno >= logging.WARNING:
            send_telegram_message(log_entry)


class CustomFormatter(logging.Formatter):
    def format(self, record):
        record.msg = record.msg.encode('utf-8', 'replace').decode('utf-8')
        return super().format(record)


formatter = CustomFormatter(
    fmt='[%(asctime)s - %(levelname)s] - %(message)s',
    datefmt='%d-%m-%Y-%H:%M:%S'
)

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)

logger = logging.getLogger(__name__)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

telegram_handler = TelegramLogHandler()
telegram_handler.setFormatter(formatter)
logger.addHandler(telegram_handler)

observing_modes = {}
like_process = {}
verif_errors = {}
emoji_errors = {}
vzaimok = 0
vzaimok_formal = 0

def extract_inline_buttons(reply_markup):
    """Возвращает список inline-кнопок (текст) из reply_markup, если он является inline-клавиатурой."""
    if not reply_markup or not isinstance(reply_markup, ReplyInlineMarkup):
        return []
    buttons = []
    for row in reply_markup.rows:
        for button in row.buttons:
            buttons.append(button.text)
    return buttons

def extract_reply_buttons(reply_markup):
    """Возвращает список reply-кнопок (текст) из reply_markup, если он является reply-клавиатурой."""
    if not reply_markup or not isinstance(reply_markup, ReplyKeyboardMarkup):
        return []
    buttons = []
    # У reply-клавиатуры кнопки хранятся в атрибуте "rows" (каждая строка – список кнопок)
    for row in reply_markup.rows:
        for button in row.buttons:
            buttons.append(button.text)
    return buttons

async def process_buttons(client, bot_entity):
    """
    Получает последнее сообщение от бота, пытается обработать inline и reply кнопки.
    Если под последним сообщением не найдена клавиатура, ищет в старых сообщениях отдельно inline и reply клавиатуры.
    """
    # Получаем последнее сообщение от бота
    messages = await client.get_messages(bot_entity, limit=1)
    if not messages:
        logger.error("Нет сообщений от бота")
        return

    message = messages[0]
    reply_markup = message.reply_markup

    if reply_markup:
        # Если клавиатура найдена, пытаемся различить inline и reply
        inline_buttons = extract_inline_buttons(reply_markup)
        reply_buttons = extract_reply_buttons(reply_markup)
        if inline_buttons:
            logger.info("Inline-кнопки под сообщением: %s", inline_buttons)
            # Пример: нажимаем первую inline-кнопку
            await message.click(0, 0)
        if reply_buttons:
            logger.info("Reply-кнопки под сообщением: %s", reply_buttons)
            # Пример: отправляем текст, равный первой reply-кнопке, чтобы имитировать нажатие
            await client.send_message(bot_entity, reply_buttons[0])
    else:
        logger.info("Клавиатура не найдена под последним сообщением. Поиск в старых сообщениях...")
        # Ищем отдельно inline и reply клавиатуры в старых сообщениях
        inline_found = None
        reply_found = None
        i = 1
        while not inline_found and not reply_found:
            msgs = await client.get_messages(bot_entity, limit=i)
            if not msgs:
                break
            last_msg = msgs[-1]
            rm = last_msg.reply_markup
            if rm:
                if not inline_found and isinstance(rm, ReplyInlineMarkup):
                    inline_found = last_msg
                if not reply_found and isinstance(rm, ReplyKeyboardMarkup):
                    reply_found = last_msg
            i += 1
            if i > 10:
                # Ограничим поиск первыми 10 сообщениями
                break
        if inline_found:
            inline_buttons = extract_inline_buttons(inline_found.reply_markup)
            logger.info("Найдены inline-кнопки в старом сообщении: %s", inline_buttons)
            await inline_found.click(0, 0)
        else:
            logger.info("Inline-клавиатура не найдена в старых сообщениях.")
        if reply_found:
            reply_buttons = extract_reply_buttons(reply_found.reply_markup)
            logger.info("Найдены reply-кнопки в старом сообщении: %s", reply_buttons)
            await client.send_message(bot_entity, reply_buttons[0])
        else:
            logger.info("Reply-клавиатура не найдена в старых сообщениях.")



def load_session_config(phone, informal_contact=True):
    session_path = os.path.join(SESSIONS_DIR, f'{phone}.json' if informal_contact else f'formal_contact/{phone}.json')
    if os.path.exists(session_path):
        with open(session_path, 'r') as file:
            return json.load(file)
    else:
        logger.error(f"Файл конфигурации {session_path} не найден!")
        return None


def load_messages(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return [line.strip() for line in file.readlines()]


def generate_random_message(messages, synonyms):
    random_message = random.choice(messages)
    random_word = random.choice(synonyms)
    return f"{random_message} {random_word}"


def generate_text_keyboard(keyboard):
    buttons = keyboard.rows
    result = []
    for row in buttons:
        for button in row.buttons:
            result.append(button.text)
    return result


async def like_people(phone, client, session_count, session_count_formal_contact, new_like_func, informal_contact=True):
    global observing_modes, like_process
    logger.info(f"[{phone}] Запуск функции like_people")
    staying_alive = False
    iterrations = 1
    buttons_not_found = 0
    buttons_not_found_2 = 0
    count_sended_envelope = 0
    count_dislike = 0
    errors_conv = 0

    observing_modes[phone] = False
    like_process[phone] = True
    generated_message = load_messages(MESSAGES_FILE)
    generated_synonym = load_messages(SYNONYMS_FILE)

    like_sended = False

    reply_keyboard = None
    last_idd = None
    message_is_null = 0

    while True:
        while iterrations <= MAX_LIMIT and MAX_LIMIT != 0 and not observing_modes[phone]:
            try:
                logger.info(f"[{phone}] ИТЕРАЦИЯ #{iterrations}")
                bot = await client.get_entity('ashqua_bot')
                await asyncio.sleep(5)

                if last_idd is None:
                    messages = await client.get_messages(bot, limit=1)
                else:
                    messages = await client.get_messages(bot, limit=1, offset_id=last_idd)
                if messages:
                    msg_tempp = messages[0]
                    last_idd = msg_tempp.id

                if not messages:
                    if message_is_null > 5:
                        logger.error(f"{phone} Ошибка 5 раз не найдено последнее сообения, выход из цикла")
                        iterrations += 999
                    message_is_null += 1
                    last_idd = None
                    continue

                if messages is None or len(messages) < 1:
                    if message_is_null > 5:
                        logger.error(f"{phone} Ошибка 5 раз не найдено последнее сообения, выход из цикла")
                        iterrations += 999
                    message_is_null += 1
                    last_idd = None
                    continue

                if "Слишком много" in messages[0].message:
                    observing_modes[phone] = True
                    break

                if "Неверный формат" in messages[0].message:
                    await client.send_message(bot, "20")
                    await asyncio.sleep(1)
                    await client.send_message(bot, "🔎 Лента")
                    continue
                if "Твой возраст" in messages[0].message:
                    await client.send_message(bot, "20")
                    await asyncio.sleep(1)
                    await client.send_message(bot, "🔎 Лента")
                    continue

                if "Лайков пока нету ❤️‍🩹" in messages[0].message:
                    await asyncio.sleep(1)
                    await client.send_message(bot, "👤 Профиль")
                    continue

                if "Недостаточно супер-лайков" in messages[0].message:
                    await asyncio.sleep(1)
                    await client.send_message(bot, "👤 Профиль")
                    await asyncio.sleep(1)
                    await client.send_message(bot, "🔎 Лента")
                    continue

                if "💡 В нашем боте доступна верификация" in messages[0].message:
                    await asyncio.sleep(1)
                    await client.send_message(bot, "👤 Профиль")
                    await asyncio.sleep(1)
                    await client.send_message(bot, "🔎 Лента")
                    continue

                if "💡 В боте можно установить" in messages[0].message:
                    await asyncio.sleep(1)
                    await client.send_message(bot, "👤 Профиль")
                    await asyncio.sleep(1)
                    await client.send_message(bot, "🔎 Лента")
                    continue
                if "Плюс статус" in messages[0].message:
                    await asyncio.sleep(1)
                    await client.send_message(bot, "👤 Профиль")
                    await asyncio.sleep(1)
                    await client.send_message(bot, "🔎 Лента")
                    continue
                if "💳 " in messages[0].message:
                    await asyncio.sleep(1)
                    await client.send_message(bot, "👤 Профиль")
                    await asyncio.sleep(1)
                    await client.send_message(bot, "🔎 Лента")
                    continue
                if buttons_not_found >= 3:
                    await asyncio.sleep(1)
                    # Если inline-кнопки не найдены, можно попытаться кликнуть по сообщению
                    await messages[0].click()
                elif buttons_not_found >= 5:
                    logger.warning(f"[{phone}] Клавиатура НЕ найдена 5 раз!")
                    await asyncio.sleep(1)
                    await client.send_message(bot, "/profile")
                    await asyncio.sleep(1)
                    await messages[0].click()
                elif buttons_not_found >= 7:
                    logger.warning(f"[{phone}] Клавиатура НЕ найдена 7 раз!")
                    await asyncio.sleep(1)
                    await client.send_message(bot, "/profile")
                    await asyncio.sleep(1)
                    messages = await client.get_messages(bot, limit=1)
                    await messages[0].click()
                elif buttons_not_found >= 9:
                    logger.error(f"[{phone}] Клавиатура НЕ найдена 9 РАЗ!")
                    await asyncio.sleep(1)
                    buttons_not_found = 0
                    await messages[0].click()


                i = 1
                reply_markup = None
                while True:
                    await asyncio.sleep(1)
                    messages = await client.get_messages(bot, limit=i)
                    reply_markup_temp = messages[-1].reply_markup
                    if isinstance(reply_markup_temp, ReplyInlineMarkup):
                        if reply_markup is None:
                            reply_markup = reply_markup_temp

                    reply_keyboard_temp = reply_keyboard
                    reply_keyboard = None

                    if isinstance(reply_markup_temp, ReplyKeyboardMarkup):
                        reply_keyboard = reply_markup_temp
                    if (not reply_markup and not isinstance(reply_markup, ReplyInlineMarkup)) or not reply_keyboard:
                        i += 1
                    else:
                        break
                    if i > 4 and reply_keyboard_temp is not None:
                        reply_keyboard = reply_keyboard_temp
                    if 10 < i < 12 and reply_keyboard is None and reply_markup is not None:
                        await client.send_message(bot, "👤 Профиль")


                keyboard_text = generate_text_keyboard(reply_markup)
                keyboard_reply_text = generate_text_keyboard(reply_keyboard)
                if reply_markup:
                    logger.info(f"[{phone}] Клавиатура найдена: {keyboard_text}")
                if "Забрать награду" in keyboard_text:
                    try:
                        await messages[0].click(0, 0)
                        await asyncio.sleep(2)
                        await client.send_message(bot, "🔎 Лента")
                    except Exception:
                        try:
                            await messages[0].click(1, 1)
                            await asyncio.sleep(2)
                            await client.send_message(bot, "🔎 Лента")
                        except Exception:
                            pass
                found = False
                buttons = reply_markup.rows

                await asyncio.sleep(3)

                # Пример для кнопки "➡️" – ищем и кликаем её
                found_global = False
                if 'Посмотреть' in keyboard_text:
                    for row_idx, row in enumerate(buttons):
                        for btn_idx, button in enumerate(row.buttons):
                            if button.text == 'Посмотреть':
                                #await messages[0].click(row_idx, btn_idx)
                                await client.send_message(bot, "🔎 Лента")
                                await asyncio.sleep(1)
                                found = True
                                break
                        if found:
                            found_global = True
                            break
                if 'Редактировать профиль' in keyboard_text:
                    await client.send_message(bot, "🔎 Лента")
                    await asyncio.sleep(1)
                    found_global = True


                if found_global:
                    continue
                if '🔎 Лента' in keyboard_reply_text:
                    await client.send_message(bot, "🔎 Лента")
                    continue

                like_found = False
                # Перебор кнопок для других вариантов
                await asyncio.sleep(ENVELOPE_TIME_BEFORE_SEND_MESSAGE)
                for row_idx, row in enumerate(buttons):
                    if like_found:
                        break
                    for btn_idx, button in enumerate(row.buttons):
                        if like_found:
                            break
                        #if button.text == "1 🚀":
                        #    await messages[0].click(row_idx, btn_idx)
                        #    await asyncio.sleep(3)
                        #    found = True
                        #    break
                        if button.text == "Ладно":
                            await messages[0].click(row_idx, btn_idx)
                            await asyncio.sleep(3)
                            found = True
                            break
                        if "Купить плюс" in button.text:
                            await asyncio.sleep(1)
                            await client.send_message(bot, "👤 Профиль")
                            await asyncio.sleep(1)
                            await client.send_message(bot, "🔎 Лента")
                            found = True
                            break
                        if "Купить премиум" in button.text:
                            await asyncio.sleep(1)
                            await client.send_message(bot, "👤 Профиль")
                            await asyncio.sleep(1)
                            await client.send_message(bot, "🔎 Лента")
                            found = True
                            break
                        if "Перейти к оплате" in button.text:
                            await asyncio.sleep(1)
                            await client.send_message(bot, "👤 Профиль")
                            await asyncio.sleep(1)
                            await client.send_message(bot, "🔎 Лента")
                            found = True
                            break
                        if button.text == '✖️':
                            await messages[0].click(row_idx, btn_idx)
                            await asyncio.sleep(3)
                            found = True
                            break
                        if "Пожаловаться" in button.text:
                            try:
                                # Если есть второй блок сообщений, пытаемся кликнуть там
                                await messages[1].click(row_idx, btn_idx)
                                await asyncio.sleep(2)
                                found = True
                                break
                            except Exception:
                                await messages[0].click(row_idx, btn_idx)
                                await asyncio.sleep(2)
                                found = True
                                break
                        # Если количество строк > 1 или кнопок меньше 3, кликаем по найденной кнопке
                        #if len(buttons) > 1 or len(row.buttons) < 3:
                        #    await messages[0].click(row_idx, btn_idx)
                        #    await asyncio.sleep(2)
                        #    found = True
                        #    break

                        # Если отправлено достаточно конвертов – отправляем лайк/дизлайк через нажатие кнопок
                        if count_sended_envelope >= MAX_ENVELOPE_MESSAGES_ALL_SESSIONS:
                            try:
                                if count_dislike == 0:

                                    random_like = random.randint(0, 1)
                                    if not like_sended:
                                        # Ищем кнопку с нужным смайлом и кликаем по ней
                                        if button.text in ["❤️", "👍"]:
                                            try:
                                                await messages[0].click(row_idx, btn_idx)
                                                logger.info(f"[{phone}] Отправлен лайк после {count_sended_envelope} конвертов.")
                                                await asyncio.sleep(5)
                                                like_found = True
                                                like_sended = True
                                                found = True
                                                #await asyncio.sleep(5)
                                                break
                                            except (DataInvalidError, IndexError):
                                                await messages[0].click(0, 0)
                                                logger.info(f"[{phone}] Принудительно Нажата кнопка 0 0")
                                                logger.info(
                                                    f"[{phone}] Отправлен лайк после {count_sended_envelope} конвертов.")
                                                await asyncio.sleep(5)
                                                like_found = True
                                                like_sended = True
                                                found = True
                                                # await asyncio.sleep(5)
                                                break


                                    if button.text == "🤮":
                                        try:
                                            await messages[0].click(row_idx, btn_idx)
                                            logger.info(f"[{phone}] Отправлен дизлайк после {count_sended_envelope} конвертов и лайка.")
                                            like_sended = False
                                            found = True
                                            like_found = True

                                            count_dislike += 1
                                            count_sended_envelope = 0
                                            iterrations += 1
                                            await asyncio.sleep(1)
                                            break
                                        except (DataInvalidError, IndexError):
                                            await messages[0].click(0, 4)
                                            logger.info(f"[{phone}] Принудительно Нажата кнопка 0 4")
                                            logger.info(
                                                f"[{phone}] Отправлен лайк после {count_sended_envelope} конвертов.")
                                            await asyncio.sleep(5)
                                            like_found = True
                                            like_sended = True
                                            found = True
                                            # await asyncio.sleep(5)
                                            break
                                    else:
                                        continue
                            except Exception:
                                found = True
                                like_found = True
                                break

                        # Если кнопка содержит эмодзи для отправки конверта
                        if any(char in button.text for char in ENVELOPE_EMOJI if char.strip()):
                            await asyncio.sleep(1)
                            found = True
                            like_found = True
                            if informal_contact:
                                try:
                                    await messages[0].click(row_idx, btn_idx)
                                    logger.info(f"[{phone}] Нажата кнопка {button.text}")
                                except (DataInvalidError, IndexError):
                                    await messages[0].click(0, 1)
                                    logger.info(f"[{phone}] Принудительно Нажата кнопка 0 1")
                                messages2 = await client.get_messages(bot, limit=1)
                                if "Недостаточно супер-лайков 💔" not in messages2[0].message:
                                    random_message = generate_random_message(generated_message, generated_synonym)
                                    await asyncio.sleep(1)
                                    envelope_time_random = random.randint(ENVELOPE_TIME_BEFORE_SEND_MESSAGE,
                                                                          ENVELOPE_TIME_BEFORE_SEND_MESSAGE + 20)
                                    logger.info(f"[{phone}] Рандомная задержка перед отправкой сообщения: {envelope_time_random} секунд")
                                    await asyncio.sleep(envelope_time_random)
                                    # Если нужно, можно предусмотреть отдельный inline-интерфейс для ввода сообщения
                                    await client.send_message(bot, random_message)
                                    logger.info(f"[{phone}] Отправлено сообщение: {random_message}")
                                    await asyncio.sleep(1)
                            else:
                                random_like = random.randint(0, 1)
                                await asyncio.sleep(5)
                                for i, r in enumerate(buttons):
                                    for j, b in enumerate(r.buttons):
                                        if b.text in ["❤️", "👍"]:
                                            await messages[0].click(i, j)
                                            break
                                    else:
                                        continue
                                    break
                            count_sended_envelope += 1
                            count_dislike = 0
                            break
                    if found:
                        buttons_not_found = 0
                        buttons_not_found_2 = 0
                        break

                if not found and buttons_not_found_2 < 4:
                    if "Купить плюс" in keyboard_text:
                        await asyncio.sleep(1)
                        await client.send_message(bot, "👤 Профиль")
                        await asyncio.sleep(1)
                        await client.send_message(bot, "🔎 Лента")
                        found = True

                    else:
                        logger.info(f"[{phone}] ({keyboard_text}) Не удалось нажать ни на одну кнопку, нажимаем на первую")
                        # Если не удалось найти нужную кнопку – кликаем по первой найденной
                        try:
                            await messages[0].click(0 + buttons_not_found_2, 0)
                        except Exception:
                            try:
                                await messages[0].click(0, 1)
                            except Exception:
                                await messages[0].click(0, 0)
                    buttons_not_found_2 += 1

                    await asyncio.sleep(1)
                elif not found and buttons_not_found_2 >= 3:
                    try:
                        logger.info(f"[{phone}] ({keyboard_text}) Не удалось нажать кнопку 3 раза, выполняем сброс через /start")
                        buttons_not_found_2 = 0
                        await client.send_message(bot, "/start")
                        await asyncio.sleep(1)
                        await client.send_message(bot, "🔎 Лента")
                        await asyncio.sleep(1)
                        try:
                            await messages[0].click(0, 2)
                        except Exception:
                            pass
                        await asyncio.sleep(1)
                    except Exception as e:
                        logger.info(f"[{phone}] Ошибка при попытке сброса: {e}")
                        break

                await asyncio.sleep(3)

                errors_conv = 0

            except Exception as e:
                errors_conv += 1
                formatted_traceback = ''.join(traceback.format_exception(*sys.exc_info()))
                logger.warning(f"{formatted_traceback}")
                logger.warning(f"[{phone}] Ошибка в цикле like_people, продолжаем попытки: {e}")
                try:
                    await client.connect()
                except Exception as e:
                    logger.warning(f"[{phone}] Ошибка client.connect: {e}")
                if errors_conv >= 3:
                    logger.info(f"[{phone}] Слишком много ошибок, переходим в режим наблюдения")
                    break

        if iterrations > MAX_LIMIT:
            observing_modes[phone] = True
            like_process[phone] = False
            bot = await client.get_entity('ashqua_bot')
            await client.send_message(bot, "/likes")
            await new_like_func()

        while observing_modes[phone] and not shutdown_event.is_set():
            if not staying_alive:
                logger.info(f"[{phone}] Режим наблюдения – дальнейшие действия не выполняются")
                staying_alive = True
            count_wards = sum(1 for value in observing_modes.values() if value)
            if count_wards >= session_count + session_count_formal_contact:
                text_ward_bot = "👁️ Все аккаунты ашки перешли в режим мониторинга, можно начинать дальнейшие действия"
                send_start_message(CHAT_ID, text_ward_bot, thread_id=4294972606)
            like_process[phone] = False
            # ← вот здесь мы «отдаём» цикл другим корутинам и хендлерам
            try:
                # Будем ждать сигнала или cancellation
                await shutdown_event.wait()
            except asyncio.CancelledError:
                # Таску отменили — надо выйти из like_people
                logger.info(f"[{phone}] like_people cancelled, exiting.")
                return


def get_random_mention(mentions: set[str], min_length: int = 3) -> str | None:
    """
    Возвращает случайное упоминание из множества, длина которого не меньше min_length.
    Если подходящих упоминаний нет, возвращает None.
    """
    valid = [m for m in mentions if len(m) >= min_length]
    return random.choice(valid) if valid else None

async def process_session(phone, session_count, session_count_formal_contact, pool: asyncpg.Pool, informal_contact=True):
    global like_process
    verif_errors[phone] = False
    emoji_errors[phone] = False

    config = load_session_config(phone, informal_contact)
    if not config:
        observing_modes[phone] = True
        return

    device_model = config.get('device_model')
    system_version = config.get('system_version')

    if config.get('only_checker'):
        observing_modes[phone] = True
        return

    api_id = config.get('app_id')
    api_hash = config.get('app_hash')
    session_file = os.path.join(SESSIONS_DIR,
                                f'{phone}.session' if informal_contact else f'formal_contact/{phone}.session')

    proxy = config.get('proxy')
    proxy_type = config.get('proxy_type', '').upper()
    proxy_info = {
        "type": proxy_type,
        "connection": None,
        "connection_cortege": None
    }

    if proxy and proxy_type in ["HTTP", "SOCKS5"]:
        proxy_info["connection_cortege"] = (proxy_type, proxy[1], proxy[2], proxy[3], proxy[4], proxy[5])
    elif proxy and proxy_type == "MTPROTO":
        proxy_info["connection"] = "ConnectionTcpMTProxy"
        proxy_info["connection_cortege"] = (proxy[1], proxy[2], proxy[5])

    if proxy_info["type"] == "MTPROTO":
        client = TelegramClient(
            session_file,
            api_id,
            api_hash,
            proxy=proxy_info["connection_cortege"],
            connection=proxy_info["connection"],
            device_model=device_model,
            system_version=system_version,
            app_version='8.4',
            connection_retries=52,
            request_retries=52
        )
    elif proxy_info["type"] in ["HTTP", "SOCKS5"]:
        client = TelegramClient(
            session_file,
            api_id,
            api_hash,
            proxy=proxy_info["connection_cortege"],
            device_model=device_model,
            system_version=system_version,
            app_version='8.4',
            connection_retries=52,
            request_retries=52
        )
    else:
        client = TelegramClient(
            session_file,
            api_id,
            api_hash,
            device_model=device_model,
            system_version=system_version,
            app_version='8.4',
            connection_retries=52,
            request_retries=52
        )

    def prevent_code_request():
        observing_modes[phone] = True
        raise IOError("требует код, проверьте сессии")

    # helper: «очистка» строки, чтобы остались только [A-Za-z0-9_], и проверка длины
    def clean_username(raw: str) -> str | None:
        """
        Оставляет в raw только символы A-Z, a-z, 0-9 и _,
        а затем проверяет длину 5–32. Если совпадает — возвращает их, иначе None.
        """
        # Удаляем всё, что не буква/цифра/_
        cleaned = re.sub(r'[^A-Za-z0-9_]', '', raw)
        if 5 <= len(cleaned) <= 32:
            return cleaned
        return None

    async def extract_mentions(message: Message) -> set[str]:
        text = message.text or ''
        mentions = set()

        # 1) Entities of type @username
        for ent in message.entities or []:
            if isinstance(ent, MessageEntityMention):
                username = text[ent.offset + 1: ent.offset + ent.length]
                mentions.add(username)
            elif isinstance(ent, MessageEntityTextUrl):
                # Hyperlink with URL pointing to a user
                url = f"({ent.url})"
                m = TME_LINK_PATTERN.search(url)
                if m:
                    mentions.add(m.group(1))
            elif isinstance(ent, MessageEntityMentionName):
                # Name mention, contains user_id
                try:
                    user = await client.get_entity(ent.user_id)
                    if user.username:
                        mentions.add(user.username)
                    else:
                        mentions.add(f"{ent.user_id}")
                except Exception:
                    # если не удалось, возвращаем хотя бы ID
                    mentions.add(f"{ent.user_id}")

        # 2) Если entity ничего не выдали, делаем «сырой» поиск по тексту
        if not mentions or len(mentions) < 1:
            # raw-упоминания вида @username
            for uname in USERNAME_PATTERN.findall(text):
                if cleaned := clean_username(uname):
                    mentions.add(cleaned)

            # raw-ссылки вида t.me/username (может быть без скобок)
            for uname in TME_LINK_PATTERN.findall(text):
                if cleaned := clean_username(uname):
                    mentions.add(cleaned)

        return mentions

    async def save_mentions(session: AsyncSession, account_id: int, mention: str, table: str = 'a'):
        """
        Save mentions into MentionA or MentionB if not already present for the account.
        """
        model = MentionA if table == 'a' else MentionB
        if mention is None:
            return 1
        # Проверяем, есть ли уже такая запись
        exists = await session.scalar(
            select(model)
            .where(model.AccountId == account_id, model.Username == mention)
            .limit(1)
        )
        try:
            found = 0
            if not exists:
                session.add(model(AccountId=account_id, Username=mention))
            else:
                found = 1
        finally:
            await session.commit()

        return found

    try:
        await client.start(phone=phone, code_callback=prevent_code_request)
        if await client.is_user_authorized():
            logger.info(f"[{phone}] Успешная авторизация для {phone}")
        else:
            logger.error(f"[{phone}] Не удалось авторизоваться для {phone}")

        #@client.on(events.NewMessage(pattern='🔗 Юз'))
        #@client.on(events.NewMessage(pattern='Есть взаимная симпатия! Начинай общаться'))
        #async def handle_favorite_message(event):
        #    global vzaimok, vzaimok_formal
        #    await client.forward_messages('me', event.message)
        #    logger.info(f"[{phone}] Сообщение переслано в избранное: {event.raw_text}")
        #    if informal_contact:
        #        vzaimok += 1
        #    else:
        #        vzaimok_formal += 1

        @client.on(events.NewMessage(pattern='У тебя новый мэтч'))
        async def handle_new_metch_message(event):
            global like_process
            if like_process[phone]:
                return
            try:
                like_process[phone] = True
                found = False
                one_more = True
                reply_markup = event.message.reply_markup
                if reply_markup is None:
                    found = False
                    return
                buttons = reply_markup.rows
                for row_idx, row in enumerate(buttons):
                    for btn_idx, button in enumerate(row.buttons):
                        if button.text == 'Посмотреть':
                            await event.message.click(row_idx, btn_idx)
                            await asyncio.sleep(1)
                            found = True
                            break
                    if found:
                        break
                await asyncio.sleep(3)
                bot = await client.get_entity('ashqua_bot')

                one_more = True
                try_n = 0
                already_satisfied = 0
                while one_more:
                    one_more = False
                    if already_satisfied >= 5:
                        break
                    if try_n >= 5:
                        break
                    await asyncio.sleep(1)
                    messages = await client.get_messages(bot, limit=1)
                    usernames = await extract_mentions(messages[0])
                    random_mention = get_random_mention(usernames)
                    reply_markup = messages[0].reply_markup
                    if reply_markup is None:
                        await client.send_message(bot, "/matches")
                        await asyncio.sleep(1)
                        one_more = True
                        try_n += 1
                        continue
                    global_repeat = False
                    buttons = reply_markup.rows
                    for row_idx, row in enumerate(buttons):
                        for btn_idx, button in enumerate(row.buttons):
                            if button.text == 'Посмотреть':
                                await client.send_message(bot, "/matches")
                                await asyncio.sleep(1)
                                one_more = True
                                global_repeat = True
                                break
                            if button.text == '⬅️ Предыдущий':
                                global vzaimok, vzaimok_formal
                                temp_already_satisfied = already_satisfied
                                try:
                                    async with AsyncSessionLocal() as session:
                                        # Find or create account
                                        me = await client.get_me()
                                        acc_username = me.username
                                        del me
                                        result = await session.scalar(
                                            select(Account).where(Account.Username == acc_username)
                                        )
                                        if not result:
                                            account = Account(Username=acc_username,
                                                              Phone=phone)
                                            session.add(account)
                                            await session.commit()
                                        else:
                                            account = result
                                        # Save mentions
                                        if random_mention:
                                            already_satisfied += await save_mentions(session, account.Id, random_mention, table='a')
                                except Exception as e:
                                    logger.error(f"[{phone}] AsyncSessionLocal {e}")
                                    already_satisfied += 1

                                if temp_already_satisfied == already_satisfied:
                                    group = await get_or_create_group(client, "My mind", pool, phone)
                                    await client.forward_messages(group, messages[0])
                                    logger.info(
                                        f"[{phone}] Сообщение переслано в избранное: {random_mention}")

                                    if informal_contact:
                                        vzaimok += 1
                                    else:
                                        vzaimok_formal += 1
                                await messages[0].click(row_idx, btn_idx)
                                await asyncio.sleep(1)
                                one_more = True
                        if global_repeat:
                            break
                like_process[phone] = False
                await client.send_message(bot, "🔎 Лента")
            #except Exception as e:
            #    logger.error(f"handle_match [{phone}]\n{e}")
            #    await pool.execute("DELETE FROM tg_groups_ashqua WHERE phone_number = $1", phone)
            finally:
                like_process[phone] = False

        @client.on(events.NewMessage(pattern='❤️ У тебя'))
        @client.on(events.NewMessage(pattern='💌 У тебя'))
        async def handle_new_like_message(event=None, manual=False):
            global like_process
            if like_process[phone]:
                return
            try:
                like_process[phone] = True
                found = False
                one_more = True
                bot = await client.get_entity('ashqua_bot')
                if not manual and event is not None:
                    reply_markup = event.message.reply_markup
                    if reply_markup is None:
                        found = False
                        return
                    buttons = reply_markup.rows
                    for row_idx, row in enumerate(buttons):
                        for btn_idx, button in enumerate(row.buttons):
                            if button.text == 'Посмотреть':
                                await event.message.click(row_idx, btn_idx)
                                await asyncio.sleep(1)
                                found = True
                                break
                        if found:
                            break
                if manual:
                    await client.send_message(bot, "/likes")
                    await asyncio.sleep(3)
                await asyncio.sleep(3)
                found = False
                while one_more:
                    one_more = False
                    messages = await client.get_messages(bot, limit=1)
                    reply_markup = messages[0].reply_markup
                    if reply_markup is None:
                        found = False
                        await client.send_message(bot, "/likes")
                        await asyncio.sleep(4)

                    messages = await client.get_messages(bot, limit=1)
                    reply_markup = messages[0].reply_markup
                    if reply_markup is None:
                        found = False
                        await asyncio.sleep(1)
                        break

                    buttons = reply_markup.rows
                    global_repeat = False
                    for row_idx, row in enumerate(buttons):
                        for btn_idx, button in enumerate(row.buttons):
                            if button.text == 'Посмотреть':
                                await client.send_message(bot, "/likes")
                                await asyncio.sleep(3)
                                one_more = True
                                global_repeat = True
                                break
                            if button.text in ["❤️", "👍"]:
                                await messages[0].click(row_idx, btn_idx)
                                await asyncio.sleep(1)
                                found = True
                            if button.text == '⬅️ Предыдущий':
                                one_more = True
                        if global_repeat:
                            break
                await asyncio.sleep(1)
                await client.send_message(bot, "/matches")
                await asyncio.sleep(1)
                one_more = True
                already_satisfied = 0
                try_n_n = 0
                while one_more:
                    one_more = False
                    if already_satisfied >= 3:
                        break
                    if try_n_n >= 5:
                        break
                    await asyncio.sleep(1)
                    messages = await client.get_messages(bot, limit=1)
                    usernames = await extract_mentions(messages[0])
                    if usernames:
                        random_mention = str(get_random_mention(usernames))
                    else:
                        random_mention = None
                    reply_markup = messages[0].reply_markup
                    if reply_markup is None:
                        await client.send_message(bot, "/matches")
                        await asyncio.sleep(1)
                        one_more = True
                        try_n_n += 1
                        continue
                    global_repeat = False
                    buttons = reply_markup.rows
                    for row_idx, row in enumerate(buttons):
                        for btn_idx, button in enumerate(row.buttons):
                            if button.text == 'Посмотреть':
                                await client.send_message(bot, "/matches")
                                await asyncio.sleep(1)
                                one_more = True
                                global_repeat = True
                                break
                            if button.text == '⬅️ Предыдущий':
                                global vzaimok, vzaimok_formal
                                temp_already_satisfied = already_satisfied
                                try:
                                    async with AsyncSessionLocal() as session:
                                        # Find or create account
                                        me = await client.get_me()
                                        acc_username = me.username
                                        del me
                                        result = await session.scalar(
                                            select(Account).where(Account.Username == acc_username)
                                        )
                                        if not result:
                                            account = Account(Username=acc_username,
                                                              Phone=phone)
                                            session.add(account)
                                            await session.commit()
                                        else:
                                            account = result
                                        # Save mentions
                                        if random_mention:
                                            already_satisfied += await save_mentions(session, account.Id,
                                                                                     random_mention, table='a')
                                        else:
                                            already_satisfied += 1
                                except Exception as e:
                                    logger.error(f"[{phone}] AsyncSessionLocal {e}")
                                    already_satisfied += 1

                                if temp_already_satisfied == already_satisfied:
                                    group = await get_or_create_group(client, "My mind", pool, phone)
                                    await client.forward_messages(group, messages[0])
                                    logger.info(
                                        f"[{phone}] Сообщение переслано в избранное: {messages[0].message[0: 14]}")

                                    if informal_contact:
                                        vzaimok += 1
                                    else:
                                        vzaimok_formal += 1
                                await messages[0].click(row_idx, btn_idx)
                                await asyncio.sleep(1)
                                one_more = True
                        if global_repeat:
                            break
                like_process[phone] = False
                await client.send_message(bot, "🔎 Лента")
            except Exception as e:
                logger.error(f"handle_like\n{e}")
            finally:
                like_process[phone] = False



        #@client.on(events.NewMessage(pattern=r'Для верификации'))
        #async def handle_verification(event):
        #    global verif_errors, emoji_errors, observing_modes
        #    logger.info(f"[{phone}] Словил вериф")
        #    observing_modes[phone] = True
        #    try:
        #        bot = await client.get_entity('leomatchbot')
        #        if hasattr(event.message, 'message'):
        #            message_text = event.message.message
        #            if emoji_errors[phone] == False:
        #                logger.info(f"[{phone}] Начинаю авто-вериф")
        #                if verif_errors[phone]:
        #                    logger.info(f"[{phone}] Авто-верификация не прошла, повторяю попытку: {event.raw_text}")
        #                    verif_errors[phone] = False
        #                    await client.send_message(bot, "1")
        #                emoji_to_file = {
        #                    "✋": VERIF_FIVE,
        #                    "👍": VERIF_UP,
        #                    "👎": VERIF_DOWN,
        #                    "✌️": VERIF_TWIN,
        #                    "✊": VERIF_FIST,
        #                    "🤟": VERIF_ROCK,
        #                    "☝️": VERIF_FINGER,
        #                }
        #                if not verif_errors[phone]:
        #                    found_emoji = next((emoji for emoji in emoji_to_file if emoji in message_text), None)
        #                    if found_emoji:
        #                        verif_errors[phone] = True
        #                        await client.send_file(event.chat_id, file=emoji_to_file[found_emoji], video_note=True)
        #                        logger.info(f"[{phone}] Авто-верификация прошла: {event.raw_text}")
        #                        await asyncio.sleep(10)
        #                    else:
        #                        logger.error(f"[{phone}] Авто-верификация не прошла, эмодзи не найден: {event.raw_text}")
        #                        verif_errors[phone] = True
        #                        emoji_errors[phone] = True
        #                        return
        #    except Exception as e:
        #        if not verif_errors[phone]:
        #            verif_errors[phone] = True
        #            emoji_errors[phone] = True
        #            logger.error(f"[{phone}] Ошибка авто-верификации: {e} || {event.raw_text}")

        await like_people(phone, client, session_count, session_count_formal_contact, handle_new_like_message,  informal_contact)
    except asyncio.CancelledError:
        # задача была отменена из main(): логируем и пробрасываем дальше
        logger.info(f"[{phone}] Получена отмена задачи, закрываем client...")
        raise
    except SessionPasswordNeededError:
        logger.error(f"[{phone}] Необходим пароль для двухфакторной аутентификации")
    except Exception as e:
        logger.error(f"[{phone}] Ошибка: {e}")
    finally:
        try:
            observing_modes[phone] = True
            await client.disconnect()
            logger.info(f"[{phone}] TelegramClient корректно отключен.")
        except Exception as e:
            logger.warning(f"[{phone}] Ошибка при отключении клиента: {e}")


def send_start_message(chat_id, text, retries=3, delay=2, thread_id=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    if thread_id is not None:
        data = {"chat_id": chat_id, "text": text, "message_thread_id": thread_id}
    else:
        data = {"chat_id": chat_id, "text": text}
    attempt = 0
    while attempt < retries:
        try:
            response = requests.post(url, data=data)
            if response.status_code == 200:
                return
            else:
                logger.error("Ошибка отправки сообщения: %s", response.text)
                break
        except requests.exceptions.SSLError as e:
            logger.error("SSL ошибка: %s. Попытка %d из %d", e, attempt + 1, retries)
            attempt += 1
            time.sleep(delay)
        except Exception as e:
            logger.error("Неожиданная ошибка: %s", e)
            break
    logger.error("Не удалось отправить сообщение после %d попыток.", retries)


async def perform_action_at_time(pool: asyncpg.Pool):
    target_time = datetime.datetime.strptime(BB_TIME, "%H:%M").time()
    while True:
        now = datetime.datetime.now()
        target_datetime = datetime.datetime.combine(now.date(), target_time)
        if target_datetime < now:
            target_datetime += datetime.timedelta(hours=8)
        remaining_seconds = (target_datetime - now).total_seconds()
        await asyncio.sleep(remaining_seconds)
        text_bb_bot = "💤 Софт завершает свою работу."
        admin_bb_bot = f"💤 Софт завершает свою работу.\n🤝 Взаимок: неформ {vzaimok} форм {vzaimok_formal}"
        logger.info(f"Время {BB_TIME}! Выполняется запланированное действие.")
        #send_start_message(CHAT_ID_rezerv, text_bb_bot)
        send_start_message(CHAT_ID, text_bb_bot, thread_id=4294972606)
        send_start_message(CHAT_ID_ADMIN, admin_bb_bot)
        await asyncio.sleep(60)
        #loop = asyncio.get_running_loop()
        #loop.stop()
        #await pool.close()
        #os._exit(0)
        #sys.exit()
        #os._exit()

        shutdown_event.set()


async def main():
    await init_db()
    pool = await asyncpg.create_pool(DATABASE_DSN2, min_size=1, max_size=5)

    text_pre_bot = "⚠️ Софт запускается, в ашку не заходим!"
    send_start_message(CHAT_ID, text_pre_bot, thread_id=4294972606)
    #send_start_message(CHAT_ID_rezerv, text_pre_bot)
    await asyncio.sleep(PRELAUNCH_MESSAGE_DELAY)

    phones = [f.split('.')[0] for f in os.listdir(SESSIONS_DIR) if f.endswith('.session')]
    if not phones:
        logger.error("Не найдено ни одной сессии в папке.")
        return

    phones_formal_contact = [f.split('.')[0] for f in os.listdir(f"{SESSIONS_DIR}formal_contact/") if f.endswith('.session')]
    if not phones_formal_contact:
        logger.error("Не найдено ни одной сессии формальных аккаунтов.")
        if not phones:
            return

    session_count = len(phones)
    session_count_formal_contact = len(phones_formal_contact)
    text_start_bot = "✅ Софт ашка запущен в стандартном режиме."
    send_start_message(CHAT_ID, text_start_bot, thread_id=4294972606)
    #send_start_message(CHAT_ID_rezerv, text_start_bot)

    text_start_admin_bot = f"✅ Софт ашка запущен в стандартном режиме.\n\n📱 Количество сессий: {session_count} + формальные: {session_count_formal_contact}\n\npowered by AlgoApi"
    send_start_message(CHAT_ID_ADMIN, text_start_admin_bot)

    asyncio.create_task(perform_action_at_time(pool))
    tasks_informal_contact = [asyncio.create_task(process_session(phone, session_count, session_count_formal_contact, pool))
                              for phone in phones]
    tasks_formal_contact = [asyncio.create_task(process_session(phone, session_count, session_count_formal_contact, pool, informal_contact=False))
                            for phone in phones_formal_contact]

    # Навешиваем сигналы
    loop = asyncio.get_running_loop()
    try:
        # Unix‑only: ловим SIGINT/SIGTERM через add_signal_handler
        if hasattr(loop, 'add_signal_handler') and sys.platform != 'win32':
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, _on_exit, *())
        else:
            # На Windows и при отсутствии метода — ничего не делаем.
            # Будем полагаться на KeyboardInterrupt ниже.
            pass
    except Exception:
        pass

    # 3) Ждём сигнала на остановку
    await shutdown_event.wait()
    logger.info("🔔 Shutdown signal received, cancelling tasks...")
    # 4) Отменяем все задачи, ждём их «finally»
    for t in (tasks_informal_contact + tasks_formal_contact):
        t.cancel()

    await asyncio.gather(*(tasks_informal_contact + tasks_formal_contact), return_exceptions=True)

    await pool.close()
    logger.info("✅ All resources closed, exiting main()")

if __name__ == "__main__":
    now = datetime.datetime.now()
    if now.hour < 10:
        target_time_wakeup = datetime.datetime.strptime(WAKEUP_AFTER_NIGHT, "%H:%M").time()
        target_datetime_wakeup = datetime.datetime.combine(now.date(), target_time_wakeup)
        logger.info("Скрипт запустился ночью, ждём утра " + str((target_datetime_wakeup - now).total_seconds()))
        time.sleep((target_datetime_wakeup - now).total_seconds())
        logger.info("bye :)")
        time.sleep(60)
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logger.info("bye :)")

