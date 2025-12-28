import traceback
import signal
import aiofiles
from telethon import TelegramClient, events
from typing import Optional, Tuple
from telethon.tl.types import InputPeerChannel
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import MessageEntityMention, MessageEntityTextUrl, MessageEntityMentionName
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import CreateChatRequest
from config import BB_TIME, SESSIONS_DIR, LOG_FILE, MESSAGES_FILE, CHAT_ID_ADMIN, PRELAUNCH_MESSAGE_DELAY, \
    SYNONYMS_FILE, ENVELOPE_TIME_BEFORE_SEND_MESSAGE, MAX_ENVELOPE_MESSAGES_ALL_SESSIONS, ENVELOPE_EMOJI, MAX_LIMIT, \
    CHAT_ID, CHAT_ID_rezerv, BOT_TOKEN
from config import (VERIFICATIONS, VERIF_FIVE, VERIF_UP, VERIF_TWIN, VERIF_FIST, VERIF_ROCK, VERIF_FINGER, VERIF_DOWN,
                    WAKEUP_AFTER_NIGHT)
from telethon import TelegramClient, errors
import json, os, random, logging, asyncio, sys, requests, time
from telethon import errors
import datetime
import json
from telethon.tl.types import Channel, Chat, InputPeerChannel, InputPeerChat
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, func, select
import re

# Regex patterns for inline mentions and t.me links
USERNAME_PATTERN = re.compile(r'@([A-Za-z0-9_]{5,32})')
TME_LINK_PATTERN = re.compile(r'(?<=\()(?:https?://)?t\.me/([A-Za-z0-9_]{5,32})(?=\))')

DATABASE_DSN = "postgresql+asyncpg://log:pass@ip:port/db_name"
DATABASE_DSN2 = "postgresql://log:pass@ip:port/db_name"

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

# ========== Глобальный shutdown_event ==========
shutdown_event = asyncio.Event()

def _on_exit():
    """SIGINT/SIGTERM handler"""
    shutdown_event.set()

import asyncpg


async def get_group_id(pool: asyncpg.Pool, phone_number: str) -> Optional[Tuple[int, int]]:
    """
    Если для phone_number в таблице есть запись — вернёт кортеж (group_id, access_hash),
    иначе — None.
    """
    row = await pool.fetchrow(
        "SELECT group_id, access_hash FROM tg_groups WHERE phone_number = $1",
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
            INSERT INTO tg_groups (phone_number, group_id, access_hash)
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
                peer = InputPeerChannel(channel_id=gid, access_hash=ah)
                # 2) Получаем entity группы
                grp = await client.get_entity(peer)
                logger.info("нашёл группу из бд")
                return grp
            except Exception as e:
                logger.error(f"{phone_number} ошибка получения get_entity или InputPeerChannel группы для взаимок с бд, "
                             f"продолжаем дальше\n{e}\n\ngid: {gid}\naccess_hash: {ah}")
                # если вдруг уже удалили/сменили — сбросим и пойдём дальше
                await pool.execute("DELETE FROM tg_groups WHERE phone_number = $1", phone_number)
        elif gid:
            try:
                # 1) Строим InputPeerChat
                #peer = InputPeerChat(chat_id=gid)
                # 2) Получаем entity группы
                # given gid = chat_id
                full = await client(GetFullChatRequest(chat_id=gid))
                # the “chats” array contains one Chat object
                grp = full.chats[0]
                logger.info("нашёл группу из бд")
                if grp is None:
                    raise RuntimeError(f"full.chats[0] is none")
                if not grp:
                    raise RuntimeError(f"if not full.chats[0]")
                return grp
            except Exception as e:
                logger.error(f"{phone_number} ошибка получения get_entity или InputPeerChat группы для взаимок с бд, "
                             f"продолжаем дальше\n{e}\n\ngid: {gid}\naccess_hash: {ah}")
                # если вдруг уже удалили/сменили — сбросим и пойдём дальше
                await pool.execute("DELETE FROM tg_groups WHERE phone_number = $1", phone_number)
        else:
            logger.warning(f"{phone_number} ошибка получения id и access_hash = None группы для взаимок с бд, "
                           f"продолжаем дальше\ninfo_group: {info_group}")
            await pool.execute("DELETE FROM tg_groups WHERE phone_number = $1", phone_number)
    else:
        logger.warning(f"{phone_number} ошибка получения info_group (id и access_hash) группы для взаимок с бд, "
                       f"продолжаем дальше\ninfo_group: {info_group}")
        await pool.execute("DELETE FROM tg_groups WHERE phone_number = $1", phone_number)

    # 2) ищем в последних группах (быстро) по title
    group = await find_group(client, group_title)
    if group:
        if isinstance(group, Channel):
            ah = group.access_hash
        else:
            # Chat у Telethon действительно не имеет access_hash
            ah = 0
        # …сохраняем в БД и возвращаем
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
                if isinstance(new_group, Channel):
                    ah = new_group.access_hash
                else:
                    # Chat у Telethon действительно не имеет access_hash
                    ah = 0
                # …сохраняем в БД и возвращаем
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
verif_errors = {}
emoji_errors = {}
vzaimok = 0
vzaimok_formal = 0


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


async def like_people(phone, client, session_count, session_count_formal_contact, pool, informal_contact=True):
    global observing_modes
    logger.info(f"[{phone}] Запуск функции like_people")
    staying_alive = False
    iterrations = 1
    buttons_not_found = 0
    buttons_not_found_2 = 0
    count_sended_envelope = 0
    count_dislike = 0
    errors_conv = 0

    observing_modes[phone] = False
    generated_message = load_messages(MESSAGES_FILE)
    generated_synonym = load_messages(SYNONYMS_FILE)

    group = await get_or_create_group(client, "My dreems", pool, phone)

    while True:
        while iterrations <= MAX_LIMIT and MAX_LIMIT != 0 and not observing_modes[phone]:
            try:
                logger.info(f"[{phone}] ИТЕРАЦИЯ #{iterrations}")
                bot = await client.get_entity('leomatchbot')
                messages = await client.get_messages(bot, limit=1)

                if "Слишком много" in messages[0].message:
                    observing_modes[phone] = True
                    break

                reply_markup = messages[0].reply_markup

                if buttons_not_found >= 3:
                    await asyncio.sleep(1)
                    await client.send_message(bot, "1")
                elif buttons_not_found >= 5:
                    logger.warning(f"[{phone}] Клавиатура НЕ найдена 5 раз!")
                    await asyncio.sleep(1)
                    await client.send_message(bot, "/myprofile")
                    await asyncio.sleep(1)
                    await client.send_message(bot, "1")
                elif buttons_not_found >= 7:
                    logger.warning(f"[{phone}] Клавиатура НЕ найдена 7 раз!")
                    await asyncio.sleep(1)
                    await client.send_message(bot, "/myprofile")
                    await asyncio.sleep(1)
                    messages = await client.get_messages(bot, limit=1)
                    await messages[0].click()
                elif buttons_not_found >= 9:
                    logger.error(f"[{phone}] Клавиатура НЕ найдена 9 РАЗ!")
                    await asyncio.sleep(1)
                    buttons_not_found = 0
                    await client.send_message(bot, "1")

                if not reply_markup:
                    buttons_not_found += 1
                    logger.info(
                        f"[{phone}] Под последним сообщением не найдена клавиатура, делаем поиск по старым сообщениям")
                    i = 1
                    while True:
                        await asyncio.sleep(2)
                        messages = await client.get_messages(bot, limit=i)
                        reply_markup = messages[-1].reply_markup
                        if not reply_markup:
                            i += 1
                        else:
                            break

                keyboard_text = generate_text_keyboard(reply_markup)
                if reply_markup:
                    logger.info(f"[{phone}] Клавиатура найдена: {keyboard_text}")

                found = False
                buttons = reply_markup.rows

                await asyncio.sleep(3)

                if '➡️' in keyboard_text:
                    await client.send_message(bot, "➡️")
                    await asyncio.sleep(1)
                    found = True

                for row in buttons:
                    for button in row.buttons:
                        if button.text == "1 🚀":
                            await client.send_message(bot, "1 🚀")
                            await asyncio.sleep(3)
                            found = True
                            break
                        if button.text == '✖️':
                            await client.send_message(bot, '✖️')
                            await asyncio.sleep(3)
                            found = True
                            break
                        if "Пожаловаться" in button.text:
                            try:
                                await messages[1].click()
                                await asyncio.sleep(2)
                                found = True
                                break
                            except:
                                await messages[0].click()
                                await asyncio.sleep(2)
                                found = True
                                break
                        if len(buttons) > 1 or len(row.buttons) < 3:
                            await messages[0].click()
                            await asyncio.sleep(2)
                            found = True
                            break

                        if count_sended_envelope >= (MAX_ENVELOPE_MESSAGES_ALL_SESSIONS):
                            if count_dislike == 0:
                                await asyncio.sleep(ENVELOPE_TIME_BEFORE_SEND_MESSAGE)
                                random_like = random.randint(0, 1)

                                await client.send_message(bot, "❤️" if random_like == 0 else "👍")
                                logger.info(f"[{phone}] Отправлен лайк после {count_sended_envelope} конвертов.")

                                await asyncio.sleep(5)
                                await client.send_message(bot, "👎")
                                logger.info(
                                    f"[{phone}] Отправлен дизлайк после {count_sended_envelope} конвертов и лайка.")

                                count_dislike += 1
                                count_sended_envelope = 0
                                iterrations += 1
                                await asyncio.sleep(1)
                                continue

                        if any(char in item for item in button.text for char in ENVELOPE_EMOJI if char.strip()):
                            await asyncio.sleep(1)
                            found = True
                            if informal_contact:
                                await client.send_message(bot, button.text)
                                logger.info(f"[{phone}] Нажата кнопка {button.text}")
                                random_message = generate_random_message(generated_message, generated_synonym)
                                await asyncio.sleep(1)
                                envelope_time_random = random.randint(ENVELOPE_TIME_BEFORE_SEND_MESSAGE,
                                                                      ENVELOPE_TIME_BEFORE_SEND_MESSAGE + 20)
                                logger.info(
                                    f"[{phone}] Рандомно спим прежде чем отправить сообщение: {envelope_time_random} секунд")
                                await asyncio.sleep(envelope_time_random)
                                await client.send_message(bot, random_message)
                                logger.info(f"[{phone}] Отправлено сообщение: {random_message}")
                                await asyncio.sleep(1)
                            else:
                                random_like = random.randint(0, 1)
                                await asyncio.sleep(5)
                                await client.send_message(bot, "❤️" if random_like == 0 else "👍")
                            count_sended_envelope += 1
                            count_dislike = 0
                            break

                    if found:
                        buttons_not_found = 0
                        buttons_not_found_2 = 0
                        break

                if not found and buttons_not_found_2 < 4:
                    logger.info(f"[{phone}] ({keyboard_text}) Не удалось нажать ни на одну кнопку, нажимаем на первую")
                    buttons_not_found_2 += 1
                    await messages[0].click()
                    await asyncio.sleep(1)
                elif not found and buttons_not_found_2 >= 3:
                    try:
                        logger.info(
                            f"[{phone}] ({keyboard_text}) Не удалось нажать ни на одну кнопку 3 раза, нажимаем на вторую")
                        buttons_not_found_2 = 0
                        await client.send_message(bot, "/start")
                        await asyncio.sleep(1)
                        await client.send_message(bot, "1")
                        await asyncio.sleep(1)
                    except Exception as e:
                        logger.info(f"[{phone}] Ошибка при нажатии на вторую кнопку, выходим из цикла")
                        break

                await asyncio.sleep(3)

            except Exception as e:
                errors_conv += 1
                logger.warning(f"[{phone}] Ошибка в цикле like_people, продолжаем попытки: {e}")
                try:
                    await client.connect()
                except Exception as e:
                    logger.warning(f"[{phone}] Ошибка в цикле like_people (client.connect), продолжаем попытки: {e}")
                if errors_conv >= 3:
                    logger.info(
                        f"[{phone}] Ошибка в цикле like_people больше трёх раз, выходим из цикла в режим наблюдения")
                    break

        if iterrations > MAX_LIMIT:
            observing_modes[phone] = True

        while observing_modes[phone] == True and not shutdown_event.is_set():
            if staying_alive == False:
                observing_modes[phone] = True
                logger.info(f"[{phone}] Наблюдаем за ситуацией, больше не тыкаем никуда!")
                staying_alive = True
            count_wards = sum(1 for value in observing_modes.values() if value)
            if count_wards >= session_count + session_count_formal_contact:
                text_ward_bot = f"👁️ Все аккаунты написали письма и успешно перешли в мониторинг, теперь можно заходить в ДВ"
                send_start_message(CHAT_ID, text_ward_bot, thread_id=4294972606)
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
    verif_errors[phone] = False
    emoji_errors[phone] = False

    config = load_session_config(phone, informal_contact)
    if not config:
        return

    if config.get('only_ashqua'):
        observing_modes[phone] = True
        return

    if config.get('only_checker'):
        observing_modes[phone] = True
        return

    device_model = config.get('device_model')
    system_version = config.get('system_version')

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

    elif proxy_info["type"] == "HTTP" or proxy_info["type"] == "SOCKS5":
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
        raise IOError(f"требует код, проверьте сессии")

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

    async def extract_mentions(event) -> set[str]:
        """
        Извлекает все упоминания из объекта event.TelegramEvent (например, NewMessage.Event).
        Возвращает множество юзернеймов (без '@').
        """
        # Получаем сам текстовый Message
        message = event.message
        text = message.text or ''
        mentions = set()

        # Обрабатываем встроенные entity
        if entities := getattr(message, 'entities', None):
            for ent in entities:
                if isinstance(ent, MessageEntityMention):
                    # @username
                    uname = text[ent.offset + 1: ent.offset + ent.length]
                    if uname:
                        mentions.add(uname)

                elif isinstance(ent, MessageEntityTextUrl):
                    # [текст](https://t.me/username)
                    if m := TME_LINK_PATTERN.search(f"({ent.url})"):
                        mentions.add(m.group(1))

                elif isinstance(ent, MessageEntityMentionName):
                    # упоминание по ID
                    try:
                        user = await client.get_entity(ent.user_id)
                        if user.username:
                            mentions.add(user.username)
                        else:
                            mentions.add(f"{ent.user_id}")
                    except Exception:
                        # приватный пользователь или другая ошибка — пропускаем
                        mentions.add(f"{ent.user_id}")
                        continue

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


    async def save_mentions(session: AsyncSession, account_id: int, mention: str, table: str = 'b'):
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

    #
    try:
        await client.start(phone=phone, code_callback=prevent_code_request)
        if await client.is_user_authorized():
            logger.info(f"[{phone}] Успешная авторизация для {phone}")
        else:
            logger.error(f"[{phone}] Не удалось авторизоваться для {phone}")

        @client.on(events.NewMessage(pattern='Отлично! Надеюсь хорошо проведете время'))
        @client.on(events.NewMessage(pattern='Есть взаимная симпатия! Начинай общаться'))
        async def handle_favorite_message(event):
            global vzaimok
            global vzaimok_formal
            try:
                group = await get_or_create_group(client, "My dreems", pool, phone)
                await client.forward_messages(group, event.message)
            except Exception as e:
                logger.error(f"handle_vzaimki_dv [{phone}] \n{e} \n {group}")
                await pool.execute("DELETE FROM tg_groups WHERE phone_number = $1", phone)
            logger.info(f"[{phone}] Сообщение переслано в избранное для {phone}: {event.raw_text}")
            usernames = await extract_mentions(event)
            random_mention = get_random_mention(usernames)
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
                        await save_mentions(session, account.Id, random_mention, table='b')
            except Exception as e:
                logger.error(f"[{phone}] AsyncSessionLocal {e}")

            if informal_contact:
                vzaimok += 1
            else:
                vzaimok_formal += 1

        @client.on(events.NewMessage(pattern=r'Ты понравил'))
        @client.on(events.NewMessage(pattern=r'Кому-то понравилась'))
        async def handle_favorite_message(event):
            bot = await client.get_entity('leomatchbot')
            await client.send_message(bot, "1")
            logger.info(f"[{phone}] Пришёл лайк для {phone}: {event.raw_text}")

        @client.on(events.NewMessage(pattern=r'Буст повышается только у подписчиков моего канала'))
        @client.on(events.NewMessage(pattern=r'буст твоей анкеты понижен'))
        async def handle_favorite_message(event):
            channel = await client.get_entity('leoday')
            await client(JoinChannelRequest(channel))
            logger.info(f"[{phone}] Кажется кто-то не подписался на канал {phone}: {event.raw_text}")

        @client.on(events.NewMessage(pattern=r'Для верификации'))
        async def handle_verification(event):
            global verif_errors, emoji_errors, observing_modes
            logger.info(f"[{phone}] Словил вериф")
            observing_modes[phone] = True
            try:
                bot = await client.get_entity('leomatchbot')
                if hasattr(event.message, 'message'):
                    message_text = event.message.message
                    if emoji_errors[phone] == False:
                        logger.info(f"[{phone}] Начинаю авто-вериф")
                        if verif_errors[phone] == True:
                            logger.info(
                                f"[{phone}] Авто-верификация у +{phone} не прошла, повторяю попытку: {event.raw_text}")
                            verif_errors[phone] = False
                            await client.send_message(bot, "1")
                            pass

                        emoji_to_file = {
                            "✋": VERIF_FIVE,
                            "👍": VERIF_UP,
                            "👎": VERIF_DOWN,
                            "✌️": VERIF_TWIN,
                            "✊": VERIF_FIST,
                            "🤟": VERIF_ROCK,
                            "☝️": VERIF_FINGER,
                        }

                        if not verif_errors[phone]:
                            found_emoji = next((emoji for emoji in emoji_to_file if emoji in message_text), None)
                            if found_emoji:
                                verif_errors[phone] = True
                                await client.send_file(event.chat_id, file=emoji_to_file[found_emoji], video_note=True)
                                logger.info(f"[{phone}] Авто-верификация у +{phone} прошла: {event.raw_text}")
                                await asyncio.sleep(10)
                            else:
                                logger.error(
                                    f"[{phone}] Авто-верификация у +{phone} не прошла, т.к. не найден эмодзи: {event.raw_text}")
                                verif_errors[phone] = True
                                emoji_errors[phone] = True
                                return
            except Exception as e:
                if verif_errors[phone] == False:
                    verif_errors[phone] = True
                    emoji_errors[phone] = True
                    logger.error(
                        f"[{phone}] Авто-верификация у +{phone} не прошла, а также произошла ошибка {e} || {event.raw_text}")

        @client.on(events.NewMessage(pattern=r'Лайк отправлен'))
        @client.on(events.NewMessage(pattern=r'Для верификации'))
        async def test_event(event):
            logger.info(f"[{phone}] ---------------------------------------")
            logger.info(f"[{phone}] Тестовый ивент отработал ('Лайк отправлен')")
            try:
                logger.info(f"[{phone}] observing_modes[{phone}]: {observing_modes[phone]}")
                logger.info(f"[{phone}] verif_errors[{phone}]: {verif_errors[phone]}")
                logger.info(f"[{phone}] emoji_errors[{phone}]: {emoji_errors[phone]}")
            except:
                logger.warning(f"[{phone}] Тестовый ивент вызвал ошибку")
            logger.info(f"[{phone}] ---------------------------------------")

        await like_people(phone, client, session_count, session_count_formal_contact, pool, informal_contact)
    except asyncio.CancelledError:
        # задача была отменена из main(): логируем и пробрасываем дальше
        logger.info(f"[{phone}] Получена отмена задачи, закрываем client...")
        raise
    except SessionPasswordNeededError:
        logger.error(f"[{phone}] Необходим пароль для двухфакторной аутентификации для {phone}")
    except Exception as e:
        logger.error(f"[{phone}] Ошибка для {phone}: {e}")
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
                logger.error("Ошибка при отправке сообщения в Telegram: %s", response.text)
                break
        except requests.exceptions.SSLError as e:
            logger.error("SSL ошибка при отправке сообщения: %s. Попытка %d из %d", e, attempt + 1, retries)
            attempt += 1
            time.sleep(delay)
        except Exception as e:
            logger.error("Неожиданная ошибка при отправке сообщения: %s", e)
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

        text_bb_bot = f"💤 Софт завершает свою работу."
        admin_bb_bot = f"💤 Софт завершает свою работу.\n🤝 Взаимок собрано за день: неформ {vzaimok} форм {vzaimok_formal}"

        logger.info(f"Время {BB_TIME}! Выполняется запланированное действие.")
        send_start_message(CHAT_ID, text_bb_bot, thread_id=4294972606)
        #send_start_message(CHAT_ID_rezerv, text_bb_bot)
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
    pool = await asyncpg.create_pool(DATABASE_DSN2, min_size=1, max_size=5)

    text_pre_bot = "⚠️ Софт запускается, в дв не заходим!"
    send_start_message(CHAT_ID, text_pre_bot, thread_id=4294972606)
    #send_start_message(CHAT_ID_rezerv, text_pre_bot)
    await asyncio.sleep(PRELAUNCH_MESSAGE_DELAY)

    phones = [f.split('.')[0] for f in os.listdir(SESSIONS_DIR) if f.endswith('.session')]
    if not phones:
        logger.error("Не найдено ни одной сессии в папке.")
        return

    phones_formal_contact = [f.split('.')[0] for f in os.listdir(fr"{SESSIONS_DIR}formal_contact/") if
                             f.endswith('.session')]
    if not phones_formal_contact:
        logger.error("Не найдено ни одной сессии в папке формальных аккаунтов.")
        if not phones:
            return

    session_count = len(phones)
    session_count_formal_contact = len(phones_formal_contact)
    text_start_bot = "✅ Софт запущен в стандартном режиме."
    send_start_message(CHAT_ID, text_start_bot, thread_id=4294972606)
    #send_start_message(CHAT_ID_rezerv, text_start_bot)

    text_start_admin_bot = f"✅ Софт запущен в стандартном режиме.\n\n📱 Количество сессий: {session_count} + для формального контакта {session_count_formal_contact}\n\npowered by AlgoApi"
    send_start_message(CHAT_ID_ADMIN, text_start_admin_bot)

    asyncio.create_task(perform_action_at_time(pool))
    tasks_informal_contact = [asyncio.create_task(process_session(phone, session_count, session_count_formal_contact, pool))
                              for phone in phones]
    tasks_formal_contact = [
        asyncio.create_task(process_session(phone, session_count, session_count_formal_contact, pool, informal_contact=False))
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
