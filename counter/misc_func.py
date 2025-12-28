import asyncio
import re
from telethon import errors
from telethon.tl.functions.messages import CreateChatRequest
from telethon import TelegramClient
import asyncpg
from logger_service import logger


def normalize_phone(phone1: str) -> str:
    # Убираем все нецифровые символы
    digits = re.sub(r'\D', '', phone1)

    # Заменяем +7 на 7 (если он есть)
    if digits.startswith("8"):
        digits = "7" + digits[1:]
    elif digits.startswith("7"):
        pass  # уже в нужном формате
    else:
        return "None"  # Некорректный номер

    return digits

async def get_group_id(pool: asyncpg.Pool, phone_number: str) -> int | None:
    """
    Если для phone_number в таблице есть запись — вернёт group_id, иначе None.
    """
    row = await pool.fetchrow(
        "SELECT group_id FROM tg_groups WHERE phone_number = $1",
        phone_number
    )
    return row['group_id'] if row else None

async def set_group_id(pool: asyncpg.Pool, phone_number: str, group_id: int):
    """
    Вставляет новую запись или обновляет старую по conflict по phone_number.
    """
    await pool.execute("""
        INSERT INTO tg_groups (phone_number, group_id)
        VALUES ($1, $2)
        ON CONFLICT (phone_number) DO
          UPDATE SET group_id = EXCLUDED.group_id
    """, phone_number, group_id)

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
    gid = await get_group_id(pool, phone_number)
    if gid:
        try:
            grp = await client.get_entity(gid)
            logger.info("нашёл группу из бд")
            return grp
        except Exception:
            logger.error("ошибка получения id группы для взаимок с бд, продолжаем дальше")
            # если вдруг уже удалили/сменили — сбросим и пойдём дальше
            await pool.execute("DELETE FROM tg_groups WHERE phone_number = $1", phone_number)

    # 2) ищем в последних группах (быстро) по title
    group = await find_group(client, group_title)
    if group:
        # …сохраняем в БД и возвращаем
        await set_group_id(pool, phone_number, group.id)
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
                await set_group_id(pool, phone_number, created.id)
                logger.info("создали новою группу")
                return created
            except Exception:
                # либо нет поля .updates, либо .updates.chats пуст
                pass

            await asyncio.sleep(2)
            new_group = await find_group(client, group_title)
            if new_group:
                # …сохраняем в БД и возвращаем
                await set_group_id(pool, phone_number, new_group.id)
                logger.info("создали новою группу")
                return new_group

        except errors.FloodWaitError as e:
            logger.warning("ФЛУД получения новой группы ждём")
            await asyncio.sleep(e.seconds + 1)
            # после сна ещё раз пробуем найти в недавних группах
            group = await find_group(client, group_title)
            if group:
                # …сохраняем в БД и возвращаем
                await set_group_id(pool, phone_number, group.id)
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
