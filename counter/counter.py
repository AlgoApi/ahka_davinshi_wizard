import telethon

from config import CHECK_TIME
from sessions_manager import *
from db_service import *
from calc_metric import *
from telethon.errors.rpcerrorlist import AuthKeyDuplicatedError

def effective_response_time(start: datetime, end: datetime) -> timedelta:
    """
    Вычисляет разницу между start и end за вычетом времени, попадающего в интервал с 00:00 до 08:00.
    Предполагается, что start и end имеют одинаковый tzinfo (или оба без tzinfo).
    """
    if end <= start:
        return timedelta(0)

    total_duration = end - start
    night_excluded = timedelta(0)

    current_date = start.date()
    end_date = end.date()

    # Итерируем по каждому дню от даты start до даты end включительно
    while current_date <= end_date:
        # Определяем ночной интервал для текущего дня (с 00:00 до 08:00)
        night_start = datetime.combine(current_date, time(0, 0), tzinfo=start.tzinfo)
        night_end = datetime.combine(current_date, time(8, 0), tzinfo=start.tzinfo)

        # Находим пересечение ночного интервала с [start, end]
        overlap_start = max(start, night_start)
        overlap_end = min(end, night_end)
        if overlap_start < overlap_end:
            night_excluded += (overlap_end - overlap_start)

        current_date += timedelta(days=1)

    effective_duration = total_duration - night_excluded
    return effective_duration

def load_session_config(phone):
    session_path = os.path.join(SESSIONS_DIR, f'{phone}.json')
    if os.path.exists(session_path):
        with open(session_path, 'r') as file:
            return json.load(file)
    else:
        logger.error(f"Файл конфигурации {session_path} не найден!")
        return None

async def check_favorites_for_new_messages(phone, client, entity, days=2):
    try:
        messages = await client.get_messages(entity, limit=100)
    except Exception as e:
        logger.error(f"Ошибка при получении сообщений для {phone}: {e}")
        return False

    local_tz = pytz.timezone("Europe/Moscow")
    now = datetime.now(local_tz)
    two_days_ago = now - timedelta(days=days)

    for message in messages:
        message_date = message.date.astimezone(local_tz)
        if message_date > two_days_ago:
            logger.info("Message date: %s", message_date.strftime("%Y-%m-%d %H:%M:%S"))
            return True

    return False

# ---------------------------
# Функция обработки одного клиента (аккаунта)
# ---------------------------
async def process_client(client: TelegramClient, phone: str, lite_mode: bool):
    """
    Используя уже подключённый клиент для аккаунта с номером phone,
    происходит:
      - Проверка наличия аккаунта в БД (создаётся, если отсутствует)
      - Получение 15 последних личных чатов (диалоги с одним пользователем)
      - Сбор новых сообщений (начиная с даты последнего сохранённого сообщения)
      - Вычисление времени ответа агента (если группа сообщений пользователя завершается сообщением агента)
      - Обновление полей last_message_date, pending_response и inactive для чата
      - Обновление last_active аккаунта
    """
    # Если аккаунт с данным номером отсутствует – создаём его
    account = db.query(Account).filter_by(Phone=phone).first()
    if not account:
        me = await client.get_me()
        user_name = me.username
        account = Account(
            Phone=phone,
            Username=user_name,
            LastActive=datetime.now(timezone.utc)
        )
        db.add(account)
        db.commit()
        logger.info(f"Аккаунт с телефоном {phone} добавлен в БД.")
    else:
        logger.info(f"Обработка аккаунта: {phone}")

    # Получаем диалоги – оставляем только личные (с пользователями)
    dialogs = await client.get_dialogs()
    private_dialogs = [d for d in dialogs if d.is_user]
    # Выбираем 50 последних чатов, сортируя по дате последнего сообщения (от новых к старым)
    private_dialogs = sorted(private_dialogs, key=lambda d: d.date, reverse=True)[:50]

    for dialog in private_dialogs:
        entity = dialog.entity
        if entity.bot:
            continue
        if entity.support:
            continue
        if entity.is_self:
            continue
        if entity.id == 777000:
            continue
        if not hasattr(entity, 'id'):
            continue
        tg_chat_id = entity.id

        # Поиск записи чата или создание новой
        chat_record = db.query(Chat).filter_by(ChatId=tg_chat_id, AccountId=account.Id).first()
        if not chat_record:
            name = ''
            if hasattr(entity, 'first_name'):
                name += entity.first_name or ''
            if hasattr(entity, 'last_name'):
                name += ' ' + (entity.last_name or '')
            chat_record = Chat(
                AccountId=account.Id,
                ChatId=tg_chat_id,
                Username=getattr(entity, 'username', None),
                Name=name.strip(),
                UserId=tg_chat_id,
                Phone=getattr(entity, 'phone', None),
                CreatedAt=None,
                LastMessageDate=None,
                Inactive=False,
                PendingResponse=False
            )
            db.add(chat_record)
            db.commit()
            db.refresh(chat_record)

        last_saved_date = chat_record.LastMessageDate
        # Загружаем новые сообщения: если чат новый – загружаем всю историю, иначе – сообщения после last_saved_date
        messages = []
        async for msg in client.iter_messages(entity, offset_date=last_saved_date, reverse=True):
            # 2) Пропустим ровно ту же дату (msg.date == last_saved_date),
            #    а также всё, что <= last_saved_date (на всякий случай):
            if not msg.date:
                continue
            elif last_saved_date and msg.date <= last_saved_date:
                continue
            messages.append(msg)
        if messages:
            chat_record.LastMessageDate = messages[-1].date
            db.commit()
            db.refresh(chat_record)

        if not messages:
            continue

        dt_telegram = None
        # Буфер для накопления подряд идущих сообщений пользователя до ответа агента
        user_msgs_buffer = []
        utc = pytz.UTC

        for msg in messages:
            sender = 'agent' if msg.out else 'user'
            msg_record = Message(
                ChatId=chat_record.Id,
                MessageId=msg.id,
                Sender=sender,
                Content=msg.message,
                Timestamp=msg.date,
                ResponseTime=None
            )
            # Если сообщение от агента и в буфере есть сообщения пользователя – вычисляем время ответа
            if sender == 'agent' and user_msgs_buffer:
                first_user_msg_time = user_msgs_buffer[0].date
                resp_time = effective_response_time(first_user_msg_time, msg.date)
                msg_record.ResponseTime = resp_time
                user_msgs_buffer = []  # очищаем буфер
            elif sender == 'user':
                user_msgs_buffer.append(msg)
            db.add(msg_record)
            # Обновляем last_message_date, если текущее сообщение новее
            #chat_record.LastMessageDate = chat_record.LastMessageDate.astimezone(timezone.utc)
            try:
                dt_telegram = msg.date.tzinfo
                if (chat_record.LastMessageDate is None) or (msg.date > chat_record.LastMessageDate):
                    chat_record.LastMessageDate = msg.date.astimezone(timezone.utc)
            except TypeError:
                dt_telegram = msg.date.tzinfo
                if (chat_record.LastMessageDate is None) or (msg.date > utc.localize(chat_record.LastMessageDate)):
                    chat_record.LastMessageDate = msg.date.astimezone(timezone.utc)
            # Если created_at не установлено – устанавливаем дату первого сообщения
            if chat_record.CreatedAt is None:
                chat_record.CreatedAt = msg.date.astimezone(timezone.utc)
            #chat_record.LastMessageDate = chat_record.LastMessageDate.astimezone(timezone.utc)
        chat_record.LastMessageDate = chat_record.LastMessageDate.astimezone(timezone.utc)
        db.commit()

        # Отметка чата: если последнее сообщение более 8 часов назад и не от агента, ставим pending_response
        now = datetime.now(dt_telegram)
        db.refresh(chat_record)
        #chat_record.LastMessageDate = chat_record.LastMessageDate.astimezone(timezone.utc)
        if chat_record.LastMessageDate and (now - chat_record.LastMessageDate > timedelta(hours=8)):
            last_msg = db.query(Message).filter_by(ChatId=chat_record.Id) \
                .order_by(Message.Timestamp.desc()).first()
            chat_record.PendingResponse = (last_msg and last_msg.Sender != 'agent')

        # Если обновлений в чате не было более 3 дней, помечаем его как неактивный
        if chat_record.LastMessageDate and (now - chat_record.LastMessageDate > timedelta(days=3)):
            chat_record.Inactive = True

        db.commit()

    # Обновляем last_active аккаунта – берем максимальную дату из всех чатов данного аккаунта
    latest = (
        db.query(func.max(Chat.LastMessageDate))
        .filter(Chat.AccountId == account.Id)
        .scalar()
    )

    # если чатов нет или все LastMessageDate = NULL, ставим «сейчас»
    latest = latest.astimezone(timezone.utc)
    account.LastActive = latest or datetime.now(timezone.utc)
    db.commit()

    agent_mapping = scope_sheet_data()

    if phone not in agent_mapping:
        logger.info(f"Для аккаунта {phone} агент не найден в Google Sheets")

    agent_name_gs = agent_mapping[phone]["name"]

    # Ищем агента по имени, если не найден – создаём нового
    agent = db.query(Agent).filter(Agent.Name == agent_name_gs).first()
    if not agent:
        agent = Agent(Name=agent_name_gs, LastActive=datetime.now(timezone.utc))
        db.add(agent)
        db.commit()  # чтобы получить agent.id
        logger.info(f"Создан новый агент: {agent_name_gs}")

    # Если аккаунт уже привязан к агенту с таким же agent_id, изменений не вносим.
    if account.AgentId != agent.Id:
        # Если привязки нет или привязан к другому агенту, обновляем привязку
        account.AgentId = agent.Id

        # Обновляем показатель LastActive агента, если активность аккаунта новее
        if account.LastActive and (not agent.LastActive or account.LastActive > agent.LastActive):
            agent.LastActive = account.LastActive

        db.commit()
        logger.info(f"Аккаунт {phone} привязан к агенту {agent_name_gs}.")

    return dialogs

def format_structure(obj, indent: int = 0) -> str:
    """
    Рекурсивно форматирует obj, который может быть:
    - dict: превращается в блок «ключ: ...»
    - list/tuple: каждый элемент выводится в строке с «- »
    - остальное: просто строка
    """
    lines = []
    prefix = " " * indent

    if isinstance(obj, dict):
        for key, val in obj.items():
            # заголовок блока
            lines.append(f"{prefix}{key}:")
            # рекурсивно форматируем значение с большим отступом
            lines.append(format_structure(val, indent + 2))
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            # для каждого элемента списка
            if isinstance(item, (dict, list, tuple)):
                # вложенная структура — рекурсия
                lines.append(f"{prefix}-:")
                lines.append(format_structure(item, indent + 2))
            else:
                lines.append(f"{prefix}- {item}")
    else:
        # базовый тип
        lines.append(f"{prefix}{obj}")

    return "\n".join(lines)

async def check_inactive_phones():
    pool = await asyncpg.create_pool(DATABASE_DSN, min_size=1, max_size=5)
    phones = [f.split('.')[0] for f in os.listdir(SESSIONS_DIR) if f.endswith('.session')]


    addition_queue = set()
    disactivate_queue = set()

    agent_mapping = scope_sheet_data()

    for phone in phones:
        if not agent_mapping.get(phone):
            disactivate_queue.add(phone)
            continue
        if not agent_mapping.get(phone).get("active"):
            disactivate_queue.add(phone)

    for phone in agent_mapping:
        if agent_mapping.get(phone).get("active") and phone not in phones:
           addition_queue.add(phone)

    if len(addition_queue) > 0:
        log = format_structure(add_files(addition_queue, 1))
        send_telegram_message(f"addition_queue code 1: \n{log}")
        log = format_structure(add_files(addition_queue, 0))
        send_telegram_message(f"addition_queue code 0: \n{log}")
        del log
    if len(disactivate_queue) > 0:
        log = format_structure(move_files(disactivate_queue, 1))
        send_telegram_message(f"move_files code 1: \n{log}")
        log = format_structure(move_files(disactivate_queue, 0))
        send_telegram_message(f"move_files code 0: \n{log}")
        del log

    inactive_phones = []
    debug = os.listdir(SESSIONS_DIR)
    phones = [f.split('.')[0] for f in os.listdir(SESSIONS_DIR) if f.endswith('.session')]

    if not phones:
        logger.error("Не найдено ни одной сессии в папке.")
        return

    for phone in phones:
        config = load_session_config(phone)
        if not config:
            continue

        api_id = config.get('app_id')
        api_hash = config.get('app_hash')
        session_file = os.path.join(SESSIONS_DIR, f'{phone}.session')
        lite = config.get('lite_mode')
        client = TelegramClient(
            session_file,
            api_id,
            api_hash,
            device_model=config.get('device_model'),
            system_version=config.get('system_version'),
            app_version='8.4',
            connection_retries=52,
            request_retries=52
        )
        code_requst = False
        login_check = True
        try:
            def prevent_code_request():
                nonlocal code_requst
                code_requst = True
                raise IOError(f"требует код, проверьте сессии")
            all_dialogs = None
            try:
                await client.start(phone=phone, code_callback=prevent_code_request)
            except AuthKeyDuplicatedError:
                login_check = False
                continue
            if await client.is_user_authorized():
                if not lite:
                    chat_my_dreems = await get_or_create_group(client, "My dreems", pool, phone)
                    chat_my_mind = await get_or_create_group(client, "My mind", pool, phone)
                    has_new_messages = await check_favorites_for_new_messages(phone, client, chat_my_dreems)
                    if not has_new_messages:
                        me = await client.get_me()
                        user_name = me.username
                        inactive_phones.append(f"+{phone} | @{user_name}")
                    has_new_messages = await check_favorites_for_new_messages(phone, client, chat_my_mind, days=4)
                    if not has_new_messages:
                        me = await client.get_me()
                        user_name = me.username
                        inactive_phones.append(f"+{phone} | @{user_name} | ashqua")

                all_dialogs = await process_client(client, phone, lite)
            else:
                logger.error(f"[{phone}] Не удалось авторизоваться.")
        except Exception as e:
            logger.error(f"[{phone}] Ошибка: {e}")
        except AuthKeyDuplicatedError:
            login_check = False
            continue
        finally:
            if login_check:
                if not code_requst:
                    # После обработки всех аккаунтов вычисляем метрики для каждого аккаунта
                    try:
                        if not lite:
                            account = db.query(Account).filter_by(Phone=phone).first()
                            account_metrics = await compute_metrics_for_account(account, client, all_dialogs, phone, pool, db)
                            store_metrics('account', account.Id, account_metrics, db)
                            logger.info(f"Метрики для аккаунта {account.Phone} сохранены.")
                    except Exception as e:
                        logger.error(f"[{phone}] Ошибка: {e}")
                    finally:
                        pass
                await client.disconnect()
            else:
                continue

    await pool.close()



    # Вычисляем агрегированные метрики для каждого агента
    agents = db.query(Agent).all()
    for agent in agents:
        agent_metrics = compute_metrics_for_agent(agent, db)
        store_metrics('agent', agent.Id, agent_metrics, db)
        logger.info(f"Метрики для агента {agent.Name} сохранены.")

    if inactive_phones:
        text = "😣Подозрения на тени:\n" + "\n".join(inactive_phones)
        send_telegram_message(text)
    else:
        send_telegram_message("🤩Теней нет!")

async def perform_action_at_time():
    target_time = datetime.strptime(CHECK_TIME, "%H:%M").time()

    while True:
        now = datetime.now()
        target_datetime = datetime.combine(now.date(), target_time)

        if target_datetime < now:
            target_datetime += timedelta(days=1)

        remaining_seconds = (target_datetime - now).total_seconds()
        await asyncio.sleep(remaining_seconds)

        logger.info(f"Время {CHECK_TIME}! Выполняется запланированное действие.")
        await check_inactive_phones()

async def run_continuous_check():
    while True:
        await perform_action_at_time()
        await asyncio.sleep(86400)

def send_start_message(chat_id, text, retries=3, delay=2):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": text
    }

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

async def main():
    text_start_admin_bot = f"✅ Чекер теней запущен"
    send_start_message(CHAT_ID_ADMIN, text_start_admin_bot)

    await run_continuous_check()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("bye :)")
