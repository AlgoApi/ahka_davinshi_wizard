import asyncio
import logging
import os
import json
import re
import requests
import sys
import pytz
import gspread
import time
from google.oauth2.service_account import Credentials
from config import LOG_FILE, CHAT_ID_ADMIN, BOT_TOKEN, CHECK_TIME
from datetime import datetime, timezone, timedelta, time
from telethon import TelegramClient
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Interval, ForeignKey, Text, Float, \
    BigInteger
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl, MessageEntityMentionName
from sqlalchemy.orm import sessionmaker, declarative_base
from pytz import timezone as tz

import os
import json
import asyncio
import re
import socks
from datetime import datetime
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetDialogsRequest, GetHistoryRequest
from telethon.tl.types import InputPeerEmpty


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


# ---------------------------
# Подключение к Google Sheets (только для чтения)
# ---------------------------
# Задаём область доступа
scope = ['https://www.googleapis.com/auth/spreadsheets.readonly']
# Путь к файлу сервисного аккаунта (скачанный JSON с Google Cloud Console)
creds = Credentials.from_service_account_file('affable-ace-453114-d7-2c7d4db15b8a.json', scopes=scope)
gs_client = gspread.authorize(creds)

# Открываем таблицу по её ID (ключу)
sheet_id = "1Vy-Q7Ro3H7oSynUSCaC7rWXlNXeowbqakofvc63ICwo"  # замените на ID вашей таблицы
sheet = gs_client.open_by_key(sheet_id)
worksheet = sheet.get_worksheet(0)  # читаем первый лист

# Получаем все записи из таблицы как список словарей
records = worksheet.get_all_records()

# Формируем словарь: номер телефона -> имя агента
agent_mapping = {}
for record in records:
    phone = normalize_phone(str(record.get("Number")).strip())  # например, "+1234567890"
    agent_name = str(record.get("Username")).strip()  # например, "AgentName"
    agent_mapping[phone] = agent_name

# ---------------------------
# Определение схемы базы данных
# ---------------------------
Base = declarative_base()


class Agent(Base):
    __tablename__ = 'Agents'
    Id = Column(Integer, primary_key=True)
    Name = Column(String)
    LastActive = Column(DateTime)
    # Дополнительные метрики можно добавить по необходимости


class Account(Base):
    __tablename__ = 'Accounts'
    Id = Column(Integer, primary_key=True)
    AgentId = Column(Integer, ForeignKey('Agents.Id'), nullable=True)  # Если аккаунт привязан к агенту
    Phone = Column(String, unique=True)  # Номер телефона аккаунта
    Username = Column(String, nullable=True)
    LastActive = Column(DateTime)


class Chat(Base):
    __tablename__ = 'Chats'
    Id = Column(Integer, primary_key=True)
    AccountId = Column(Integer, ForeignKey('Accounts.Id'))
    ChatId = Column(BigInteger)  # ID чата в Telegram
    Username = Column(String)
    Name = Column(String)
    UserId = Column(BigInteger)
    Phone = Column(String)
    CreatedAt = Column(DateTime)  # Время первого обнаруженного сообщения в чате
    LastMessageDate = Column(DateTime)  # Время самого нового сообщения в чате
    Inactive = Column(Boolean, default=False)
    PendingResponse = Column(Boolean, default=False)  # Флаг, если последнее сообщение не от агента и прошло >8 часов


class Message(Base):
    __tablename__ = 'Messages'
    Id = Column(Integer, primary_key=True)
    ChatId = Column(Integer, ForeignKey('Chats.Id'))
    MessageId = Column(BigInteger)  # ID сообщения в Telegram
    Sender = Column(String)  # 'agent' или 'user'
    Content = Column(Text)
    Timestamp = Column(DateTime)
    ResponseTime = Column(Interval, nullable=True)  # Время ответа агента (если применимо)


class Metrics(Base):
    __tablename__ = 'Metrics'
    Id = Column(Integer, primary_key=True)
    EntityType = Column(String)  # 'account' или 'agent'
    EntityId = Column(Integer)  # id аккаунта или агента
    MetricDate = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    AvgNewMsgsLastWeek = Column(Float)  # Среднее число новых сообщений в день за последнюю неделю
    AvgNewChatsLastWeek = Column(Float)  # Среднее число новых чатов в день за последнюю неделю
    NewMsgsLastDay = Column(Integer)  # Количество новых сообщений за последний день
    NewChatsLastDay = Column(Integer)  # Количество новых чатов за последний день
    AvgResponseTimeLastDay = Column(Interval, nullable=True)  # Среднее время отклика за последний день
    AvgResponseTimeLastWeek = Column(Interval, nullable=True)  # Среднее время отклика за последнюю неделю
    NewVzaimkiMsgsLastDay = Column(Integer)
    NewVzaimkiMsgsLastWeek = Column(Integer)
    AvgVzaimkiMsgsLastWeek = Column(Float)
    NewValidDialogsLastDay = Column(Integer)
    NewValidDialogsLastWeek = Column(Integer)
    AvgValidDialogsLastWeek = Column(Float)
    OutdatedDialogsLastWeek = Column(Integer)


def extract_username_from_message(message) -> str | int | None:
    """
    Извлекает username из гиперссылки в сообщении.
    Если среди сущностей есть MessageEntityTextUrl (или MessageEntityUrl),
    и его URL начинается с "https://t.me/", то извлекается имя пользователя.
    """
    if not message.entities:
        return None

    for entity in message.entities:
        # Если ссылка оформлена как гиперссылка (TextUrl)
        if isinstance(entity, MessageEntityTextUrl):
            url = entity.url
            if url.startswith("https://t.me/"):
                # Извлекаем часть URL после "https://t.me/"
                username = url.split("https://t.me/")[-1]
                return username
        # Если гиперссылка не оформлена как TextUrl, а как Url
        elif isinstance(entity, MessageEntityUrl):
            # Получаем сам текст, который соответствует сущности
            url = message.text[entity.offset:entity.offset + entity.length]
            if url.startswith("https://t.me/"):
                username = url.split("https://t.me/")[-1]
                return username

        elif isinstance(entity, MessageEntityMentionName):
            return entity.user_id
    return None


# ---------------------------
# Настройка подключения к базе данных
# ---------------------------
DATABASE_URL = 'postgresql://logibn:pass@ip:port/maindb'
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
db = SessionLocal()

# УБРАТЬ ПОСЛЕ ПЕРВОГО ЗАПУСКА
Base.metadata.create_all(bind=engine)


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


# ---------------------------
# Функция обработки одного клиента (аккаунта)
# ---------------------------
async def process_client(client: TelegramClient, phone: str):
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
        print(f"Аккаунт с телефоном {phone} добавлен в БД.")
    else:
        print(f"Обработка аккаунта: {phone}")

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

        last_saved_date = chat_record.LastMessageDate

        # Загружаем новые сообщения: если чат новый – загружаем всю историю, иначе – сообщения после last_saved_date
        messages = []
        async for msg in client.iter_messages(entity, offset_date=last_saved_date, reverse=True):
            messages.append(msg)
        if not messages:
            continue

        # Буфер для накопления подряд идущих сообщений пользователя до ответа агента
        user_msgs_buffer = []
        utc = pytz.UTC
        first = False
        for msg in messages:
            if not first:
                chat_record.CreatedAt = msg.date
                first = True
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
                print("resp_time debug")
                print(f"resp_time: {resp_time}")
                print(f"first_user_msg_time: {first_user_msg_time}")
                print(f"msg.date: {msg.date}")
            elif sender == 'user':
                user_msgs_buffer.append(msg)
            db.add(msg_record)
            # Обновляем last_message_date, если текущее сообщение новее
            try:
                if (chat_record.LastMessageDate is None) or (msg.date > chat_record.LastMessageDate):
                    chat_record.LastMessageDate = msg.date
            except TypeError:
                if (chat_record.LastMessageDate is None) or (msg.date > utc.localize(chat_record.LastMessageDate)):
                    chat_record.LastMessageDate = msg.date
            # Если created_at не установлено – устанавливаем дату первого сообщения
            if chat_record.CreatedAt is None:
                chat_record.CreatedAt = msg.date

        db.commit()

        # Отметка чата: если последнее сообщение более 8 часов назад и не от агента, ставим pending_response
        now = datetime.now(timezone.utc)
        if chat_record.LastMessageDate and (
                now - chat_record.LastMessageDate.replace(tzinfo=tz('UTC')) > timedelta(hours=8)):
            last_msg = db.query(Message).filter_by(ChatId=chat_record.Id) \
                .order_by(Message.Timestamp.desc()).first()
            chat_record.PendingResponse = (last_msg and last_msg.Sender != 'agent')
            db.commit()

        # Если обновлений в чате не было более 3 дней, помечаем его как неактивный
        if chat_record.LastMessageDate and (
                now - chat_record.LastMessageDate.replace(tzinfo=tz('UTC')) > timedelta(days=3)):
            chat_record.Inactive = True
            db.commit()

    # Обновляем last_active аккаунта – берем максимальную дату из всех чатов данного аккаунта
    chats = db.query(Chat).filter_by(AccountId=account.Id).all()
    latest = None
    for ch in chats:
        if ch.LastMessageDate and (latest is None or ch.LastMessageDate > latest):
            latest = ch.LastMessageDate
    account.LastActive = latest if latest else datetime.now(timezone.utc)
    db.commit()

    if phone not in agent_mapping:
        print(f"Для аккаунта {phone} агент не найден в Google Sheets")

    agent_name_gs = agent_mapping[phone]

    # Ищем агента по имени, если не найден – создаём нового
    agent = db.query(Agent).filter(Agent.Name == agent_name_gs).first()
    if not agent:
        agent = Agent(Name=agent_name_gs, LastActive=datetime.now(timezone.utc))
        db.add(agent)
        db.commit()  # чтобы получить agent.id
        print(f"Создан новый агент: {agent_name_gs}")

    # Если аккаунт уже привязан к агенту с таким же agent_id, изменений не вносим.
    if account.AgentId != agent.Id:
        # Если привязки нет или привязан к другому агенту, обновляем привязку
        account.AgentId = agent.Id

        # Обновляем показатель LastActive агента, если активность аккаунта новее
        if account.LastActive and account.LastActive != agent.LastActive:
            agent.LastActive = account.LastActive

        db.commit()
        print(f"Аккаунт {phone} привязан к агенту {agent_name_gs}.")

    return dialogs


# ---------------------------
# Функции вычисления и сохранения метрик
# ---------------------------
async def compute_metrics_for_account(account: Account, client: TelegramClient, all_dialogs):
    """
    Вычисляет метрики для аккаунта:
      - Среднее число новых сообщений в день за последнюю неделю
      - Среднее число новых чатов в день за последнюю неделю
      - Количество новых сообщений за последний день
      - Количество новых чатов за последний день
      - Среднее время отклика агента за последний день и неделю
    """
    now = datetime.now(timezone.utc)
    # Начало предыдущего дня (00:00:00)
    one_day_ago = datetime.combine((now - timedelta(days=1)).date(), time.min, tzinfo=timezone.utc)

    # Начало дня 7 дней назад (00:00:00)
    seven_days_ago = datetime.combine((now - timedelta(days=7)).date(), time.min, tzinfo=timezone.utc)

    # Получаем список id чатов аккаунта
    chat_ids = [chat.Id for chat in db.query(Chat).filter_by(AccountId=account.Id).all()]

    new_msgs_last_day = db.query(Message).filter(Message.ChatId.in_(chat_ids),
                                                 Message.Timestamp >= one_day_ago).count()
    new_msgs_last_week = db.query(Message).filter(Message.ChatId.in_(chat_ids),
                                                  Message.Timestamp >= seven_days_ago).count()
    avg_new_msgs_last_week = new_msgs_last_week / 7.0

    new_chats_last_day = db.query(Chat).filter(Chat.AccountId == account.Id,
                                               Chat.CreatedAt >= one_day_ago).count()
    new_chats_last_week = db.query(Chat).filter(Chat.AccountId == account.Id,
                                                Chat.CreatedAt >= seven_days_ago).count()
    avg_new_chats_last_week = new_chats_last_week / 7.0

    agent_msgs_last_day = db.query(Message).filter(
        Message.ChatId.in_(chat_ids),
        Message.Sender == 'agent',
        Message.Timestamp >= one_day_ago).all()
    if agent_msgs_last_day:
        total_response_time_day = sum(
            [msg.ResponseTime.total_seconds() for msg in agent_msgs_last_day if msg.ResponseTime is not None])
        avg_response_time_last_day = timedelta(seconds=(total_response_time_day / len(agent_msgs_last_day)))
    else:
        avg_response_time_last_day = None

    agent_msgs_last_week = db.query(Message).filter(
        Message.ChatId.in_(chat_ids),
        Message.Sender == 'agent',
        Message.Timestamp >= seven_days_ago).all()
    if agent_msgs_last_week:
        total_response_time_week = sum(
            [msg.ResponseTime.total_seconds() for msg in agent_msgs_last_week if msg.ResponseTime is not None])
        avg_response_time_last_week = timedelta(seconds=(total_response_time_week / len(agent_msgs_last_week)))
    else:
        avg_response_time_last_week = None

    # Получаем избранные сообщения.
    # Предполагается, что избранное – это чат с id=777000

    mutual_msgs_last_day = []
    mutual_msgs_last_week = []

    # Множества для уникальных диалогов, которые "засчитаны" (имеют >=2 сообщения от агента)
    valid_dialogs_last_day = set()
    valid_dialogs_last_week = set()
    outdated_dialogs_last_week = set()

    async for msg in client.iter_messages("me", offset_date=one_day_ago, reverse=True):
        if not msg.text:
            continue

        # Фильтруем сообщения, начинающиеся с нужного текста.
        if not msg.text.startswith("Есть взаимная симпатия!") and not msg.text.startswith(
                "Отлично! Надеюсь хорошо проведете время ;)"):
            continue

        # Пробуем извлечь имя с помощью регулярного выражения.
        extracted_name = extract_username_from_message(msg)

        if msg.date < seven_days_ago:
            continue

        # Ищем в базе диалог (chat) с этим человеком по имени.
        # Здесь можно настроить фильтрацию: например, ищем записи в таблице Chat, где поле name содержит extracted_name.

        target_dialog = None

        if isinstance(extracted_name, int):
            chat = db.query(Chat).filter(Chat.UserId == extracted_name).first()
            for dialog in all_dialogs:
                # Проверяем, что это личный чат (не группа, не канал)
                if dialog.is_user:
                    # Если у сущности есть username и он совпадает с целевым
                    if getattr(dialog.entity, 'id', None) == extracted_name:
                        target_dialog = dialog
                        break
        else:
            chat = db.query(Chat).filter(Chat.Username.ilike(f"%{extracted_name}%")).first()
            for dialog in all_dialogs:
                # Проверяем, что это личный чат (не группа, не канал)
                if dialog.is_user:
                    # Если у сущности есть username и он совпадает с целевым
                    if getattr(dialog.entity, 'username', None) == extracted_name:
                        target_dialog = dialog
                        break

        if chat:
            utc = pytz.UTC
            if utc.localize(chat.CreatedAt) >= msg.date:
                # Учитываем сообщение по дате.
                if msg.date >= one_day_ago:
                    mutual_msgs_last_day.append(msg)
                elif msg.date >= seven_days_ago:
                    mutual_msgs_last_week.append(msg)
                outdated = False
            else:
                outdated = True
                outdated_dialogs_last_week.add(chat.ChatId)
            if target_dialog:
                if target_dialog.date < seven_days_ago:
                    outdated_dialogs_last_week.add(target_dialog.id)
                else:
                    # Считаем, сколько сообщений от агента в найденном чате.
                    agent_msg_count = db.query(Message).filter(Message.ChatId == chat.Id,
                                                               Message.Sender == 'agent').count()
                    if agent_msg_count >= 2:
                        # Если сообщение попадает в период, засчитываем этот диалог.
                        if msg.date >= one_day_ago:
                            valid_dialogs_last_day.add(chat.Id)
                        elif msg.date >= seven_days_ago:
                            valid_dialogs_last_week.add(chat.Id)
            else:
                if not outdated:
                    # Считаем, сколько сообщений от агента в найденном чате.
                    agent_msg_count = db.query(Message).filter(Message.ChatId == chat.Id,
                                                               Message.Sender == 'agent').count()
                    if agent_msg_count >= 2:
                        # Если сообщение попадает в период, засчитываем этот диалог.
                        if msg.date >= one_day_ago:
                            valid_dialogs_last_day.add(chat.Id)
                        elif msg.date >= seven_days_ago:
                            valid_dialogs_last_week.add(chat.Id)
        else:
            if target_dialog:
                first_message = await client.get_messages(target_dialog, limit=1, reverse=True)
                if first_message:
                    first_message = first_message[0]
                    if first_message.date >= msg.date:
                        # Учитываем сообщение по дате.
                        if msg.date >= one_day_ago:
                            mutual_msgs_last_day.append(msg)
                        elif msg.date >= seven_days_ago:
                            mutual_msgs_last_week.append(msg)
                    if target_dialog.date < seven_days_ago:
                        outdated_dialogs_last_week.add(target_dialog.id)
            else:
                # Учитываем сообщение по дате.
                if msg.date >= one_day_ago:
                    mutual_msgs_last_day.append(msg)
                elif msg.date >= seven_days_ago:
                    mutual_msgs_last_week.append(msg)

    # Вычисляем показатели:
    new_vzaimki_msgs_last_day = len(mutual_msgs_last_day)
    avg_vzaimki_msgs_last_week = len(mutual_msgs_last_week) / 7.0
    new_valid_dialogs_last_day = len(valid_dialogs_last_day)
    avg_valid_dialogs_last_week = len(valid_dialogs_last_week) / 7.0
    new_valid_dialogs_last_week = len(valid_dialogs_last_week)
    new_vzaimki_msgs_last_week = len(mutual_msgs_last_week)

    return {
        'AvgNewMsgsLastWeek': avg_new_msgs_last_week,
        'AvgNewChatsLastWeek': avg_new_chats_last_week,
        'NewMsgsLastDay': new_msgs_last_day,
        'NewChatsLastDay': new_chats_last_day,
        'AvgResponseTimeLastDay': avg_response_time_last_day,
        'AvgResponseTimeLastWeek': avg_response_time_last_week,
        'new_vzaimki_msgs_last_day': new_vzaimki_msgs_last_day,
        'avg_vzaimki_msgs_last_week': avg_vzaimki_msgs_last_week,
        'new_valid_dialogs_last_day': new_valid_dialogs_last_day,
        'avg_valid_dialogs_last_week': avg_valid_dialogs_last_week,
        'new_valid_dialogs_last_week': new_valid_dialogs_last_week,
        'new_vzaimki_msgs_last_week': new_vzaimki_msgs_last_week,
        'outdated_dialogs_last_week': len(outdated_dialogs_last_week)
    }


def compute_metrics_for_agent(agent: Agent):
    """
    Вычисляет агрегированные метрики для агента по всем его аккаунтам.
    """
    now = datetime.now(timezone.utc)
    # Начало предыдущего дня (00:00:00)
    one_day_ago = datetime.combine((now - timedelta(days=1)).date(), time.min, tzinfo=timezone.utc)

    # Начало дня 7 дней назад (00:00:00)
    seven_days_ago = datetime.combine((now - timedelta(days=7)).date(), time.min, tzinfo=timezone.utc)

    accounts = db.query(Account).filter_by(AgentId=agent.Id).all()
    account_ids = [acc.Id for acc in accounts]
    chat_ids = [chat.Id for chat in db.query(Chat).filter(Chat.AccountId.in_(account_ids)).all()]

    new_msgs_last_day = db.query(Message).filter(Message.ChatId.in_(chat_ids),
                                                 Message.Timestamp >= one_day_ago).count()
    new_msgs_last_week = db.query(Message).filter(Message.ChatId.in_(chat_ids),
                                                  Message.Timestamp >= seven_days_ago).count()
    avg_new_msgs_last_week = new_msgs_last_week / 7.0

    new_chats_last_day = db.query(Chat).filter(Chat.AccountId.in_(account_ids),
                                               Chat.CreatedAt >= one_day_ago).count()
    new_chats_last_week = db.query(Chat).filter(Chat.AccountId.in_(account_ids),
                                                Chat.CreatedAt >= seven_days_ago).count()
    avg_new_chats_last_week = new_chats_last_week / 7.0

    agent_msgs_last_day = db.query(Message).filter(
        Message.ChatId.in_(chat_ids),
        Message.Sender == 'agent',
        Message.Timestamp >= one_day_ago).all()
    if agent_msgs_last_day:
        total_response_time_day = sum(
            [msg.ResponseTime.total_seconds() for msg in agent_msgs_last_day if msg.ResponseTime is not None])
        avg_response_time_last_day = timedelta(seconds=(total_response_time_day / len(agent_msgs_last_day)))
    else:
        avg_response_time_last_day = None

    agent_msgs_last_week = db.query(Message).filter(
        Message.ChatId.in_(chat_ids),
        Message.Sender == 'agent',
        Message.Timestamp >= seven_days_ago).all()
    if agent_msgs_last_week:
        total_response_time_week = sum(
            [msg.ResponseTime.total_seconds() for msg in agent_msgs_last_week if msg.ResponseTime is not None])
        avg_response_time_last_week = timedelta(seconds=(total_response_time_week / len(agent_msgs_last_week)))
    else:
        avg_response_time_last_week = None

    total_new_vzaimki_msgs_day = 0
    total_new_vzaimki_msgs_week = 0
    total_new_valid_dialogs_day = 0
    total_new_valid_dialogs_week = 0
    total_outdated_dialogs_last_week = 0

    for acc in accounts:
        account_metric = (
            db.query(Metrics)
            .filter(Metrics.EntityType == 'account', Metrics.EntityId == acc.Id)
            .order_by(Metrics.MetricDate.desc())
            .first()
        )
        if account_metric:
            total_new_vzaimki_msgs_day += account_metric.NewVzaimkiMsgsLastDay or 0
            total_new_vzaimki_msgs_week += account_metric.NewVzaimkiMsgsLastWeek or 0
            total_new_valid_dialogs_day += account_metric.NewValidDialogsLastDay or 0
            total_new_valid_dialogs_week += account_metric.NewValidDialogsLastWeek or 0
            total_outdated_dialogs_last_week += account_metric.OutdatedDialogsLastWeek or 0

        # Вычисляем средние значения за неделю (считаем, что неделя = 7 дней)
    avg_vzaimki_msgs_week = total_new_vzaimki_msgs_week / 7.0 if total_new_vzaimki_msgs_week else 0.0
    avg_valid_dialogs_week = total_new_valid_dialogs_week / 7.0 if total_new_valid_dialogs_week else 0.0

    return {
        'AvgNewMsgsLastWeek': avg_new_msgs_last_week,
        'AvgNewChatsLastWeek': avg_new_chats_last_week,
        'NewMsgsLastDay': new_msgs_last_day,
        'NewChatsLastDay': new_chats_last_day,
        'AvgResponseTimeLastDay': avg_response_time_last_day,
        'AvgTesponseTimeLastWeek': avg_response_time_last_week,
        'new_vzaimki_msgs_last_day': total_new_vzaimki_msgs_day,
        'avg_vzaimki_msgs_last_week': avg_vzaimki_msgs_week,
        'new_valid_dialogs_last_day': total_new_valid_dialogs_day,
        'avg_valid_dialogs_last_week': avg_valid_dialogs_week,
        'new_valid_dialogs_last_week': total_new_valid_dialogs_week,
        'new_vzaimki_msgs_last_week': total_new_vzaimki_msgs_week,
        'outdated_dialogs_last_week': total_outdated_dialogs_last_week
    }


def store_metrics(entity_type: str, entity_id: int, metrics_data: dict):
    """
    Сохраняет вычисленные метрики в таблицу Metrics.
    """
    now = datetime.now(timezone.utc)
    three_weeks_ago = datetime.combine((now - timedelta(weeks=3)).date(), time.min, tzinfo=timezone.utc)

    # Удаление записей старше 3 недель
    db.query(Metrics).filter(Metrics.MetricDate < three_weeks_ago).delete(synchronize_session=False)

    m = Metrics(
        EntityType=entity_type,
        EntityId=entity_id,
        MetricDate=datetime.now(timezone.utc),
        AvgNewMsgsLastWeek=metrics_data.get('AvgNewMsgsLastWeek'),
        AvgNewChatsLastWeek=metrics_data.get('AvgNewChatsLastWeek'),
        NewMsgsLastDay=metrics_data.get('NewMsgsLastDay'),
        NewChatsLastDay=metrics_data.get('NewChatsLastDay'),
        AvgResponseTimeLastDay=metrics_data.get('AvgResponseTimeLastDay'),
        AvgResponseTimeLastWeek=metrics_data.get('AvgResponseTimeLastWeek'),
        NewVzaimkiMsgsLastDay=metrics_data.get("new_vzaimki_msgs_last_day"),
        AvgVzaimkiMsgsLastWeek=metrics_data.get("avg_vzaimki_msgs_last_week"),
        NewValidDialogsLastDay=metrics_data.get("new_valid_dialogs_last_day"),
        AvgValidDialogsLastWeek=metrics_data.get("avg_valid_dialogs_last_week"),
        NewValidDialogsLastWeek=metrics_data.get("new_valid_dialogs_last_week"),
        NewVzaimkiMsgsLastWeek=metrics_data.get("new_vzaimki_msgs_last_week"),
        OutdatedDialogsLastWeek=metrics_data.get('outdated_dialogs_last_week')
    )
    db.add(m)
    db.commit()


SESSIONS_DIR = './sessions_checker/'
LOG_FILE = './log/checker.log'


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


def load_session_config(phone):
    session_path = os.path.join(SESSIONS_DIR, f'{phone}.json')
    if os.path.exists(session_path):
        with open(session_path, 'r') as file:
            return json.load(file)
    else:
        logger.error(f"Файл конфигурации {session_path} не найден!")
        return None


async def check_favorites_for_new_messages(phone, client):
    try:
        messages = await client.get_messages("me", limit=100)
    except Exception as e:
        logger.error(f"Ошибка при получении сообщений для {phone}: {e}")
        return False

    local_tz = pytz.timezone("Europe/Moscow")
    now = datetime.now(local_tz)
    two_days_ago = now - timedelta(days=2)

    for message in messages:
        message_date = message.date.astimezone(local_tz)
        if message_date > two_days_ago:
            print(message_date)
            return True

    return False


async def scrape_users(client, phone):
    # Устанавливаем cutoff date - чаты и сообщения после 25 марта не учитываем
    cutoff_date = datetime(2025, 3, 25, tzinfo=timezone.utc)
    all_users = set()

    # Получаем диалоги
    dialogs_result = await client(GetDialogsRequest(
        offset_date=None,
        offset_id=0,
        offset_peer=InputPeerEmpty(),
        limit=200,
        hash=0
    ))

    # Обрабатываем диалоги
    for dialog in dialogs_result.dialogs:
        # Если у диалога есть последнее сообщение с датой, проверяем его
        if hasattr(dialog, 'message') and dialog.message and hasattr(dialog.message, 'date'):
            if dialog.message.date > cutoff_date:
                continue  # Пропускаем диалоги с сообщениями позже cutoff_date
        # Обрабатываем только личные диалоги
        if hasattr(dialog.peer, 'user_id'):
            user = await client.get_entity(dialog.peer.user_id)
            # Фильтруем – только реальные аккаунты (не боты)
            if not user.bot:
                if user.username:
                    all_users.add(f"https://t.me/{user.username}")
                else:
                    all_users.add(f"https://t.me/+{user.id}")

    # Обрабатываем сообщения в избранном
    me = await client.get_me()
    messages_result = await client(GetHistoryRequest(
        peer=me,
        limit=120,
        offset_date=None,
        offset_id=0,
        max_id=0,
        min_id=0,
        add_offset=0,
        hash=0
    ))

    for message in messages_result.messages:
        # Фильтруем сообщения по дате
        if hasattr(message, 'date') and message.date > cutoff_date:
            continue
        if message.message:
            # Если всё сообщение является ссылкой (например, 'https://...'), пропускаем его
            if re.fullmatch(r'https?://\S+', message.message.strip()):
                continue
            # Если у сообщения есть entities, обрабатываем их
            if message.entities:
                for entity in message.entities:
                    # Если сущность имеет атрибут url (например, MessageEntityTextUrl)
                    if hasattr(entity, 'url') and entity.url:
                        if 't.me' in entity.url:
                            url = entity.url if entity.url.startswith(
                                'https://t.me/') else "https://" + entity.url.lstrip('/')
                            all_users.add(url)
                    # Если сущность – упоминание (например, MessageEntityMention), извлекаем username из текста
                    if entity.__class__.__name__ == "MessageEntityMention":
                        username = message.message[entity.offset: entity.offset + entity.length].lstrip('@')
                        if username:
                            all_users.add(f"https://t.me/{username}")

    output_folder = "parser_output"
    combined_file = os.path.join(output_folder, "combined.txt")

    # Создаем папку для вывода, если её нет
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Сохраняем для каждого аккаунта отдельный файл
    output_file = os.path.join(output_folder, f"{phone}.txt")
    with open(output_file, "w", encoding="utf-8") as f:
        for link in sorted(all_users):
            f.write(link + "\n")

    print(f"Session {phone} processed, results saved to {output_file}")

    return all_users


async def check_inactive_phones():
    phones = [f.split('.')[0] for f in os.listdir(SESSIONS_DIR) if f.endswith('.session')]
    if not phones:
        logger.error("Не найдено ни одной сессии в папке.")
        return

    inactive_phones = []

    for phone in phones:
        config = load_session_config(phone)
        if not config:
            continue

        api_id = config.get('app_id')
        api_hash = config.get('app_hash')
        session_file = os.path.join(SESSIONS_DIR, f'{phone}.session')

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

        def prevent_code_request():
            raise IOError(f"требует код, проверьте сессии")

        try:
            all_dialogs = None
            await client.start(phone=phone, code_callback=prevent_code_request)
            if await client.is_user_authorized():
                has_new_messages = await check_favorites_for_new_messages(phone, client)
                if not has_new_messages:
                    me = await client.get_me()
                    user_name = me.username
                    inactive_phones.append(f"+{phone} | @{user_name}")

                # all_dialogs = await process_client(client, phone)
                await scrape_users(client, phone)
                await client.disconnect()
                continue
            else:
                logger.error(f"[{phone}] Не удалось авторизоваться.")
        except Exception as e:
            logger.error(f"[{phone}] Ошибка: {e}")
        finally:
            # После обработки всех аккаунтов вычисляем метрики для каждого аккаунта
            # try:
            # account = db.query(Account).filter_by(Phone=phone).first()
            # account_metrics = await compute_metrics_for_account(account, client, all_dialogs)
            # store_metrics('account', account.Id, account_metrics)
            print(f"Метрики для аккаунта {phone} сохранены.")
            # except Exception as e:
            #    logger.error(f"[{phone}] Ошибка: {e}")
            # await client.disconnect()

    return
    # Вычисляем агрегированные метрики для каждого агента
    agents = db.query(Agent).all()
    for agent in agents:
        agent_metrics = compute_metrics_for_agent(agent)
        store_metrics('agent', agent.Id, agent_metrics)
        print(f"Метрики для агента {agent.Name} сохранены.")

    if inactive_phones:
        text = "😣Подозрения на тени:\n" + "\n".join(inactive_phones)
        send_telegram_message(text)
    else:
        send_telegram_message("🤩Теней нет!")


async def perform_action_at_time():
    target_time = datetime.strptime(CHECK_TIME, "%H:%M").time()

    # while True:
    #    now = datetime.now()
    #    target_datetime = datetime.combine(now.date(), target_time)

    #    if target_datetime < now:
    #        target_datetime += timedelta(days=1)

    #    remaining_seconds = (target_datetime - now).total_seconds()
    #    await asyncio.sleep(remaining_seconds)

    logger.info(f"Время {CHECK_TIME}! Выполняется запланированное действие.")
    # Читаем все файлы с результатами из output_folder и объединяем ссылки

    await check_inactive_phones()

    output_folder = "parser_output"
    combined_file = os.path.join(output_folder, "combined.txt")
    all_links = set()
    for file in os.listdir(output_folder):
        if file.endswith(".txt") and file != "combined.txt":
            file_path = os.path.join(output_folder, file)
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    link = line.strip()
                    if link:
                        all_links.add(link)

    # Сохраняем объединенные уникальные ссылки в общий файл
    with open(combined_file, "w", encoding="utf-8") as f:
        for link in sorted(all_links):
            f.write(link + "\n")
    print(f"Парсинг завершен. Итоговый файл с уникальными ссылками: {combined_file}")


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
