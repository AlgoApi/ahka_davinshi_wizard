from sqlalchemy.orm import Session
from models import *
from typing import Any
import pytz
from sqlalchemy import union_all, literal_column, select
import time
from datetime import datetime, timezone, timedelta, time
from telethon import TelegramClient
from logger_service import *

# ---------------------------
# Функции вычисления и сохранения метрик
# ---------------------------
async def compute_metrics_for_account(account: Account, client: TelegramClient, all_dialogs, phone_num, pool, db: Session):
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
        total_response_time_day = sum([msg.ResponseTime.total_seconds() for msg in agent_msgs_last_day if msg.ResponseTime is not None])
        avg_response_time_last_day = timedelta(seconds=(total_response_time_day / len(agent_msgs_last_day)))
    else:
        avg_response_time_last_day = None

    agent_msgs_last_week = db.query(Message).filter(
        Message.ChatId.in_(chat_ids),
        Message.Sender == 'agent',
        Message.Timestamp >= seven_days_ago).all()
    if agent_msgs_last_week:
        total_response_time_week = sum([msg.ResponseTime.total_seconds() for msg in agent_msgs_last_week if msg.ResponseTime is not None])
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

    def fetch_last_week_combined(db_session: Session, account_id: int) -> list[dict[str, Any]]:
        """
        Возвращает единый список строк из mentions_a и mentions_b, у которых:
          - AccountId == переданный account_id
          - CreatedAt >= (текущая метка UTC − 7 дней)

        Результат — список dict'ов вида:
            {
                "Id": ...,          # id записи (из mentions_a или mentions_b)
                "AccountId": ...,   # всегда будет = account_id
                "Username": ...,    # username из упоминания
                "CreatedAt": ...,   # aware datetime (UTC)
                "source": "A" или "B"
            }
        """
        # 1) Берём текущий момент в UTC
        now = datetime.now(timezone.utc)
        seven_days_ago = datetime.combine((now - timedelta(days=7)).date(), time.min, tzinfo=timezone.utc)

        # 2) SELECT для mentions_a с двух фильтрами: по account_id и по дате
        print(account_id)
        print(seven_days_ago)
        stmt_a = (
            select(
                MentionA.Id.label("Id"),
                MentionA.AccountId.label("AccountId"),
                MentionA.Username.label("Username"),
                MentionA.CreatedAt.label("CreatedAt"),
                literal_column("'A'").label("source")
            )
            .where(
                MentionA.AccountId == account_id,
                MentionA.CreatedAt >= seven_days_ago
            )
        )

        # 3) SELECT для mentions_b с теми же фильтрами
        stmt_b = (
            select(
                MentionB.Id.label("Id"),
                MentionB.AccountId.label("AccountId"),
                MentionB.Username.label("Username"),
                MentionB.CreatedAt.label("CreatedAt"),
                literal_column("'B'").label("source")
            )
            .where(
                MentionB.AccountId == account_id,
                MentionB.CreatedAt >= seven_days_ago
            )
        )

        # 4) Объединяем оба запроса через UNION ALL
        union_stmt = union_all(stmt_a, stmt_b)

        # 5) Выполняем объединённый запрос и сразу возвращаем список dict
        rows = db_session.execute(union_stmt).mappings().all()
        return [dict(r) for r in rows]

    mentions = fetch_last_week_combined(db, account.Id)

    for mention in mentions:
        if not mention.get("Username", None) or not mention.get("CreatedAt", None) or mention.get("Username", None) == 'ashqua_bot':
            continue

        extracted_name = mention.get("Username")
        extracted_data = mention.get("CreatedAt").astimezone(timezone.utc)
        # Ищем в базе диалог (chat) с этим человеком по имени.
        # Здесь можно настроить фильтрацию: например, ищем записи в таблице Chat, где поле name содержит extracted_name.

        target_dialog = None

        if isinstance(extracted_name, int):
            chat = db.query(Chat).filter(
                Chat.UserId == extracted_name,
                Chat.AccountId == account.Id).first()
            for dialog in all_dialogs:
                # Проверяем, что это личный чат (не группа, не канал)
                if dialog.is_user:
                    # Если у сущности есть username и он совпадает с целевым
                    if getattr(dialog.entity, 'id', None) == extracted_name:
                        target_dialog = dialog
                        break
        else:
            chat = db.query(Chat).filter(Chat.Username.ilike(f"%{extracted_name}%"),
                                         Chat.AccountId == account.Id).first()
            for dialog in all_dialogs:
                # Проверяем, что это личный чат (не группа, не канал)
                if dialog.is_user:
                    # Если у сущности есть username и он совпадает с целевым
                    if getattr(dialog.entity, 'username', None) == extracted_name:
                        target_dialog = dialog
                        break

        if chat:
            utc = pytz.UTC
            if chat.CreatedAt >= extracted_data:
                # Учитываем сообщение по дате.
                if extracted_data >= one_day_ago:
                    mutual_msgs_last_day.append(mention)
                elif extracted_data >= seven_days_ago:
                    mutual_msgs_last_week.append(mention)
                outdated = False
            else:
                outdated = True
                outdated_dialogs_last_week.add(chat.Username)
            if target_dialog:
                if target_dialog.date < seven_days_ago:
                    outdated_dialogs_last_week.add(target_dialog.entity.username)
                else:
                    # Считаем, сколько сообщений от агента в найденном чате.
                    agent_msg_count = db.query(Message).filter(Message.ChatId == chat.Id,
                                                               Message.Sender == 'agent').count()
                    if agent_msg_count >= 2:
                        # Если сообщение попадает в период, засчитываем этот диалог.
                        if extracted_data >= one_day_ago:
                            valid_dialogs_last_day.add(chat.Username)
                        elif extracted_data >= seven_days_ago:
                            valid_dialogs_last_week.add(chat.Username)
            else:
                if not outdated:
                    # Считаем, сколько сообщений от агента в найденном чате.
                    agent_msg_count = db.query(Message).filter(Message.ChatId == chat.Id,
                                                               Message.Sender == 'agent').count()
                    if agent_msg_count >= 2:
                        # Если сообщение попадает в период, засчитываем этот диалог.
                        if extracted_data >= one_day_ago:
                            valid_dialogs_last_day.add(chat.Username)
                        elif extracted_data >= seven_days_ago:
                            valid_dialogs_last_week.add(chat.Username)
        else:
            if target_dialog:
                first_message = await client.get_messages(target_dialog, limit=1, reverse=True)
                if first_message:
                    first_message = first_message[0]
                    if first_message.date >= extracted_data:
                        # Учитываем сообщение по дате.
                        if extracted_data >= one_day_ago:
                            mutual_msgs_last_day.append(mention)
                        elif extracted_data >= seven_days_ago:
                            mutual_msgs_last_week.append(mention)
                    if target_dialog.date < seven_days_ago:
                        outdated_dialogs_last_week.add(target_dialog.entity.username)
            else:
                # Учитываем сообщение по дате.
                if extracted_data >= one_day_ago:
                    mutual_msgs_last_day.append(mention)
                elif extracted_data >= seven_days_ago:
                    mutual_msgs_last_week.append(mention)

    # Вычисляем показатели:
    new_vzaimki_msgs_last_day = len(mutual_msgs_last_day)
    avg_vzaimki_msgs_last_week = len(mutual_msgs_last_week) / 7.0
    new_valid_dialogs_last_day = len(valid_dialogs_last_day)
    avg_valid_dialogs_last_week = len(valid_dialogs_last_week) / 7.0
    new_valid_dialogs_last_week = len(valid_dialogs_last_week)
    new_vzaimki_msgs_last_week = len(mutual_msgs_last_week)

    if new_vzaimki_msgs_last_day and new_valid_dialogs_last_day:
        not_valid = new_vzaimki_msgs_last_day - new_valid_dialogs_last_day
        prochent = 100 / (new_vzaimki_msgs_last_day / new_valid_dialogs_last_day)
        logger.warning(f"---\n[{account.Phone}] пришедшие взаимки: {new_vzaimki_msgs_last_day} обработано: {new_valid_dialogs_last_day} необработано: {not_valid} res: {prochent}\noutdated: {outdated_dialogs_last_week}\n'''")

    return {
        'AvgNewMsgsLastWeek': avg_new_msgs_last_week,
        'AvgNewChatsLastWeek': avg_new_chats_last_week,
        'NewMsgsLastDay': new_msgs_last_day,
        'NewChatsLastDay': new_chats_last_day,
        'AvgResponseTimeLastDay': avg_response_time_last_day,
        'AvgResponseTimeLastWeek': avg_response_time_last_week,
        'new_vzaimki_msgs_last_day' : new_vzaimki_msgs_last_day,
        'avg_vzaimki_msgs_last_week': avg_vzaimki_msgs_last_week,
        'new_valid_dialogs_last_day': new_valid_dialogs_last_day,
        'avg_valid_dialogs_last_week': avg_valid_dialogs_last_week,
        'new_valid_dialogs_last_week': new_valid_dialogs_last_week + new_valid_dialogs_last_day,
        'new_vzaimki_msgs_last_week': new_vzaimki_msgs_last_week + new_vzaimki_msgs_last_day,
        'outdated_dialogs_last_week': len(outdated_dialogs_last_week)
    }


def compute_metrics_for_agent(agent: Agent, db: Session):
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
        total_response_time_day = sum([msg.ResponseTime.total_seconds() for msg in agent_msgs_last_day if msg.ResponseTime is not None])
        avg_response_time_last_day = timedelta(seconds=(total_response_time_day / len(agent_msgs_last_day)))
    else:
        avg_response_time_last_day = None

    agent_msgs_last_week = db.query(Message).filter(
        Message.ChatId.in_(chat_ids),
        Message.Sender == 'agent',
        Message.Timestamp >= seven_days_ago).all()
    if agent_msgs_last_week:
        total_response_time_week = sum([msg.ResponseTime.total_seconds() for msg in agent_msgs_last_week if msg.ResponseTime is not None])
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

    if total_new_vzaimki_msgs_day and total_new_valid_dialogs_day:
        not_valid = total_new_vzaimki_msgs_day - total_new_valid_dialogs_day
        prochent = 100 / (total_new_vzaimki_msgs_day / total_new_valid_dialogs_day)
        logger.warning(f"+++\n[{agent.Name}] пришедшие взаимки: {total_new_vzaimki_msgs_day} обработано: {total_new_valid_dialogs_day} необработано: {not_valid} res: {prochent}\noutdated: {total_outdated_dialogs_last_week}\n+++")

    return {
        'AvgNewMsgsLastWeek': avg_new_msgs_last_week,
        'AvgNewChatsLastWeek': avg_new_chats_last_week,
        'NewMsgsLastDay': new_msgs_last_day,
        'NewChatsLastDay': new_chats_last_day,
        'AvgResponseTimeLastDay': avg_response_time_last_day,
        'AvgTesponseTimeLastWeek': avg_response_time_last_week,
        'new_vzaimki_msgs_last_day' : total_new_vzaimki_msgs_day,
        'avg_vzaimki_msgs_last_week': avg_vzaimki_msgs_week,
        'new_valid_dialogs_last_day': total_new_valid_dialogs_day,
        'avg_valid_dialogs_last_week': avg_valid_dialogs_week,
        'new_valid_dialogs_last_week': total_new_valid_dialogs_week,
        'new_vzaimki_msgs_last_week': total_new_vzaimki_msgs_week,
        'outdated_dialogs_last_week': total_outdated_dialogs_last_week
    }


def store_metrics(entity_type: str, entity_id: int, metrics_data: dict, db: Session):
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