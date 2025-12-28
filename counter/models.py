from sqlalchemy.sql import func
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Interval, ForeignKey, Text, Float, BigInteger
from sqlalchemy.orm import declarative_base, relationship

# ---------------------------
# Определение схемы базы данных
# ---------------------------
Base = declarative_base()
default_now = func.now()


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
    LastMessageDate = Column(
        DateTime(timezone=True),
        nullable=True
    )  # Время самого нового сообщения в чате
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
    MetricDate = Column(DateTime, default=lambda : datetime.now(timezone.utc))
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
