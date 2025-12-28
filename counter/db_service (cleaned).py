from sqlalchemy import event
from datetime import timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import *

# ---------------------------
# Настройка подключения к базе данных
# ---------------------------
DATABASE_URL = 'postgresql://login:pass@ip:port/db_name'
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=True)
db = SessionLocal()

# УБРАТЬ ПОСЛЕ ПЕРВОГО ЗАПУСКА
#Base.metadata.create_all(bind=engine)

@event.listens_for(Message, 'load')
@event.listens_for(Message, "refresh")
def load_convert_to_utc(Message, _context, _=None):
    if Message.Timestamp is not None:
        Message.Timestamp = (Message.Timestamp.astimezone(timezone.utc))

@ event.listens_for(Account, 'load')
@ event.listens_for(Account, "refresh")
def load_convert_to_utc4(Account, _context, _=None):
    if Account.LastActive is not None:
        Account.LastActive = (Account.LastActive.astimezone(timezone.utc))

@event.listens_for(Chat, 'load')
@event.listens_for(Chat, "refresh")
def load_convert_to_utc1(Chat, _context, _=None):
    if Chat.CreatedAt is not None:
        Chat.CreatedAt = (Chat.CreatedAt.astimezone(timezone.utc))

@event.listens_for(Chat, 'load')
@event.listens_for(Chat, "refresh")
def load_convert_to_utc5(Chat, _context, _=None):
    if Chat.LastMessageDate is not None:
        Chat.LastMessageDate = Chat.LastMessageDate.astimezone(timezone.utc)

@event.listens_for(MentionA, 'load')
@event.listens_for(MentionA, "refresh")
def load_convert_to_utc2(MentionA, _context, _=None):
    if MentionA.CreatedAt is not None:
        MentionA.CreatedAt = MentionA.CreatedAt.astimezone(timezone.utc)

@event.listens_for(MentionB, 'load')
@event.listens_for(MentionB, "refresh")
def load_convert_to_utc3(MentionB, _context, _=None):
    if MentionB.CreatedAt is not None:
        MentionB.CreatedAt = MentionB.CreatedAt.astimezone(timezone.utc)
