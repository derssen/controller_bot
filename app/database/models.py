# models.py
from sqlalchemy import Column, Integer, String, DateTime, Boolean, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from aiogram.fsm.state import State, StatesGroup
from pydantic import BaseModel

engine = create_engine('sqlite:///database.db')
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

class UserInfo(Base):
    __tablename__ = 'user_info'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    date = Column(DateTime, default=datetime.utcnow)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    leads = Column(Integer, default=0)
    has_photo = Column(Integer, default=0)  # Новое поле
    started = Column(Boolean, default=False)

class MotivationalPhrases(Base):
    __tablename__ = 'motivational_phrases'
    id = Column(Integer, primary_key=True, autoincrement=True)
    phrase = Column(String, nullable=False)

class MotivationalEngPhrases(Base):
    __tablename__ = 'motivational_eng_phrases'
    id = Column(Integer, primary_key=True, autoincrement=True)
    phrase = Column(String, nullable=False)

Base.metadata.create_all(engine)


# Состояния для добавления/удаления пользователей
class AddUserState:
    waiting_for_user = "waiting_for_user"           # Ждем user_id
    waiting_for_rank = "waiting_for_rank"           # Выбор ранга (1=менеджер, 2=валидатор, 3=РОП)
    waiting_for_name = "waiting_for_name"           # Ждем реального имени (AMO CRM)
    waiting_for_language = "waiting_for_language"   # Ждем 'ru' или 'en'
    waiting_for_rop_username = "waiting_for_rop_username"  # Ждем username РОПа (если rank=1 или 2)

class DelUserState:
    waiting_for_user_to_delete = "waiting_for_user_to_delete"

class LeadData(BaseModel):
    chat_id: str
    lead_count: int
