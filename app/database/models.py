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


# Определение состояний для FSM
class AddManagerState(StatesGroup):
    waiting_for_user = State()
    waiting_for_name = State()
    waiting_for_rop = State()
    waiting_for_language = State()

class AddHeadState(StatesGroup):
    waiting_for_user = State()
    waiting_for_name = State()

class DelManagerState(StatesGroup):
    waiting_for_user = State()

class DelHeadState(StatesGroup):
    waiting_for_user = State()

class LeadData(BaseModel):
    chat_id: str
    lead_count: int
