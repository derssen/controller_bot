from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, create_engine, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from aiogram.fsm.state import State, StatesGroup
from datetime import datetime


engine = create_engine('sqlite:///database.db')
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

class GeneralInfo(Base):
    __tablename__ = 'general_info'
    
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, default=datetime.utcnow)
    total_leads = Column(Integer, default=0)

class UserInfo(Base):
    __tablename__ = 'user_info'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    date = Column(DateTime, default=datetime.utcnow)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    leads = Column(Integer, default=0)
    general_id = Column(Integer, ForeignKey('general_info.id'))
    general = relationship('GeneralInfo', back_populates='user_info')
    started = Column(Boolean, default=False)  # новое поле для отслеживания состояния

GeneralInfo.user_info = relationship('UserInfo', order_by=UserInfo.id, back_populates='general')

class MotivationalPhrases(Base):
    __tablename__ = 'motivational_phrases'

    id = Column(Integer, primary_key=True, autoincrement=True)
    phrase = Column(String, nullable=False)

Base.metadata.create_all(engine)

# Определение состояний для FSM
class AddManagerState(StatesGroup):
    waiting_for_user = State()
    waiting_for_name = State()

class AddHeadState(StatesGroup):
    waiting_for_user = State()
    waiting_for_name = State()

class DelManagerState(StatesGroup):
    waiting_for_user = State()

class DelHeadState(StatesGroup):
    waiting_for_user = State()
