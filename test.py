from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()

class MotivationalPhrases(Base):
    __tablename__ = 'motivational_phrases'
    id = Column(Integer, primary_key=True)
    phrase = Column(String, nullable=False)

# Подключение к базе данных SQLite
engine = create_engine('sqlite:///database.db')
Base.metadata.create_all(engine)

# Создание сессии
Session = sessionmaker(bind=engine)
session = Session()

# Пример добавления мотивационной фразы
new_phrase = MotivationalPhrases(phrase="Не сдавайся, даже если кажется, что всё против тебя!")
session.add(new_phrase)
session.commit()
