import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, func, select, update, insert, Table, MetaData
from sqlalchemy.orm import sessionmaker, Session
from app.database.models import MotivationalPhrases, UserInfo, GeneralInfo
from datetime import datetime, timedelta
from config import JSON_FILE

# Укажите путь к вашей базе данных
DATABASE_URL = 'sqlite:///database.db'

# Создание движка и сессии
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Определение таблицы
metadata = MetaData()
names_table = Table('names', metadata, autoload_with=engine)

def format_duration(duration):
    """
    Форматировать длительность в формате "часы, минуты, секунды".
    
    Args:
        duration (timedelta): Длительность работы.
    
    Returns:
        str: Строка с форматированным временем.
    """
    total_seconds = int(duration.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    return f"{hours} час(а) {minutes} минут(ы) {seconds} секунд(ы)"

def get_random_phrase():
    """
    Получить случайную мотивационную фразу из базы данных.
    
    Returns:
        str: Мотивационная фраза или сообщение о том, что фразы отсутствуют.
    """
    phrase_record = session.query(MotivationalPhrases).order_by(func.random()).first()
    if phrase_record is None:
        return "Нет мотивационных фраз в базе данных."
    return phrase_record.phrase

def check_start_work(user_id):
    """
    Проверить, начал ли пользователь работу.
    
    Args:
        user_id (int): ID пользователя.
    
    Returns:
        UserInfo: Информация о пользователе или None, если работа не начата.
    """
    return session.query(UserInfo).filter_by(user_id=user_id, end_time=None).first()

def add_general_info():
    try:
        general_info = GeneralInfo()
        session.add(general_info)
        session.commit()
        print(f"Added GeneralInfo with id: {general_info.id}")
        return general_info.id
    except Exception as e:
        session.rollback()
        print(f"Failed to add GeneralInfo: {e}")
        return None


def add_user_info(user_id, general_id, start_time, started=False):
    try:
        user_info = UserInfo(user_id=user_id, general_id=general_id, start_time=start_time, started=started)
        session.add(user_info)
        session.commit()
        print(f"Added UserInfo for user_id: {user_id} with general_id: {general_id}")
    except Exception as e:
        session.rollback()
        print(f"Failed to add UserInfo: {e}")


def update_leads(user_id, leads):
    try:
        # Найти последнюю запись пользователя с не завершенным сеансом
        user = session.query(UserInfo).filter_by(user_id=user_id, end_time=None).order_by(UserInfo.id.desc()).first()
        
        if user:
            # Добавить новые лиды к текущему количеству
            user.leads += leads
            session.commit()
        else:
            # Если запись не найдена, создать новую запись
            general_id = add_general_info()
            start_time = datetime.utcnow()
            add_user_info(user_id, general_id, start_time, started=True)
            
            # Создаем новую запись с обновленными лидами
            user_info = UserInfo(user_id=user_id, general_id=general_id, leads=leads, start_time=start_time, started=True)
            session.add(user_info)
            session.commit()
    except Exception as e:
        session.rollback()
        print(f"Ошибка при обновлении лидов: {e}")
    finally:
        session.close()


def end_work(user_id, end_time):
    """
    Завершить рабочий день для пользователя и отправить статистику.
    
    Args:
        user_id (int): ID пользователя.
        end_time (datetime): Время окончания работы.
    
    Returns:
        tuple: Сообщение о ежедневной статистике и сообщение о общей статистике.
    """
    try:
        user = session.query(UserInfo).filter_by(user_id=user_id, end_time=None).first()
        
        if user:
            # Обновляем время окончания работы
            user.end_time = end_time
            session.commit()

            # Рассчитываем продолжительность рабочего дня
            work_duration = end_time - user.start_time
            work_duration_str = format_duration(work_duration)
            
            # Рассчитываем общее время работы и общее количество лидов
            all_records = session.query(UserInfo).filter_by(user_id=user_id).all()
            
            total_duration = timedelta()
            total_leads = 0

            for record in all_records:
                if record.end_time:
                    duration = record.end_time - record.start_time
                    total_duration += duration
                    total_leads += record.leads

            total_work_duration_str = format_duration(total_duration)

            # Формируем сообщения со статистикой
            daily_message = f"Ты сегодня проработал {work_duration_str}, закрыл {user.leads} лида(ов). Так держать!"
            total_message = f"За всё время ты проработал {total_work_duration_str} и закрыл {total_leads} лида(ов)."

            return daily_message, total_message
        else:
            return "Пользователь не найден.", ""
    except Exception as e:
        session.rollback()
        print(f"Произошла ошибка при завершении работы: {e}")
        return f"Произошла ошибка при завершении работы: {e}", ""
    finally:
        session.close()

def add_admin_to_db(user_id, user_name):
    try:
        # Создаем сессию
        with Session() as session:
            # Проверяем, существует ли уже такой пользователь в базе
            result = session.execute(select(names_table.c.real_user_id).where(names_table.c.real_user_id == user_id)).fetchone()

            if result:
                # Обновляем имя, если пользователь уже есть в базе
                session.execute(
                    names_table.update().where(names_table.c.real_user_id == user_id).values(real_name=user_name)
                )
                print(f"Updated existing admin with user_id {user_id} to name: {user_name}")
            else:
                # Вставляем нового пользователя
                session.execute(
                    names_table.insert().values(real_user_id=user_id, real_name=user_name)
                )
                print(f"Added new admin with user_id {user_id} and name: {user_name}")
            
            # Сохраняем изменения
            session.commit()

            # Создаем страницу в Google Sheets при успешном добавлении
            update_sheet(user_name)
            print(f"Worksheet '{user_name}' created.")
    except Exception as e:
        print(f"Failed to add admin: {e}")

def authorize_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_FILE, scope)
    client = gspread.authorize(creds)
    return client


def update_sheet(real_name):
    client = authorize_google_sheets()
    spreadsheet = client.open("BALI LOVERS")

    try:
        worksheet = spreadsheet.worksheet(real_name)
        print(f"Worksheet '{real_name}' exists.")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=real_name, rows="100", cols="20")
        worksheet.update('A1', [[real_name]])
        print(f"Worksheet '{real_name}' created.")