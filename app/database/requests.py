import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, func, select, update, insert, Table, MetaData
from sqlalchemy.orm import sessionmaker, Session
from app.database.models import MotivationalPhrases, UserInfo, GeneralInfo
from datetime import datetime, timedelta, timezone
from config import JSON_FILE

# Укажите путь к вашей базе данных
DATABASE_URL = 'sqlite:///database.db'

# Определение часового пояса для GMT+8
bali_tz = timezone(timedelta(hours=11))

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
    print('========== format duration', hours+8, minutes, seconds)
    
    return f"{hours+8} час(а) {minutes} минут(ы) {seconds} секунд(ы)"

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
        # Конвертируем время начала в часовой пояс Bali
        start_time = start_time.astimezone(bali_tz)
        user_info = UserInfo(user_id=user_id, general_id=general_id, start_time=start_time, started=started)
        session.add(user_info)
        session.commit()
        print(f"Added UserInfo for user_id: {user_id} with general_id: {general_id}")
    except Exception as e:
        session.rollback()
        print(f"Failed to add UserInfo: {e}")


def update_leads(user_id, leads):
    try:
        user = session.query(UserInfo).filter_by(user_id=user_id, end_time=None).order_by(UserInfo.id.desc()).first()
        if user:
            user.leads += leads
            session.commit()
        else:
            general_id = add_general_info()
            # Устанавливаем время начала в текущем времени с часовой зоной Bali
            start_time = start_time.astimezone(bali_tz)
            print('=====================start', start_time)
            add_user_info(user_id, general_id, start_time, started=True)
            user_info = UserInfo(user_id=user_id, general_id=general_id, leads=leads, start_time=start_time, started=True)
            session.add(user_info)
            session.commit()
    except Exception as e:
        session.rollback()
        print(f"Ошибка при обновлении лидов: {e}")
    finally:
        session.close()


def end_work(user_id, end_time):
    try:
        user = session.query(UserInfo).filter_by(user_id=user_id, end_time=None).first()
        
        if user:
            # Конвертируем время окончания в часовой пояс Bali
            end_time = end_time.astimezone(bali_tz)
            print(f"Setting end_time for user {user_id} to {end_time}")
            
            user.end_time = end_time
            session.commit()
            print(f"End time successfully recorded for user {user_id}")

            # Конвертируем время начала в часовой пояс Bali
            start_time = user.start_time.astimezone(bali_tz)
            work_duration = end_time - start_time
            print('---start end', start_time, end_time)
            print('==========duration', work_duration)
            work_duration_str = format_duration(work_duration)
            
            # Получаем все записи для пользователя
            all_records = session.query(UserInfo).filter_by(user_id=user_id).all()
            
            total_duration = timedelta()
            total_leads = 0

            for record in all_records:
                if record.end_time:
                    # Конвертируем время начала и окончания в часовой пояс Bali
                    record_start_time = record.start_time.astimezone(bali_tz)
                    record_end_time = record.end_time.astimezone(bali_tz)
                    duration = record_end_time - record_start_time
                    total_duration += duration
                    total_leads += record.leads

            total_work_duration_str = format_duration(total_duration)

            daily_message = f"Ты сегодня проработал {work_duration_str}, закрыл {user.leads} лида(ов). Так держать!"
            total_message = f"За всё время ты проработал {total_work_duration_str} и закрыл {total_leads} лида(ов)."

            return daily_message, total_message
        else:
            print(f"No active work found for user {user_id}")
            return "Пользователь не найден.", ""
    except Exception as e:
        session.rollback()
        print(f"Произошла ошибка при завершении работы: {e}")
        return f"Произошла ошибка при завершении работы: {e}", ""




def add_admin_to_db(user_id, user_name):
    try:
        # Создаем сессию
        with Session() as session:
            # Проверяем, существует ли уже такой пользователь в базе
            result = session.execute(select(names_table.c.real_user_id).where(names_table.c.real_user_id == user_id)).fetchone()

            if result:
                # Обновляем имя, если пользователь уже есть в базе
                session.execute(
                    names_table.update().where(names_table.c.real_user_id == user_id).values(real_name=user_name),
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


def update_group_id(user_id, chat_id):
    """
    Обновляет group_id в таблице names для соответствующего real_user_id.
    
    Args:
        user_id (int): ID пользователя (real_user_id).
        chat_id (int): ID группового чата.
    """
    try:
        with Session() as session:
            # Проверяем, существует ли пользователь
            result = session.execute(
                select(names_table.c.real_user_id).where(names_table.c.real_user_id == user_id)
            ).fetchone()

            if result:
                # Обновляем group_id, если пользователь существует
                session.execute(
                    update(names_table)
                    .where(names_table.c.real_user_id == user_id)
                    .values(group_id=chat_id)
                )
                session.commit()
                print(f"Updated group_id for user_id {user_id} to chat_id {chat_id}.")
            else:
                print(f"User with user_id {user_id} not found.")
    except Exception as e:
        print(f"Failed to update group_id: {e}")


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



