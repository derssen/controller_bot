import gspread
import logging
import requests
import export_google 
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, func, select, update, insert, Table, MetaData, and_
from sqlalchemy.orm import sessionmaker, Session
from app.database.models import MotivationalPhrases, MotivationalEngPhrases, UserInfo, GeneralInfo
from datetime import datetime, timedelta, timezone, date
from config import JSON_FILE, DATABASE_URL, GOOGLE_SHEET, API_TOKEN

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Создание движка и сессии
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Определение таблицы
metadata = MetaData()
names_table = Table('names', metadata, autoload_with=engine)
heads_table = Table('heads', metadata, autoload_with=engine)

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


def get_eng_random_phrase():
    """
    Получить случайную мотивационную фразу из базы данных.
    
    Returns:
        str: Мотивационная фраза или сообщение о том, что фразы отсутствуют.
    """
    phrase_record = session.query(MotivationalEngPhrases).order_by(func.random()).first()
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
        user_info = UserInfo(user_id=user_id, general_id=general_id, date=start_time, start_time=start_time, started=started)
        session.add(user_info)
        session.commit()
        print(f"Added UserInfo for user_id: {user_id} with general_id: {general_id}")
    except Exception as e:
        session.rollback()
        print(f"Failed to add UserInfo: {e}")


'''def update_leads(chat_id, leads):
    print(f'Сработала функция update leads, chat_id={chat_id}, leads={leads}')
    try:
        # Fetch the real_user_id from the names table using chat_id (group_id)
        name_entry = session.query(names_table).filter_by(group_id=chat_id).first()
        
        if not name_entry:
            print("No user found with the given chat_id.")
            return

        real_user_id = name_entry.real_user_id
        
        # Fetch the most recent UserInfo entry for the real_user_id where work has not ended
        user = session.query(UserInfo).filter_by(user_id=real_user_id, end_time=None).order_by(UserInfo.id.desc()).first()
        
        if user:
            # If an active user session exists, update the leads
            user.leads += leads
            session.commit()
        else:
            # If no active session exists, create a new entry
            general_id = add_general_info()
            start_time = datetime.now()
            # Adding a new user info entry with the leads count
            add_user_info(real_user_id, general_id, start_time, started=True)
            user_info = UserInfo(user_id=real_user_id, general_id=general_id, leads=leads, start_time=start_time, started=True)
            session.add(user_info)
            session.commit()

    except Exception as e:
        session.rollback()
        print(f"Ошибка при обновлении лидов: {e}")
    finally:
        session.close()'''

def send_time_to_telegram(start_time):
    print(f'Сработала функция send_time_to_telegram, start_time={start_time}')
    try:
        # Получаем текущее время сервера
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
        # Формируем текст сообщения
        message = f"Текущее время сервера: {current_time}, \n Время старта: {start_time}"
        print(message)
        
        # URL для отправки сообщения через Telegram API
        url = f"https://api.telegram.org/bot7408947547:AAG4_hbMdwp7cWAMnTaP3ZIZyn1cRRxC2Ig/sendMessage"
        chat_id = '-4540710078'
        # Параметры запроса
        payload = {
            "chat_id": chat_id,
            "text": message
        }
        
        # Отправка сообщения
        response = requests.post(url, data=payload)
        
        # Проверка успешности запроса
        if response.status_code == 200:
            print("Сообщение успешно отправлено!")
        else:
            print(f"Ошибка отправки: {response.status_code}")
    except Exception as e:
        print(f"send_time_to_telegram - An error occurred: {e}")        


def update_leads_from_crm(chat_id, leads):
    print(f'Сработала функция update_leads_from_crm, chat_id={chat_id}, leads={leads}')
    
    try:
        # Получаем real_user_id из таблицы names по chat_id (group_id)
        name_entry = session.query(names_table).filter_by(group_id=chat_id).first()
        
        if not name_entry:
            print(f"Не удалось найти пользователя с указанным chat_id: {chat_id}")
            return

        # Извлекаем real_user_id
        real_user_id = name_entry.real_user_id
        print(f"Найден real_user_id: {real_user_id} для chat_id: {chat_id}")
        
        # Текущая дата
        today = datetime.now().date()
        print(f"Ищем запись для user_id={real_user_id} на дату {today}")
        
        # Проверяем наличие записи с текущей датой (сравнение только по дате)
        user_info = session.query(UserInfo).filter(
            and_(
                UserInfo.user_id == real_user_id,
                func.date(UserInfo.date) == today  # Сравниваем только дату
            )
        ).first()
        print(f'Найдена запись UserInfo: {user_info}')
        
        if user_info:
            # Обновляем количество лидов, если запись найдена
            user_info.leads += leads
            session.commit()
            print(f"Обновлена запись для пользователя {real_user_id}: добавлено {leads} лидов.")
        else:
            # Создаём новую запись, если записи с текущей датой нет
            general_id = add_general_info()
            new_user_info = UserInfo(
                user_id=real_user_id,
                general_id=general_id,
                leads=leads,
                date=datetime.now(),  # Сохраняем дату и время текущего момента
                start_time=None,
                end_time=None,
                started=True
            )
            session.add(new_user_info)
            session.commit()
            print(f"Создана новая запись для пользователя {real_user_id} с {leads} лидами. Время: {datetime.now()}")
        
        # Обновление Google Sheets
        print('Лиды добавлены в БД через вебхук, запускается отрисовка таблицы.')
        export_google.update_user_data()
    
    except Exception as e:
        session.rollback()
        print(f"Ошибка при обновлении лидов: {e}")
    
    finally:
        print("Закрытие сессии...")
        session.close()




def end_work(user_id, end_time):
    print(f'Сработала функция end_work, user_id={user_id}, end_time={end_time}')
    try:
        # Текущая дата
        today = end_time.date()
        print(f"Ищем первую запись с пустым end_time за {today}")

        # Ищем первую запись с пустым end_time за текущий день
        user = session.query(UserInfo).filter(
            and_(
                UserInfo.user_id == user_id,
                UserInfo.end_time.is_(None),
                func.date(UserInfo.start_time) == today  # Учитываем только дату
            )
        ).first()
        
        if user:
            print(f"Setting end_time for user {user_id} to {end_time}")
            
            # Устанавливаем end_time и сохраняем
            user.end_time = end_time
            session.commit()
            print(f"End time successfully recorded for user {user_id}")

            start_time = user.start_time
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
                    record_start_time = record.start_time
                    record_end_time = record.end_time
                    duration = record_end_time - record_start_time
                    total_duration += duration
                    total_leads += record.leads

            total_work_duration_str = format_duration(total_duration)

            daily_message = f"Ты сегодня проработал {work_duration_str}, закрыл {user.leads} лида(ов). Так держать!"
            total_message = f"За всё время ты проработал {total_work_duration_str} и закрыл {total_leads} лида(ов)."

            return daily_message, total_message
        else:
            print(f"No active work found for user {user_id} on {today}")
            return "Пользователь не найден или работа за сегодня уже завершена.", ""
    except Exception as e:
        session.rollback()
        print(f"Произошла ошибка при завершении работы: {e}")
        return f"Произошла ошибка при завершении работы: {e}", ""



def add_admin_to_db(user_id, user_name, amocrm_id, language):
    try:
        with Session() as session:
            # Проверяем, существует ли уже такой пользователь в базе
            result = session.execute(select(names_table.c.real_user_id).where(names_table.c.real_user_id == user_id)).fetchone()

            if result:
                # Обновляем запись, если пользователь уже есть в базе
                session.execute(
                    names_table.update().where(names_table.c.real_user_id == user_id).values(
                        real_name=user_name,
                        amocrm_id=amocrm_id,
                        language=language
                    ),
                )
                print(f"Updated existing admin with user_id {user_id}: name: {user_name}, amocrm_id: {amocrm_id}, language: {language}")
            else:
                # Вставляем нового пользователя
                session.execute(
                    names_table.insert().values(
                        real_user_id=user_id,
                        real_name=user_name,
                        amocrm_id=amocrm_id,
                        language=language
                    )
                )
                print(f"Added new admin with user_id {user_id}, name: {user_name}, amocrm_id: {amocrm_id}, language: {language}")

            # Сохраняем изменения
            session.commit()

            # Создаем страницу в Google Sheets при успешном добавлении
            update_sheet(user_name)
            print(f"Worksheet '{user_name}' created.")
    except Exception as e:
        print(f"Failed to add manager: {e}")



def del_manager_from_db(user_id):
    try:
        # Создаем сессию с привязкой к движку
        with Session(bind=engine) as session:
            # Проверяем, существует ли пользователь в базе
            result = session.query(names_table.c.real_user_id, names_table.c.real_name).filter(
                names_table.c.real_user_id == user_id
            ).first()

            if result:
                # Извлекаем имя пользователя перед удалением
                real_user_id, real_name = result
                # Удаляем пользователя
                session.query(names_table).filter(names_table.c.real_user_id == user_id).delete()
                output_text = f"Менеджер  {real_name} и id {real_user_id} удален."
                print(output_text)
            else:
                output_text = f"Менеджер с user_id {user_id} не найден в базе."
                print(output_text)
            # Сохраняем изменения
            session.commit()
            return output_text

    except Exception as e:
        print(f"Не удалось удалить менеджера: {e}")


def del_head_from_db(user_id):
    try:
        # Создаем сессию с привязкой к движку
        with Session(bind=engine) as session:
            # Проверяем, существует ли пользователь в базе
            result = session.query(heads_table.c.head_id, heads_table.c.head_name).filter(
                heads_table.c.head_id == user_id
            ).first()

            if result:
                # Извлекаем имя пользователя перед удалением
                head_id, head_name = result
                # Удаляем пользователя
                session.query(heads_table).filter(heads_table.c.head_id == user_id).delete()
                output_text = f"Руководитель  {head_name} и id {head_id} удален."
                print(output_text)
            else:
                output_text = f"Руководитель с user_id {user_id} не найден в базе."
                print(output_text)
            # Сохраняем изменения
            session.commit()
            return output_text

    except Exception as e:
        print(f"Не удалось удалить руководителя: {e}")


def add_head_to_db(user_id, user_name):
    try:
        # Создаем сессию
        with Session() as session:
            # Проверяем, существует ли уже такой пользователь в базе
            result = session.execute(select(heads_table.c.head_id).where(heads_table.c.head_id == user_id)).fetchone()

            if result:
                # Обновляем имя, если пользователь уже есть в базе
                session.execute(
                    heads_table.update().where(heads_table.c.head_id == user_id).values(head_name=user_name),
                )
                print(f"Updated existing head with user_id {user_id} to name: {user_name}")
            else:
                # Вставляем нового пользователя
                session.execute(
                    heads_table.insert().values(head_id=user_id, head_name=user_name)
                )
                print(f"Added new head with user_id {user_id} and name: {user_name}")
            
            # Сохраняем изменения
            session.commit()

    except Exception as e:
        print(f"Failed to add head: {e}")


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
    spreadsheet = client.open(GOOGLE_SHEET)

    try:
        worksheet = spreadsheet.worksheet(real_name)
        print(f"Worksheet '{real_name}' exists.")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=real_name, rows="100", cols="20")
        worksheet.update('A1', [[real_name]])
        print(f"Worksheet '{real_name}' created.")

def get_heads_ids():
    # Connect to the database
    with Session() as session:
        # Fetch the head IDs
        result = session.execute(select(heads_table.c.head_id)).fetchall()
        # Convert results to a set of IDs
        heads_ids = {row[0] for row in result}
        print('heads:', heads_ids)
    return heads_ids


def show_state_list():
    """
    Извлекает и форматирует список руководителей и менеджеров из базы данных.
    
    Returns:
        tuple: (строка с руководителями, строка с менеджерами).
    """
    try:
        with Session() as session:
            # Извлекаем имена руководителей
            head_names = session.execute(select(heads_table.c.head_name)).scalars().all()
            # Извлекаем имена менеджеров
            manager_names = session.execute(select(names_table.c.real_name)).scalars().all()

            # Форматируем списки
            formatted_heads = '\n'.join(head_names) if head_names else "Нет руководителей."
            formatted_managers = '\n'.join(manager_names) if manager_names else "Нет менеджеров."

            # Создаём строки для вывода
            output_heads = f"Руководители:\n{formatted_heads}"
            output_managers = f"Менеджеры:\n{formatted_managers}"

            return output_heads, output_managers
    except Exception as e:
        print(f"Ошибка при выполнении show_state_list: {e}")
        return "Ошибка при извлечении руководителей.", "Ошибка при извлечении менеджеров."



def send_daily_leads_to_group():
    """
    Calculate today's leads for each user and send the total to their respective group chat.
    """
    try:
        # Fetch all users from the names table
        users = session.query(names_table).all()
        
        today = datetime.now().date()

        for user in users:
            # Check if the user has a valid group_id
            if not user.group_id:
                print(f"No group ID found for user {user.real_name} (ID: {user.real_user_id}). Skipping.")
                continue

            # Calculate today's leads for the user
            leads_today = session.query(func.sum(UserInfo.leads)).filter(
                and_(
                    UserInfo.user_id == user.real_user_id,
                    func.date(UserInfo.date) == today
                )
            ).scalar()

            leads_today = leads_today or 0  # Set to 0 if no leads found

            # Prepare the message
            message = f"Сегодня {today.strftime('%Y-%m-%d')} у пользователя {user.real_name} закрыто {leads_today} лида(ов)."

            # Send the message to the group chat via Telegram API
            url = f"https://api.telegram.org/bot{API_TOKEN}/sendMessage"
            payload = {
            #    "chat_id": user.group_id,
                "chat_id": '-4540710078',
                "text": message
            }
            
            response = requests.post(url, data=payload)
            
            # Check if the message was sent successfully
            if response.status_code == 200:
                print(f"Message sent successfully to group {user.group_id} for user {user.real_name}!")
            else:
                print(f"Failed to send message to group {user.group_id} for user {user.real_name}: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"An error occurred: {e}")


def get_language_by_chat_id(chat_id: int):
    """
    Получить значение language для заданного chat_id.
    """
    try:
        with Session() as session:
            # Ищем запись в таблице names по group_id (chat_id)
            query = select(names_table.c.language).where(
                names_table.c.group_id == chat_id
            )
            result = session.execute(query)
            data = result.fetchone()
            if data:
                language = data[0]
                return language  # Возвращаем значение language
            else:
                logging.info(f"No language found for chat_id {chat_id}")
                return None
    except Exception as e:
        logging.info(f"Error fetching language for chat_id {chat_id}: {e}")
        return None
