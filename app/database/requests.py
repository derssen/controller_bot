import gspread
import asyncio
import logging
import requests
import httpx
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, func, select, update, Table, MetaData, and_
from sqlalchemy.orm import sessionmaker, Session
from app.database.models import MotivationalPhrases, MotivationalEngPhrases, UserInfo
from datetime import datetime, timedelta
from config import JSON_FILE, DATABASE_URL, GOOGLE_SHEET, API_TOKEN

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Создание движка и сессии
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Определение таблицы
metadata = MetaData()
metadata.reflect(bind=engine)
names_table = metadata.tables['names']    # Объединённая таблица users
# heads_table = metadata.tables['heads']  # УДАЛЕНО: Логика heads теперь не нужна
messages_table = metadata.tables['messages']

def get_phrase_from_db(key: str, language: str = 'ru') -> str:
    """
    Получаем сообщение из таблицы messages по ключу и языку.
    """
    with Session() as session:
        row = session.execute(
            select(messages_table.c.ru_text, messages_table.c.en_text)
            .where(messages_table.c.key == key)
        ).fetchone()
        if not row:
            logging.warning(f"Не найден ключ {key} в таблице messages.")
            return key  # fallback – вернём сам key

        ru_text, en_text = row[0], row[1]
        if language == 'en':
            return en_text
        else:
            return ru_text


def get_message_for_user(key: str, user_language: str) -> str:
    """Обёртка для удобства: возвращаем фразу из БД (messages) на нужном языке."""
    return get_phrase_from_db(key, user_language)

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
    logging.info(f"format_duration: {hours+8}h {minutes}m {seconds}s (UTC+8)")
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


def add_user_info(user_id, start_time, started=False):
    try:
        with Session() as session:
            today = start_time.date()
            # Ищем запись за сегодня
            existing_info = session.query(UserInfo).filter(
                UserInfo.user_id == user_id,
                func.date(UserInfo.date) == today
            ).first()
            
            if existing_info:
                # Запись за сегодня уже есть
                if existing_info.start_time is None:
                    # Дополняем start_time, если его не было
                    existing_info.start_time = start_time
                    existing_info.started = started
                    session.commit()
                    logging.info(
                        f"Updated start_time for existing UserInfo on {today} for user_id: {user_id}"
                    )
                else:
                    # start_time уже стоит или рабочий день был начат ранее
                    logging.info(
                        f"UserInfo for user_id={user_id} on {today} already has start_time={existing_info.start_time}"
                    )
            else:
                # Нет записи — создаём новую
                user_info = UserInfo(
                    user_id=user_id,
                    date=start_time,
                    start_time=start_time,
                    started=started
                )
                session.add(user_info)
                session.commit()
                logging.info(f"Added new UserInfo record for user_id: {user_id}, date={today}")
    except Exception as e:
        logging.error(f"Failed to add or update UserInfo: {e}")



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


async def update_leads_from_crm_async(chat_id, leads):
    """
    Асинхронная оболочка для update_leads_from_crm, чтобы не блокировать event loop.
    """
    loop = asyncio.get_running_loop()
    def sync_work():
        update_leads_from_crm(chat_id, leads)
    await loop.run_in_executor(None, sync_work)
    logging.info('update_leads_from_crm_async завершён.')


def update_leads_from_crm(chat_id, leads):
    """
    Обновляем количество лидов у уже существующей записи user_info за сегодня.
    Если записи нет, создаём новую.
    """
    logging.info(f'Функция update_leads_from_crm: chat_id={chat_id}, leads={leads}')
    with Session() as session:
        name_entry = session.query(names_table).filter_by(group_id=chat_id).first()
        if not name_entry:
            logging.info(f"Не найден пользователь с chat_id: {chat_id}")
            return

        real_user_id = name_entry.real_user_id
        logging.info(f"Найден user_id={real_user_id} для chat_id={chat_id}")

        today = datetime.now().date()
        user_info = session.query(UserInfo).filter(
            and_(UserInfo.user_id == real_user_id, func.date(UserInfo.date) == today)
        ).first()

        if user_info:
            # Уже есть запись за сегодня — добавляем лиды
            user_info.leads += leads
            session.commit()
            logging.info(f"Обновлены лиды для user_id={real_user_id}, добавлено {leads}.")
        else:
            # Нет записи, создаём новую
            new_user_info = UserInfo(
                user_id=real_user_id,
                date=datetime.now(),
                leads=leads,
                started=True  # если хотим считать, что день начался
            )
            session.add(new_user_info)
            session.commit()
            logging.info(f"Создана новая запись user_info для user_id={real_user_id}, leads={leads}.")

    # Запуск обновления Google Sheets
    import export_google
    asyncio.run(export_google.update_user_data())


def end_work(user_id, end_time):
    logging.info(f'end_work(user_id={user_id}, end_time={end_time}) запущен.')
    with Session() as session:
        today = end_time.date()
        user = session.query(UserInfo).filter(
            and_(
                UserInfo.user_id == user_id,
                UserInfo.end_time.is_(None),
                func.date(UserInfo.start_time) == today
            )
        ).first()
        if user:
            user.end_time = end_time
            session.commit()
            logging.info(f"End time успешно записан для user_id={user_id}")

            work_duration = end_time - user.start_time
            daily_str = format_duration(work_duration)

            # Суммируем всё время и лиды
            all_records = session.query(UserInfo).filter_by(user_id=user_id).all()
            total_duration = timedelta()
            total_leads = 0
            for rec in all_records:
                if rec.end_time:
                    total_duration += (rec.end_time - rec.start_time)
                    total_leads += rec.leads

            total_str = format_duration(total_duration)
            daily_msg = f"Ты сегодня проработал {daily_str}, закрыл {user.leads} лида(ов)."
            total_msg = f"За всё время ты проработал {total_str} и закрыл {total_leads} лида(ов)."
            return daily_msg, total_msg
        else:
            logging.info(f"Нет незавершённых записей для user_id={user_id} за {today}")
            return "Пользователь не найден или работа уже завершена.", ""


def add_admin_to_db(user_id, user_name, amocrm_id, language, rop_username=None, rank=1, username=None):
    """
    Универсальная функция для добавления в таблицу names пользователя с rank=1(менеджер), 2(валидатор), 3(РОП).
    """
    try:
        with Session() as session:
            result = session.query(names_table.c.real_user_id).filter_by(real_user_id=user_id).first()
            if result:
                session.execute(
                    names_table.update().where(names_table.c.real_user_id == user_id).values(
                        real_name=user_name,
                        amocrm_id=amocrm_id,
                        language=language,
                        rop_username=rop_username,
                        rank=rank,
                        username=username or 'username'
                    )
                )
            else:
                session.execute(
                    names_table.insert().values(
                        real_user_id=user_id,
                        real_name=user_name,
                        amocrm_id=amocrm_id,
                        language=language,
                        rop_username=rop_username,
                        rank=rank,
                        username=username or 'username'
                    )
                )
            session.commit()
            logging.info(f"add_admin_to_db: user_id={user_id} added/updated rank={rank}, username={username}")
    except Exception as e:
        logging.error(f"Failed to add/update user in names_table: {e}")


def del_manager_from_db(user_id):
    """
    Удаляем пользователя rank=1 (или любого rank) из names.
    """
    try:
        with Session() as session:
            row = session.query(names_table.c.real_user_id, names_table.c.real_name).filter(
                names_table.c.real_user_id == user_id
            ).first()
            if row:
                real_user_id, real_name = row
                session.query(names_table).filter(names_table.c.real_user_id == user_id).delete()
                session.commit()
                return f"Пользователь {real_name}, user_id {real_user_id} удалён."
            else:
                return f"Пользователь user_id={user_id} не найден."
    except Exception as e:
        logging.error(f"del_manager_from_db error: {e}")
        return f"Ошибка при удалении user_id={user_id}"


def show_state_list():
    """
    Отображаем всех пользователей, группируя по rank:
    rank=1 -> Менеджеры
    rank=2 -> Валидаторы
    rank=3 -> РОП
    """
    try:
        with Session() as session:
            all_users = session.execute(select(
                names_table.c.real_name,
                names_table.c.rank
            )).fetchall()

            # Сгруппируем по rank
            managers = []
            validators = []
            rops = []
            for row in all_users:
                real_name, rank = row[0], row[1]
                if rank == 1:
                    managers.append(real_name)
                elif rank == 2:
                    validators.append(real_name)
                elif rank == 3:
                    rops.append(real_name)

            output_managers = "Менеджеры:\n" + ("\n".join(managers) if managers else "Нет менеджеров.")
            output_validators = "Валидаторы:\n" + ("\n".join(validators) if validators else "Нет валидаторов.")
            output_rops = "РОПы:\n" + ("\n".join(rops) if rops else "Нет РОПов.")

            # Выведите одним блоком или тремя — на ваше усмотрение:
            return (output_managers, output_validators, output_rops)

    except Exception as e:
        logging.error(f"Ошибка show_state_list: {e}")
        return ("Ошибка при извлечении менеджеров.", "Ошибка при извлечении валидаторов.", "Ошибка при извлечении РОПов.")
    

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


async def get_amocrm_id_by_name(name):
    url = "http://127.0.0.1:4040/get_amocrm_id_by_name"  # Замените на реальный IP и порт сервера
    data = {
        "name": str(name)
    }
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=data)
            response.raise_for_status()
            logging.info(f"Имя '{name}' успешно отправлены на сервер")
            result = response.json()
            return result 
    except Exception as e:
        logging.error(f"get_amocrm_id_by_name - Непредвиденная ошибка: {str(e)}")

def get_head_username_from_telegram(head_id: int):
    """
    Получить username руководителя по его chat_id (head_id) через Telegram API.
    Возвращает username или None, если не удалось получить.
    """
    url = f"https://api.telegram.org/bot{API_TOKEN}/getChat?chat_id={head_id}"
    try:
        response = requests.get(url)
        data = response.json()
        if data.get("ok"):
            return data["result"].get("username")
        else:
            print(f"Не удалось получить username для head_id {head_id}, ответ: {data}")
            return None
    except Exception as e:
        print(f"Ошибка при запросе username для head_id {head_id}: {e}")
        return None
    

def mark_report_received(user_id: int, message_datetime: datetime):
    """
    Ставим has_photo=1 у записи, соответствующей «дню», вычисленному 
    по смещённым суткам (с 18:00 до 18:00). 
    
    Логика:
    - Берём фактическую дату message_datetime (например, 2024-12-25 19:05).
    - Если время >= 18:00, то отчёт считаем за «следующий календарный день».
    - Иначе — за «тот же календарный день».
    - Находим (или создаём) запись в user_info за этот день и выставляем has_photo=1.
    """
    # Приведём время к нужному часовому поясу, если нужно (пример не учитывает TZ).
    local_dt = message_datetime  # Если уже local, то ок

    # Получаем локальную дату
    day = local_dt.date()  # напр. 2024-12-25
    hour = local_dt.hour   # напр. 19

    # Смещённая логика:
    # Если >= 18, то отчёт идёт за day+1
    if hour >= 18:
        day = day + timedelta(days=1)
    logging.info(f"Запущен mark_report_received для user_id={user_id}, day={day}.")

    with Session() as local_session:
        # Ищем запись за «этот» day
        user_info = local_session.query(UserInfo).filter(
            and_(
                UserInfo.user_id == user_id,
                func.date(UserInfo.date) == day
            )
        ).first()

        if user_info:
            # Запись есть — обновляем has_photo=1
            user_info.has_photo = 1
            local_session.commit()
            logging.info(f"Отчёт обновлён: has_photo=1 для user_id={user_id}, day={day}")
        else:
            # Создаём новую запись (без start_time, если ещё нет)
            new_record = UserInfo(
                user_id=user_id,
                date=day,       # Дата — уже со смещением
                has_photo=1
            )
            local_session.add(new_record)
            local_session.commit()
            logging.info(f"Создана новая запись user_info (has_photo=1) для user_id={user_id}, day={day}")



async def check_daily_reports(key_in_messages_table: str):
    """
    Получаем локализованную фразу из таблицы messages по key_in_messages_table.
    Тегаем @username.
    """
    logging.info(f"check_daily_reports(key='{key_in_messages_table}') запущен.")
    today = datetime.now().date()

    from sqlalchemy.orm import aliased
    ui = aliased(UserInfo)

    with Session() as session:  # Но SessionLocal() не async, поэтому придётся оставить sync style
        # Здесь, чтобы не ломать сильно код, оставим sync контекст
        session_sync = Session()

        query = (session_sync.query(
                    names_table.c.real_user_id,
                    names_table.c.group_id,
                    names_table.c.language,
                    names_table.c.username,
                    ui.has_photo
                )
                .outerjoin(ui, and_(
                    ui.user_id == names_table.c.real_user_id,
                    func.date(ui.date) == today
                ))
                .filter((ui.id == None) | (ui.has_photo == 0))
        )

        users = query.all()
        session_sync.close()

    for (u_id, group_id, lang, username_in_db, has_photo) in users:
        if not group_id:
            logging.info(f"Не найден group_id для user_id={u_id}")
            continue

        # Получаем текст из messages по ключу key_in_messages_table
        phrase_text = get_message_for_user(key_in_messages_table, lang or 'ru')
        mention = f"@{username_in_db or 'manager'}"

        text_to_send = f"{mention}, {phrase_text}"
        await send_message_to_group(group_id, text_to_send)


async def send_message_to_group(chat_id: int, text: str):
    """
    Асинхронная отправка сообщения в Telegram.
    """
    url = f"https://api.telegram.org/bot{API_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    logging.info(f"send_message_to_group -> chat_id={chat_id}, text='{text}'")
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, data=payload)
        if resp.status_code == 200:
            logging.info(f"Сообщение отправлено успешно: {resp.status_code}")
        else:
            logging.error(f"Ошибка при отправке: {resp.text}")

    # Опционально: debug-message в отдельный чат
    await send_debug_message(chat_id, text)
    

async def send_debug_message(chat_id: int, message_text: str):
    """
    Отправить debug-сообщение в лог-чат, например -4529397186.
    """
    debug_chat_id = -4529397186
    debug_text = f"[DEBUG] chat_id={chat_id}: {message_text}"
    url = f"https://api.telegram.org/bot{API_TOKEN}/sendMessage"
    payload = {"chat_id": debug_chat_id, "text": debug_text}
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, data=payload)
        if resp.status_code != 200:
            logging.error(f"Ошибка при отправке debug: {resp.text}")


async def send_report_1_message(user_id: int, user_language: str):
    """
    Отправляет сообщение с ключом 'report_1' пользователю user_id, получая текст из БД по языку user_language,
    и затем отправляет это сообщение через функцию send_debug_message.
    """
    phrase_text = get_message_for_user('report_1', user_language)
    mention = f"@someusername"  # или получить username пользователя, если нужно
    text_to_send = f"{mention}, {phrase_text}"
    await send_debug_message(user_id, text_to_send)



def get_all_rops():
    """
    Возвращает список (rop_username, rop_real_name) для всех rank=3.
    """
    with Session() as local_session:
        rows = local_session.execute(
            select(names_table.c.username, names_table.c.real_name)
            .where(names_table.c.rank == 3)
        ).fetchall()
        # rows -> [(username, real_name), ...]
        rops = [(row[0], row[1]) for row in rows if row[0]]  # username, real_name
    return rops


def del_manager_from_db_by_name(real_name: str):
    """
    Удаляем пользователя из таблицы names по реальному имени (AMO CRM).
    """
    try:
        with Session() as local_session:
            row = local_session.execute(
                select(names_table.c.real_user_id, names_table.c.real_name)
                .where(names_table.c.real_name == real_name)
            ).fetchone()
            if row:
                real_user_id, db_real_name = row
                local_session.execute(
                    names_table.delete().where(names_table.c.real_user_id == real_user_id)
                )
                local_session.commit()
                return f"Пользователь '{db_real_name}' (ID={real_user_id}) удалён."
            else:
                return f"Пользователь с именем '{real_name}' не найден."
    except Exception as e:
        logging.error(f"Ошибка при удалении пользователя по имени '{real_name}': {e}")
        return f"Ошибка при удалении пользователя '{real_name}'"
