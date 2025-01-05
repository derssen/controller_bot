# scheduler.py
import logging
import asyncio
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from app.database.models import UserInfo
from app.database.requests import engine, check_daily_reports, send_report_1_message
import export_google

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

bali_tz = timezone(timedelta(hours=8))
Session = sessionmaker(bind=engine)

def end_work_automatically():
    """
    Автоматически завершает рабочий день для всех пользователей, которые не завершили его до 23:59.
    """
    try:
        session = Session()
        now = datetime.now(bali_tz).replace(hour=23, minute=59, second=0, microsecond=0).replace(tzinfo=None)

        active_users = session.query(UserInfo).filter(
            UserInfo.end_time == None,
            UserInfo.start_time <= now
        ).all()

        for user in active_users:
            if user.start_time < now:
                user.end_time = now
                logging.info(f"Автоматически завершен рабочий день для пользователя {user.user_id}.")
        session.commit()

    except Exception as e:
        session.rollback()
        logging.error(f"Произошла ошибка при автоматическом завершении работы: {e}")
    finally:
        session.close()

async def send_message_to_user(user_id, message):
    try:
        # Пример асинхронной логики отправки сообщения
        print(f"Сообщение отправлено пользователю {user_id}: {message}")
    except Exception as e:
        print(f"Ошибка при отправке сообщения пользователю {user_id}: {e}")

def update_google_sheet_wrapper():
    asyncio.run(update_google_sheet())

async def update_google_sheet():
    try:
        await export_google.main()
        print(f"Запущено обновление Google Sheet")
    except Exception as e:
        print(f"Ошибка при новлении Google Sheet: {e}")

def check_scheduler_status():
    current_time = datetime.now(bali_tz)
    logging.info(f"Текущее время: {current_time.strftime('%Y-%m-%d %H:%M:%S')} (по времени Бали)")
    jobs = scheduler.get_jobs()
    if jobs:
        for job in jobs:
            logging.info(f"Задание '{job.id}' запланировано на {job.next_run_time}.")
    else:
        logging.info("Нет запланированных заданий.")

def check_daily_reports_wrapper(key: str):
    """
    Обёртка над check_daily_reports, чтобы вызывать её асинхронно.
    Теперь key – это ключ из таблицы messages, например 'report_1', 'report_2'.
    """
    asyncio.run(check_daily_reports(key))

def send_report_1_message_wrapper(user_id: int, user_language: str):
    """
    Обёртка над check_daily_reports, чтобы вызывать её асинхронно.
    Теперь key – это ключ из таблицы messages, например 'report_1', 'report_2'.
    """
    asyncio.run(send_report_1_message(123, 'en'))

scheduler = BackgroundScheduler(timezone=bali_tz)
scheduler.add_job(end_work_automatically, 'cron', hour=23, minute=59)
scheduler.add_job(update_google_sheet_wrapper, 'cron', hour=1, minute=0)

# Вместо передачи готовой строки, передаем ключ 'report_1' для 12:00 и 'report_2' для 13:55.
scheduler.add_job(
    lambda: asyncio.run(check_daily_reports_wrapper("report_1")), 
    'cron',
    day_of_week='mon-fri',
    hour=12,
    minute=0,
    id='daily_reports_12'
)

scheduler.add_job(
    lambda: asyncio.run(check_daily_reports_wrapper("report_2")), 
    'cron',
    day_of_week='mon-fri',
    hour=13,
    minute=55,
    id='daily_reports_13_55'
)

scheduler.add_job(
    lambda: asyncio.run(send_report_1_message_wrapper(123, 'en')), 
    'cron',
    day_of_week='mon-fri',
    hour=23,
    minute=52,
    id='daily_reports_23_40'
)


scheduler.start()
logging.info("Планировщик запущен и ожидает выполнения заданий.")

if __name__ == "__main__":
    try:
        logging.info("Планировщик запущен...")
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logging.info("Планировщик остановлен.")
