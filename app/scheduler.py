import logging
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from app.database.models import UserInfo
from app.database.requests import engine, end_work

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Определение часового пояса для GMT+8
bali_tz = timezone(timedelta(hours=11))

# Создание сессии для работы с базой данных
Session = sessionmaker(bind=engine)

def end_work_automatically():
    """
    Автоматически завершает рабочий день для всех пользователей в 23:59 по времени Бали.
    """
    try:
        session = Session()
        now = datetime.now(bali_tz).replace(hour=23, minute=59, second=0, microsecond=0)

        # Обновляем записи в базе данных, устанавливая end_time для всех активных записей
        active_users = session.query(UserInfo).filter(UserInfo.end_time == None).all()

        for user in active_users:
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
        daily_message, total_message = end_work(user_id, end_time)
        await message.answer(daily_message)
        await message.answer(total_message)
        print(f"Сообщение отправлено пользователю {user_id}: {message}")
    except Exception as e:
        print(f"Ошибка при отправке сообщения пользователю {user_id}: {e}")


def check_scheduler_status():
    """
    Проверить состояние планировщика и его заданий.
    """
    jobs = scheduler.get_jobs()
    if jobs:
        for job in jobs:
            logging.info(f"Задание '{job.id}' запланировано на {job.next_run_time}.")
    else:
        logging.info("Нет запланированных заданий.")


# Настройка планировщика
scheduler = BackgroundScheduler(timezone=bali_tz)
#scheduler.add_job(end_work_automatically, 'cron', hour=23, minute=59)
scheduler.add_job(end_work_automatically, 'interval', minutes=1)

# Запуск планировщика
scheduler.start()
logging.info("Планировщик запущен и ожидает выполнения заданий.")

# Этот код запускает планировщик и держит его в рабочем состоянии
if __name__ == "__main__":
    try:
        logging.info("Планировщик запущен...")
        while True:
            pass  # Бесконечный цикл для поддержания работы планировщика
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logging.info("Планировщик остановлен.")
