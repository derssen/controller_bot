import logging
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from app.database.models import UserInfo
from app.database.requests import engine, end_work, send_daily_leads_to_group
from export_google import get_user_name, fetch_user_data, format_data_for_sheet, update_sheet


# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Определение часового пояса для GMT+8
bali_tz = timezone(timedelta(hours=8))

# Создание сессии для работы с базой данных
Session = sessionmaker(bind=engine)

def end_work_automatically():
    """
    Автоматически завершает рабочий день для всех пользователей, которые не завершили его до 23:59.
    """
    try:
        session = Session()
        # Убираем привязку к таймзоне для корректного сравнения с наивным временем из базы данных
        now = datetime.now(bali_tz).replace(hour=23, minute=59, second=0, microsecond=0).replace(tzinfo=None)

        # Находим всех пользователей, у которых еще не завершен рабочий день, но начат
        active_users = session.query(UserInfo).filter(
            UserInfo.end_time == None,  # Рабочий день еще не завершен
            UserInfo.start_time <= now  # Сравниваем с наивным временем
        ).all()

        # Список пользователей, для которых произошло авто-закрытие
        users_for_export = []

        for user in active_users:
            # Обновляем время окончания только если время начала меньше текущего
            if user.start_time < now:
                user.end_time = now
                logging.info(f"Автоматически завершен рабочий день для пользователя {user.user_id}.")
                users_for_export.append(user)  # Добавляем в список для экспорта

        session.commit()

        # Если есть пользователи для экспорта, выполняем экспорт в Google таблицу
        if users_for_export:
            for user in users_for_export:
                real_name = get_user_name(user.user_id)  # Получаем имя пользователя для Google Sheets
                if real_name:
                    user_data = fetch_user_data(user.user_id)  # Получаем данные для этого пользователя
                    formatted_data = format_data_for_sheet(user_data)  # Форматируем данные для Google Sheets
                    update_sheet(real_name, formatted_data)  # Обновляем лист в Google Sheets для пользователя
            logging.info(f"Данные экспортированы для {len(users_for_export)} пользователей.")

    except Exception as e:
        session.rollback()
        logging.error(f"Произошла ошибка при автоматическом завершении работы: {e}")
    finally:
        session.close()



async def send_message_to_user(user_id, message):
    try:
        await message.answer('Тут будет ваша статистика')
#        daily_message, total_message = end_work(user_id, end_time)
 #       await message.answer(daily_message)
  #      await message.answer(total_message)'''
        print(f"Сообщение отправлено пользователю {user_id}: {message}")
    except Exception as e:
        print(f"Ошибка при отправке сообщения пользователю {user_id}: {e}")


def check_scheduler_status():
    """
    Проверить состояние планировщика и его заданий.
    """
    # Получаем текущее время
    current_time = datetime.now(bali_tz)
    
    # Логируем текущее время один раз при проверке
    logging.info(f"Текущее время: {current_time.strftime('%Y-%m-%d %H:%M:%S')} (по времени Бали)")
    
    jobs = scheduler.get_jobs()
    if jobs:
        for job in jobs:
            logging.info(f"Задание '{job.id}' запланировано на {job.next_run_time}.")
    else:
        logging.info("Нет запланированных заданий.")
    


# Настройка планировщика
scheduler = BackgroundScheduler(timezone=bali_tz)
scheduler.add_job(send_daily_leads_to_group, 'cron', hour=18, minute=22)
scheduler.add_job(end_work_automatically, 'cron', hour=23, minute=59)
#scheduler.add_job(end_work_automatically, 'interval', minutes=1)

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