from aiogram import Router
from aiogram.types import Message
from app.database.requests import (
    add_general_info, add_user_info, get_random_phrase, 
    update_leads, end_work, session
)
from app.database.models import UserInfo
from datetime import datetime
import export_google 

router = Router()

@router.message(lambda message: message.text and message.text.lower() == "старт")
async def start_work(message: Message):
    try:
        user_id = message.from_user.id
        print(f"Received 'старт' command from user_id: {user_id}")

        general_id = add_general_info()
        print(f"Generated general_id: {general_id}")

        add_user_info(user_id, general_id, start_time=datetime.utcnow(), started=True)
        print(f"Added user info for user_id: {user_id}, general_id: {general_id}")

        phrase = get_random_phrase()
        print(f"Fetched motivational phrase: {phrase}")

        await message.answer(phrase)
        await message.answer("Продуктивного дня!")
        print("Sent start message to user.")
        
    except Exception as e:
        if 'bot was blocked by the user' in str(e):
            print("Bot was blocked by the user.")
        else:
            print(f"An error occurred: {e}")

@router.message(lambda message: message.text and message.text.startswith("+"))
async def add_lead(message: Message):
    try:
        user_id = message.from_user.id
        leads = int(message.text.lstrip('+'))
        update_leads(user_id, leads)
        #await message.answer("Лиды учтены!")  # по требованию заказчика
    except Exception as e:
        if 'bot was blocked by the user' in str(e):
            print("Bot was blocked by the user.")

@router.message(lambda message: message.text and message.text.lower() == "финиш")
async def finish_work(message: Message):
    try:
        user_id = message.from_user.id
        user = session.query(UserInfo).filter_by(user_id=user_id, end_time=None).first()
        
        if user and user.started:
            daily_message, total_message = end_work(user_id, end_time=datetime.utcnow())
            await message.answer(daily_message)
            await message.answer(total_message)
        elif user and not user.started:
            await message.answer("Ты сегодня не закрыл ни одного лида, в следующий раз постарайся лучше!")
        else:
            await message.answer("Вы не начали работу. Пожалуйста, используйте команду 'старт' для начала работы.")
    except Exception as e:
        if 'bot was blocked by the user' in str(e):
            print("Bot was blocked by the user.")
    export_google.main()    
