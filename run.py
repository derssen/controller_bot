import logging
import asyncio
from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand
from aiogram.fsm.storage.memory import MemoryStorage
from fastapi import FastAPI
import uvicorn
from contextlib import asynccontextmanager

from app.scheduler import scheduler, check_scheduler_status  # Обновленный импорт
from config import API_TOKEN
from app.handlers import router
from app.database.requests import update_leads_from_crm_async
from app.database.models import LeadData

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


async def set_commands(bot: Bot):
    commands = [
        BotCommand(command="/start", description="Начать работу"),
        BotCommand(command="/help", description="Помощь")
    ]
    await bot.set_my_commands(commands)


@asynccontextmanager
async def lifespan(appi: FastAPI):
    # Код при старте приложения
    logging.basicConfig(level=logging.INFO)
    bot = Bot(token=API_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    check_scheduler_status()  # Обновленный вызов функции
    await set_commands(bot)
    asyncio.create_task(dp.start_polling(bot))
    yield
    # Код при завершении приложения (если нужно)


appi = FastAPI(lifespan=lifespan)


# Эндпоинт для получения данных
@appi.post("/update_leads")
async def update_leads(lead_data: LeadData):
    chat_id = lead_data.chat_id
    lead_count = lead_data.lead_count
    logging.info(f'Получены данные: chat_id={chat_id}, lead_count={lead_count}')
    # Выполните нужные действия с данными (например, обновление базы данных)
    await update_leads_from_crm_async(chat_id, lead_count)
    return {"status": "success", "message": "Data received", "chat_id": chat_id, "lead_count": lead_count}


if __name__ == '__main__':
    uvicorn.run("run:appi", host="0.0.0.0", port=4046)