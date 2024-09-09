import logging
import asyncio
import app.scheduler

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand
from aiogram.fsm.storage.memory import MemoryStorage
from config import API_TOKEN
from app.handlers import router

async def set_commands(bot: Bot):
    commands = [
        BotCommand(command="/start", description="Начать работу"),
        BotCommand(command="/help", description="Помощь")
    ]
    await bot.set_my_commands(commands)

async def main():
    bot = Bot(token=API_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    
    dp.include_router(router)
    app.scheduler.check_scheduler_status()
    await set_commands(bot)
    await dp.start_polling(bot)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    try: 
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Exit from bot.')