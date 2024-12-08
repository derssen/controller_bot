from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from app.database.requests import (
    add_general_info, add_user_info, get_random_phrase, 
    end_work, session, add_admin_to_db,
    add_head_to_db, update_group_id, get_heads_ids,
    del_manager_from_db, del_head_from_db, show_state_list,
    get_language_by_chat_id, get_eng_random_phrase,
    get_amocrm_id_by_name, get_head_username_by_id
)
from app.database.models import UserInfo, AddManagerState, AddHeadState, DelManagerState, DelHeadState
from config import ALLOWED_IDS
from datetime import datetime
import export_google 
import app.keyboards as kb
import logging 
import asyncio



router = Router()
heads_ids = get_heads_ids()


# Команда /start
@router.message(CommandStart())
async def cmd_start(message: Message):
    if message.from_user.id in ALLOWED_IDS:  
        await message.answer('Привет! Вы можете добавить менеджера.', 
                         reply_markup=kb.start)
    else:
        await message.answer("Привет! \n К сожалению у вас нету никаких прав в этом мире)")


# Команда /help
@router.message(Command(commands=['help']))
async def cmd_help(message: Message):
    if message.from_user.id in ALLOWED_IDS:  
        await message.answer('Тут будет справка, но пока она не нужна.')
    else:
        await message.answer("Привет! \n К сожалению у вас нету никаких прав в этом мире)")


# Обработка нажатия на кнопку "добавить менеджера"
@router.message(F.text == "Добавить менеджера", F.from_user.id.in_(ALLOWED_IDS))
async def add_admin(message: Message, state: FSMContext):
    await message.answer("Пожалуйста, пересланное сообщение от пользователя или введите user_id.")
    await state.set_state(AddManagerState.waiting_for_user)


# Обработка нажатия на кнопку "добавить руководителя"
@router.message(F.text == "Добавить руководителя", F.from_user.id.in_(ALLOWED_IDS))
async def add_admin(message: Message, state: FSMContext):
    await message.answer("Пожалуйста, пересланное сообщение от руководителя или введите user_id.")
    await state.set_state(AddHeadState.waiting_for_user)


# Обработка нажатия на кнопку "удалить менеджера"
@router.message(F.text == "Удалить менеджера", F.from_user.id.in_(ALLOWED_IDS))
async def add_admin(message: Message, state: FSMContext):
    await message.answer("Пожалуйста, пересланное сообщение от пользователя или введите user_id.")
    await state.set_state(DelManagerState.waiting_for_user)


# Обработка нажатия на кнопку "удалить руководителя"
@router.message(F.text == "Удалить руководителя", F.from_user.id.in_(ALLOWED_IDS))
async def add_admin(message: Message, state: FSMContext):
    await message.answer("Пожалуйста, пересланное сообщение от руководителя или введите user_id.")
    await state.set_state(DelHeadState.waiting_for_user)


# Обработка нажатия на кнопку "Перечень сотрудников"
@router.message(F.text == "Перечень сотрудников", F.from_user.id.in_(ALLOWED_IDS))
async def add_admin(message: Message):
    heads, managers = show_state_list()
    await message.answer(heads)
    await message.answer(managers)


# Обработка нажатия на кнопку "Обновить форматирование Google Sheet"
@router.message(F.text == "Обновить форматирование таблиц", F.from_user.id.in_(ALLOWED_IDS))
async def update_format_google(message: Message):
    await message.answer("Обновление форматирования запущено, пожалуйста подождите...")
    loop = asyncio.get_running_loop()
    def run_main_sync():
        # Запускаем асинхронную функцию main() синхронно в отдельном потоке
        asyncio.run(export_google.main())
    # Выполняем run_main_sync в executor, чтобы не блокировать event loop
    await loop.run_in_executor(None, run_main_sync)
    await message.answer("Обновлено!")


# Обработка нажатия на кнопку "Обновить данные Google Sheet"
@router.message(F.text == "Обновить данные таблиц", F.from_user.id.in_(ALLOWED_IDS))
async def update_date_google(message: Message):
    await message.answer("Обновление данных запущено, пожалуйста подождите...")
    loop = asyncio.get_running_loop()
    def run_update_user_data_sync():
        # Запускаем асинхронную функцию main() синхронно в отдельном потоке
        asyncio.run(export_google.update_user_data())
    # Выполняем run_main_sync в executor, чтобы не блокировать event loop
    await loop.run_in_executor(None, run_update_user_data_sync)
    await export_google.update_user_data()
    await message.answer("Обновлено!")


@router.message(AddManagerState.waiting_for_user)
async def process_user_id(message: Message, state: FSMContext):
    if message.forward_from:
        user_id = message.forward_from.id
    elif message.text.isdigit():
        user_id = int(message.text)
    else:
        await message.answer("Неправильный формат. Пожалуйста, пересланное сообщение или введите user_id.")
        return
    await state.update_data(user_id=user_id)
    # Используем HTML-разметку для ссылки
    await message.answer(
        'Введите имя для менеджера. Оно должно соответствовать <a href="https://baliloversagency.amocrm.ru/settings/users/">AMO CRM</a>.',
        parse_mode='HTML'
    )
    await state.set_state(AddManagerState.waiting_for_name)


# Руководитель: Обработка пересланного сообщения или ввода user_id
@router.message(AddHeadState.waiting_for_user)
async def process_head_id(message: Message, state: FSMContext):
    if message.forward_from:
        user_id = message.forward_from.id
    elif message.text.isdigit():
        user_id = int(message.text)
    else:
        await message.answer("Неправильный формат. Пожалуйста, пересланное сообщение или введите user_id.")
        return
    
    await state.update_data(user_id=user_id)
    await message.answer("Теперь введите желаемое имя для руководителя.")
    await state.set_state(AddHeadState.waiting_for_name)


# Менеджер.Удаление: Обработка пересланного сообщения или ввода user_id
@router.message(DelManagerState.waiting_for_user)
async def process_user_id(message: Message, state: FSMContext):
    if message.forward_from:
        user_id = message.forward_from.id
    elif message.text.isdigit():
        user_id = int(message.text)
    else:
        await message.answer("Неправильный формат. Пожалуйста, пересланное сообщение или введите user_id.")
        return
    await state.update_data(user_id=user_id)
    output_text = del_manager_from_db(user_id)
    await message.answer(output_text)


# Руководитель.Удаление: Обработка пересланного сообщения или ввода user_id
@router.message(DelHeadState.waiting_for_user)
async def process_head_id(message: Message, state: FSMContext):
    if message.forward_from:
        user_id = message.forward_from.id
    elif message.text.isdigit():
        user_id = int(message.text)
    else:
        await message.answer("Неправильный формат. Пожалуйста, пересланное сообщение или введите user_id.")
        return
    await state.update_data(user_id=user_id)
    output_text = del_head_from_db(user_id)
    await message.answer(output_text)


# Менеджер: Обработка ввода имени
@router.message(AddManagerState.waiting_for_name)
async def process_admin_name(message: Message, state: FSMContext):
    admin_name = message.text
    await state.update_data(admin_name=admin_name)
    await message.answer("Введите РОПа, закрепленного за менеджером.", reply_markup=kb.get_heads_keyboard())
    await state.set_state(AddManagerState.waiting_for_rop)


# Менеджер: Обработка ввода языка и получение amocrm_id
@router.message(AddManagerState.waiting_for_language)
async def process_language(message: Message, state: FSMContext):
    language = message.text.strip().lower()
    if language not in ['ru', 'en']:
        await message.answer("Неправильный формат. Пожалуйста, введите корректный язык ('ru' или 'en').")
        return
    data = await state.get_data()
    admin_name = data['admin_name']
    user_id = data['user_id']
    rop_username = data['rop_username']  # получаем выбранного РОПа
    
    amocrm_id = await get_amocrm_id_by_name(admin_name)
    if amocrm_id is None:
        await message.answer(f"Не удалось найти amocrm_id для пользователя с именем {admin_name}.")
        await message.answer(f"Проверьте имя в AMO CRM и попробуйте снова.")
        await state.clear()
        return
    
    # Добавляем менеджера в БД, передаем rop_username
    add_admin_to_db(user_id, admin_name, amocrm_id, language, rop_username=rop_username)

    if language == 'en':
        lang_str = 'английский'
    else:
        lang_str = 'русский'        
    await message.answer(f"Менеджер {admin_name} добавлен.\n"
                         f"Телеграм айди - {user_id}.\n"
                         f"AMOCRM айди - {amocrm_id}.\n"
                         f"Интерфейс пользователя - {lang_str}.\n"
                         f"РОП - @{rop_username}")
    await state.clear()
    loop = asyncio.get_running_loop()
    def run_main_after_adding_manager():
        asyncio.run(export_google.main())
    await loop.run_in_executor(None, run_main_after_adding_manager)

    

# Руководитель: Обработка ввода имени
@router.message(AddHeadState.waiting_for_name)
async def process_head_name(message: Message, state: FSMContext):
    head_name = message.text
    data = await state.get_data()
    user_id = data['user_id']
    # Вызов функции для добавления администратора в базу данных
    add_head_to_db(user_id, head_name)
    await message.answer(f"Руководитель с именем {head_name} и user_id {user_id} был добавлен.")
    await state.clear()


@router.message(lambda message: message.text and message.text.lower() in ["старт", "start"])
async def start_work(message: Message):
    print('Start pushed')
    try:
        user_id = message.from_user.id

        # Проверка, не началась ли работа уже ранее
        # user = session.query(UserInfo).filter_by(user_id=user_id, end_time=None).first()
        # if user and user.started and user.user_id != '457118082':
        #     await message.answer("Вы уже начали работу!")
        #     return

        group_id = message.chat.id
        general_id = add_general_info()
        start_time=datetime.now()
        add_user_info(user_id, general_id, start_time, started=True)
        
        update_group_id(user_id, group_id)

        language = get_language_by_chat_id(group_id)
        if language == 'en':
            phrase = get_eng_random_phrase()
            text = "Have a productive day!"
        else:
            phrase = get_random_phrase()
            text = "Продуктивного дня!"
        logging.info(f'Юзеру {group_id} отправлено сообщение: {text}')
        await message.answer(phrase)
        await message.answer(text)
        await export_google.update_user_data()

    except Exception as e:
        if 'bot was blocked by the user' in str(e):
            print("Bot was blocked by the user.")
        else:
            print(f"An error occurred: {e}")


@router.message(lambda message: message.text and message.text.lower() in ["финиш", "finish", "stop"])
async def finish_work(message: Message):
    print('Finish pushed')
    try:
        user_id = message.from_user.id
        end_time = datetime.now()  # Время окончания работы
        
        # Вызов функции end_work для записи времени окончания работы в базу данных
        daily_message, total_message = end_work(user_id, end_time)
        
        # Отправка сообщений пользователю
        #await message.answer(daily_message)
        #await message.answer(total_message)
        language = get_language_by_chat_id(message.chat.id)
        if language == 'en':
            text = "Thank you for your work and have a pleasant rest!"
        else:
            text = "Спасибо за работу и приятного отдыха!"
        await message.answer(text)
        await export_google.update_user_data()

    except Exception as e:
        if 'bot was blocked by the user' in str(e):
            print("Bot was blocked by the user.")
        else:
            print(f"An error occurred: {e}")


@router.message()
async def forward_message(message: Message):
    """
    Forward all received messages to the specified chat ID with sender information.
    """
    try:
        # The target chat ID where messages should be forwarded
        target_chat_id = -4529397186

        # Get sender's name and username
        sender_name = message.from_user.full_name or "No Name"
        sender_username = f"@{message.from_user.username}" if message.from_user.username else "No Username"

        # Create a header with sender information
        sender_info = f"Сообщение от: {sender_name} ({sender_username})\n"

        # Combine sender information with the original message text
        if message.text:  # If the message contains text
            content = sender_info + message.text
            await message.bot.send_message(chat_id=target_chat_id, text=content)
        else:  # If the message contains non-text content, forward it
            await message.forward(chat_id=target_chat_id)

        # Optional: Log the forwarding action
        logging.info(f"Message from {sender_name} ({sender_username}) forwarded to {target_chat_id}.")
    except Exception as e:
        logging.error(f"Failed to forward message: {e}")

@router.callback_query(
    lambda c: c.data and c.data.startswith("choose_rop_"),
    StateFilter(AddManagerState.waiting_for_rop)
)
async def choose_rop_handler(callback: CallbackQuery, state: FSMContext):
    head_id_str = callback.data.split("_")[-1]
    head_id = int(head_id_str)
    rop_username = get_head_username_by_id(head_id)
    await state.update_data(rop_username=rop_username)
    await callback.message.edit_text(f"Руководитель выбран: @{rop_username}")
    await callback.message.answer("Пожалуйста, введите язык менеджера (например, 'ru' или 'en').")
    await state.set_state(AddManagerState.waiting_for_language)