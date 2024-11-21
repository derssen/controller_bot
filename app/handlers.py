from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from app.database.requests import (
    add_general_info, add_user_info, get_random_phrase, 
    update_leads, end_work, session, add_admin_to_db,
    add_head_to_db, update_group_id, get_heads_ids,
    del_manager_from_db, del_head_from_db, show_state_list
)
from app.database.models import UserInfo, AddManagerState, AddHeadState, DelManagerState, DelHeadState
from config import ALLOWED_IDS
from datetime import datetime
import export_google 
import app.keyboards as kb


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

# Обработка нажатия на кнопку "Обновить Google Sheet"
@router.message(F.text == "Обновить Google Sheet", F.from_user.id.in_(ALLOWED_IDS))
async def update_google(message: Message):
    await message.answer("Обновление запущено, пожалуйста подождите...")
    await export_google.main()
    await message.answer("Обновлено!")


# Менеджер: Обработка пересланного сообщения или ввода user_id
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
    await message.answer("Теперь введите желаемое имя для менеджера.")
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
    data = await state.get_data()
    user_id = data['user_id']
    # Вызов функции для добавления администратора в базу данных
    add_admin_to_db(user_id, admin_name)
    await message.answer(f"Менеджер с именем {admin_name} и user_id {user_id} был добавлен.")
    await state.clear()

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

@router.message(lambda message: message.text and message.text.lower() == "старт")
async def start_work(message: Message):
    print('Start pushed')
    try:
        user_id = message.from_user.id
        
        # Проверка, не началась ли работа уже ранее
        user = session.query(UserInfo).filter_by(user_id=user_id, end_time=None).first()
        if user and user.started and user.user_id != '457118082':
            await message.answer("Вы уже начали работу!")
            return
        group_id = message.chat.id
        general_id = add_general_info()
        add_user_info(user_id, general_id, start_time=datetime.now(), started=True)
        group_id = message.chat.id
        update_group_id(user_id, group_id)
        phrase = get_random_phrase()
        await message.answer(phrase)
        await message.answer("Продуктивного дня!")
        export_google.update_one_sheet(message.from_user.id)  # Экспорт данных в Google Sheets

    except Exception as e:
        if 'bot was blocked by the user' in str(e):
            print("Bot was blocked by the user.")
        else:
            print(f"An error occurred: {e}")

@router.message(lambda message: message.text and message.text.startswith("+") and message.from_user.id in heads_ids)
async def add_lead(message: Message):
    try:
        group_id = message.chat.id
        leads = int(message.text.lstrip('+'))
        print(f'Plus{leads} pushed')
        update_leads(group_id, leads)  # Update leads count in the database
        await message.answer("Лиды учтены!")  # Uncomment if a response is needed
        export_google.update_one_sheet(message.from_user.id)  # Export data to Google Sheets
    except Exception as e:
        if 'bot was blocked by the user' in str(e):
            print("Bot was blocked by the user.")
        else:
            print(f"An error occurred: {e}")

@router.message(lambda message: message.text and message.text.lower() == "финиш")
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

        await message.answer("Спасибо за работу и приятного отдыха!")
        export_google.update_one_sheet(message.from_user.id)  # Экспорт данных в Google Sheets

    except Exception as e:
        if 'bot was blocked by the user' in str(e):
            print("Bot was blocked by the user.")
        else:
            print(f"An error occurred: {e}")
