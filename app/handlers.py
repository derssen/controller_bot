# handlers.py
import logging
import asyncio
from datetime import datetime

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext

from config import ALLOWED_IDS
from app.database.models import UserInfo, AddUserState, DelUserState
from app.database.requests import (
    add_user_info, get_random_phrase, get_eng_random_phrase,
    end_work, update_group_id,
    del_manager_from_db_by_name, show_state_list,
    get_language_by_chat_id, get_amocrm_id_by_name, mark_report_received
)
import export_google
import app.keyboards as kb

router = Router()
logging.basicConfig(level=logging.INFO)

# Указываем ID лог-чата, чтобы не “захардкоживать” много раз
LOG_CHAT_ID = -4529397186


@router.message(CommandStart())
async def cmd_start(message: Message):
    if message.from_user.id in ALLOWED_IDS:
        await message.answer("Привет! Вы можете добавить или удалить пользователя.",
                             reply_markup=kb.start)
    else:
        await message.answer("Привет! У вас нет прав для взаимодействия с этим ботом.")


@router.message(Command(commands=['help']))
async def cmd_help(message: Message):
    if message.from_user.id in ALLOWED_IDS:
        await message.answer("Справка: используйте кнопки для управления пользователями и таблицами.")
    else:
        await message.answer("У вас нет прав для использования этого бота.")


# ====================== Кнопки меню ======================

@router.message(F.text == "Добавить пользователя", F.from_user.id.in_(ALLOWED_IDS))
async def add_user_cmd(message: Message, state: FSMContext):
    """
    При добавлении пользователя спрашиваем user_id (или пересланное сообщение),
    после чего выбираем категорию/имя/язык и т.д.
    """
    await message.answer("Перешлите сообщение от пользователя или введите user_id.")
    await state.set_state(AddUserState.waiting_for_user)


@router.message(F.text == "Удалить пользователя", F.from_user.id.in_(ALLOWED_IDS))
async def del_user_cmd(message: Message, state: FSMContext):
    """
    При удалении пользователя нужно знать реальное имя (AMO CRM).
    """
    await message.answer(
        'Введите имя (AMO CRM) пользователя, которого нужно удалить. '
        'Оно должно соответствовать <a href="https://baliloversagency.amocrm.ru/settings/users/">AMO CRM</a>.',
        parse_mode='HTML'
    )
    await state.set_state(DelUserState.waiting_for_user_to_delete)


@router.message(StateFilter(DelUserState.waiting_for_user_to_delete))
async def process_del_user_name(message: Message, state: FSMContext):
    """
    Удаляем пользователя по его реальному имени (AMO CRM).
    """
    real_name = message.text.strip()
    output_text = del_manager_from_db_by_name(real_name)
    await message.answer(output_text)
    await state.clear()


@router.message(F.text == "Перечень сотрудников", F.from_user.id.in_(ALLOWED_IDS))
async def show_staff(message: Message):
    managers_str, validators_str, rops_str = show_state_list()
    answer_text = f"{managers_str}\n\n{validators_str}\n\n{rops_str}"
    await message.answer(answer_text)


@router.message(F.text == "Обновить форматирование таблиц", F.from_user.id.in_(ALLOWED_IDS))
async def update_format_google(message: Message):
    await message.answer("Обновление форматирования запущено...")
    loop = asyncio.get_running_loop()

    def run_main_sync():
        asyncio.run(export_google.main())

    await loop.run_in_executor(None, run_main_sync)
    await message.answer("Обновлено!")


@router.message(F.text == "Обновить данные таблиц", F.from_user.id.in_(ALLOWED_IDS))
async def update_date_google(message: Message):
    await message.answer("Обновление данных запущено...")
    loop = asyncio.get_running_loop()

    def run_update_user_data_sync():
        asyncio.run(export_google.update_user_data())

    await loop.run_in_executor(None, run_update_user_data_sync)
    await export_google.update_user_data()
    await message.answer("Обновлено!")


# ====================== Добавление пользователя ======================

@router.message(StateFilter(AddUserState.waiting_for_user))
async def process_user_id(message: Message, state: FSMContext):
    """
    После нажатия «Добавить пользователя» – ждём user_id (пересланное сообщение или int).
    """
    if message.forward_from:
        user_id = message.forward_from.id
        user_username = message.forward_from.username or "username_not_found"
    elif message.text.isdigit():
        entered_id = int(message.text)
        try:
            user_chat = await message.bot.get_chat(entered_id)
            user_username = user_chat.username or "username_not_found"
            user_id = entered_id
        except Exception as e:
            await message.answer(f"Не удалось получить данные для user_id {entered_id}: {e}")
            return
    else:
        await message.answer("Неправильный формат. Перешлите сообщение или введите user_id (числом).")
        return

    await state.update_data(user_id=user_id, username=user_username)
    # Показываем inline-кнопки выбора категории
    await message.answer("Выберите категорию пользователя:", reply_markup=kb.get_ranks_keyboard())
    await state.set_state(AddUserState.waiting_for_rank)


@router.callback_query(F.data.startswith("choose_rank_"), StateFilter(AddUserState.waiting_for_rank))
async def process_user_rank(callback: CallbackQuery, state: FSMContext):
    rank_str = callback.data.split("_")[-1]
    rank = int(rank_str)
    await state.update_data(user_rank=rank)

    await callback.message.edit_text(
        'Введите имя для менеджера. Оно должно соответствовать <a href="https://baliloversagency.amocrm.ru/settings/users/">AMO CRM</a>.',
        parse_mode='HTML'
    )
    await state.set_state(AddUserState.waiting_for_name)


@router.message(StateFilter(AddUserState.waiting_for_name))
async def process_user_real_name(message: Message, state: FSMContext):
    admin_name = message.text.strip()
    await state.update_data(admin_name=admin_name)

    await message.answer("Введите язык пользователя (ru / en).")
    await state.set_state(AddUserState.waiting_for_language)


@router.message(StateFilter(AddUserState.waiting_for_language))
async def process_user_language(message: Message, state: FSMContext):
    language = message.text.strip().lower()
    if language not in ("ru", "en"):
        await message.answer("Некорректный язык, введите 'ru' или 'en'.")
        return

    from app.database.requests import get_amocrm_id_by_name
    data = await state.get_data()
    user_id = data['user_id']
    category = data['user_rank']
    admin_name = data['admin_name']
    username = data['username']

    amocrm_id_data = await get_amocrm_id_by_name(admin_name)
    if not amocrm_id_data:
        amocrm_id = 999999
    elif isinstance(amocrm_id_data, int):
        amocrm_id = amocrm_id_data
    elif isinstance(amocrm_id_data, dict):
        amocrm_id = amocrm_id_data.get('amocrm_id', 999999)
    else:
        amocrm_id = 999999

    # Если это РОП (category=3), сам себе "ответственный РОП"
    from app.database.requests import add_admin_to_db
    if category == 3:
        add_admin_to_db(
            user_id=user_id,
            user_name=admin_name,
            amocrm_id=amocrm_id,
            language=language,
            rank=category,
            username=username,
            rop_username=username
        )
        cat_name = {1: "Менеджер", 2: "Валидатор", 3: "РОП"}.get(category, "неизвестно")
        await message.answer(f"{cat_name} {admin_name} (user_id={user_id}) добавлен. Язык={language}, AMO={amocrm_id}.")
        await state.clear()

        # Обновляем Google Sheet
        loop = asyncio.get_running_loop()

        def run_main_after_adding_user():
            asyncio.run(export_google.main())

        await loop.run_in_executor(None, run_main_after_adding_user)
    else:
        # category=1 или 2 => нужно выбрать РОП из inline-кнопок
        from app.database.requests import get_all_rops
        rops_data = get_all_rops()  # [(rop_username, real_name), ...]
        await state.update_data(amocrm_id=amocrm_id, language=language)
        kb_rops = kb.get_rop_inline_keyboard(rops_data)
        await message.answer("Выберите ответственного РОП:", reply_markup=kb_rops)
        await state.set_state(AddUserState.waiting_for_rop_username)


@router.callback_query(F.data.startswith("select_rop_"), StateFilter(AddUserState.waiting_for_rop_username))
async def process_rop_selected(callback: CallbackQuery, state: FSMContext):
    rop_username = callback.data.split("_", maxsplit=2)[2]
    data = await state.get_data()
    user_id = data['user_id']
    admin_name = data['admin_name']
    category = data['user_rank']
    amocrm_id = data['amocrm_id']
    language = data['language']
    username = data['username']

    from app.database.requests import add_admin_to_db
    add_admin_to_db(
        user_id=user_id,
        user_name=admin_name,
        amocrm_id=amocrm_id,
        language=language,
        rank=category,
        username=username,
        rop_username=rop_username
    )

    cat_name = {1: "Менеджер", 2: "Валидатор", 3: "РОП"}.get(category, "неизвестно")
    await callback.message.edit_text(
        f"{cat_name} {admin_name} (user_id={user_id}) добавлен. Язык={language}, AMO={amocrm_id}, РОП=@{rop_username}."
    )
    await state.clear()

    # Обновляем Google Sheet
    loop = asyncio.get_running_loop()

    def run_main_after_adding_user():
        asyncio.run(export_google.main())

    await loop.run_in_executor(None, run_main_after_adding_user)


# ====================== Старт/Финиш ======================

@router.message(lambda msg: msg.text and msg.text.lower() in ["старт", "start"])
async def start_work(message: Message):
    user_id = message.from_user.id
    group_id = message.chat.id
    logging.info(f"Пользователь {user_id} активировал обработчик start_work.")  # Логируем
    try:
        start_time = datetime.now()
        add_user_info(user_id, start_time, started=True)
        update_group_id(user_id, group_id)

        language = get_language_by_chat_id(group_id) or 'ru'
        if language == 'en':
            phrase = get_eng_random_phrase()
            text = "Have a productive day!"
        else:
            phrase = get_random_phrase()
            text = "Продуктивного дня!"

        logging.info(f"User {user_id} start_work -> group_id={group_id}, lang={language}")
        await message.answer(phrase)
        await message.answer(text)

        try:
            await export_google.update_user_data()
        except TypeError as e:
            logging.error(f"start_work -> update_user_data error: {e}")
    except Exception as e:
        logging.error(f"start_work error: {e}")


@router.message(lambda msg: msg.text and msg.text.lower() in ["финиш", "finish", "stop"])
async def finish_work(message: Message):
    user_id = message.from_user.id
    group_id = message.chat.id
    logging.info(f"Пользователь {user_id} активировал обработчик finish_work.")  # Логируем
    try:
        end_time = datetime.now()
        daily_message, total_message = end_work(user_id, end_time)
        update_group_id(user_id, group_id)

        language = get_language_by_chat_id(message.chat.id) or 'ru'
        if language == 'en':
            txt = "Thank you for your work and have a pleasant rest!"
        else:
            txt = "Спасибо за работу и приятного отдыха!"

        await message.answer(txt)
        # Если хотите дополнительно выводить daily_message и total_message
        # await message.answer(daily_message)
        # await message.answer(total_message)

        try:
            await export_google.update_user_data()
        except TypeError as e:
            logging.error(f"finish_work -> update_user_data error: {e}")
    except Exception as e:
        logging.error(f"finish_work error: {e}")


# ====================== Forward всех остальных сообщений ======================
@router.message()
async def forward_message(message: Message):
    """
    Ловим все сообщения, всегда пересылаем медиаконтент в лог-чат,
    а если есть текст или подпись, отсылаем это отдельным сообщением в лог-чат.
    После этого проверяем, нет ли #отчет / #report.
    """
    user_id = message.from_user.id
    sender_name = message.from_user.full_name or "NoName"
    sender_username = f"@{message.from_user.username}" if message.from_user.username else "NoUsername"
    chat_id = message.chat.id
    try:
        # 1) Всегда пересылаем медиаконтент (или текст) как сообщение.
        #    Это гарантирует, что фото/документ/видео появятся в LOG_CHAT_ID.
        await message.forward(chat_id=LOG_CHAT_ID)
        logging.info(f"Message from {user_id} forwarded to {LOG_CHAT_ID} (possible media).")

        # 2) Если есть текст или подпись, отправим отдельным сообщением
        text_or_caption = message.text or message.caption
        if text_or_caption:
            content = (
                f"Сообщение от: {sender_name} ({sender_username})\n"
                f"(Group id: {chat_id})\n"
                f"{text_or_caption}"
            )
            await message.bot.send_message(chat_id=LOG_CHAT_ID, text=content)
            logging.info("Text/caption also sent to log chat.")

        # 3) Проверяем, нет ли в тексте/подписи ключа "#отчет"/"#report"
        if text_or_caption:
            text_lower = text_or_caption.lower()
            if "#отчет" in text_lower or "#report" in text_lower:
                msg_time = datetime.now()
                logging.info(f"{msg_time} Message with report received, user_id={user_id}.")
                mark_report_received(user_id, msg_time)

    except Exception as e:
        logging.error(f"Failed to forward message from user {user_id}: {e}")
