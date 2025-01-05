# keyboards.py
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

start = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text='Добавить пользователя'), KeyboardButton(text='Удалить пользователя')],
    [KeyboardButton(text='Перечень сотрудников'), KeyboardButton(text='Обновить данные таблиц')],
    [KeyboardButton(text='Обновить форматирование таблиц')]
],
    resize_keyboard=True,
    input_field_placeholder='Выберите действие...'
)


def get_ranks_keyboard():
    """
    Инлайн-кнопки для выбора категории: Менеджер(1), Валидатор(2), РОП(3).
    """
    builder = InlineKeyboardBuilder()
    builder.button(text="Менеджер", callback_data="choose_rank_1")
    builder.button(text="Валидатор", callback_data="choose_rank_2")
    builder.button(text="РОП", callback_data="choose_rank_3")
    builder.adjust(1)
    return builder.as_markup()


def get_rop_inline_keyboard(rops_data):
    """
    rops_data: [(rop_username, rop_real_name), ...].
    """
    builder = InlineKeyboardBuilder()
    for rop_un, rop_real_name in rops_data:
        builder.button(text=f"{rop_real_name} (@{rop_un})", callback_data=f"select_rop_{rop_un}")
    builder.adjust(1)
    return builder.as_markup()
