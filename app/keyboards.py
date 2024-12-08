from aiogram.types import (ReplyKeyboardMarkup, KeyboardButton,
                           InlineKeyboardMarkup, InlineKeyboardButton)
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from app.database.requests import get_all_heads

start = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text='Добавить менеджера'), KeyboardButton(text='Добавить руководителя')],
    [KeyboardButton(text='Удалить менеджера'), KeyboardButton(text='Удалить руководителя')],
    [KeyboardButton(text='Перечень сотрудников'), KeyboardButton(text='Обновить данные таблиц')],
    [KeyboardButton(text='Обновить форматирование таблиц')]
],
                           resize_keyboard=True,
                           input_field_placeholder='Выберите действие...')

def get_heads_keyboard():
    """
    Создает инлайн-клавиатуру с именами всех руководителей.
    """
    builder = InlineKeyboardBuilder()
    heads = get_all_heads()  # heads: [(head_name, head_id), ...]

    for head_name, head_id in heads:
        builder.button(text=head_name, callback_data=f"choose_rop_{head_id}")

    # Настраиваем расположение кнопок, например, по 1 в ряд
    builder.adjust(1)

    # Возвращаем сгенерированную клавиатуру
    return builder.as_markup()