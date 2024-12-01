from aiogram.types import (ReplyKeyboardMarkup, KeyboardButton,
                           InlineKeyboardMarkup, InlineKeyboardButton)
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder


start = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text='Добавить менеджера'), KeyboardButton(text='Добавить руководителя')],
    [KeyboardButton(text='Удалить менеджера'), KeyboardButton(text='Удалить руководителя')],
    [KeyboardButton(text='Перечень сотрудников'), KeyboardButton(text='Обновить данные таблиц')],
    [KeyboardButton(text='Обновить форматирование таблиц')]
],
                           resize_keyboard=True,
                           input_field_placeholder='Выберите действие...')