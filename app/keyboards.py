from aiogram.types import (ReplyKeyboardMarkup, KeyboardButton,
                           InlineKeyboardMarkup, InlineKeyboardButton)
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder

start = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text='Добавить менеджера')], [KeyboardButton(text='Удалить менеджера')],
    [KeyboardButton(text='Добавить руководителя')], [KeyboardButton(text='Удалить руководителя')]
],
                           resize_keyboard=True,
                           input_field_placeholder='Выберите действие...')