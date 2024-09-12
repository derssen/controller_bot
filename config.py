import os
from dotenv import load_dotenv


load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
#API_TOKEN = '7070684915:AAGgt1P9Tifiw5g9zTOfj8vtigVMCAsLZz0' #test
DATABASE_URL = "sqlite:///database.db"
JSON_FILE = 'round-pen-404209-78c5f6e5c9ea.json'
ALLOWED_IDS = [514900377, 781710702, 1419643201]
GOOGLE_SHEET = "Стата Контролер"
#GOOGLE_SHEET = "STATA" #test
# Маппинг месяцев с английского на русский
MONTHS_EN_TO_RU = {
    'January': 'Январь', 'February': 'Февраль', 'March': 'Март', 'April': 'Апрель', 'May': 'Май', 'June': 'Июнь',
    'July': 'Июль', 'August': 'Август', 'September': 'Сентябрь', 'October': 'Октябрь', 'November': 'Ноябрь', 'December': 'Декабрь'
}