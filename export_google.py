import gspread
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor

from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from datetime import datetime
from gspread_formatting import (
    format_cell_range, CellFormat, TextFormat, Color, Borders, Border
)
from config import JSON_FILE, GOOGLE_SHEET, MONTHS_EN_TO_RU, DATABASE_URL

MONTHS_RU_ORDER = {
    'Январь': 1,
    'Февраль': 2,
    'Март': 3,
    'Апрель': 4,
    'Май': 5,
    'Июнь': 6,
    'Июль': 7,
    'Август': 8,
    'Сентябрь': 9,
    'Октябрь': 10,
    'Ноябрь': 11,
    'Декабрь': 12
}

executor = ThreadPoolExecutor()

# Настройка подключения к базе данных
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()

# Отражение таблиц из базы данных
names_table = Table('names', metadata, autoload_with=engine)
user_info_table = Table('user_info', metadata, autoload_with=engine)

# Настройка Google Sheets API
def authorize_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_FILE, scope)
    client = gspread.authorize(creds)
    return client

def fetch_user_data(user_id):
    query = select(user_info_table).where(user_info_table.c.user_id == user_id)
    user_data = session.execute(query).fetchall()
    return user_data

def get_user_name(user_id):
    query = select(names_table).where(names_table.c.real_user_id == user_id)
    result = session.execute(query).fetchone()
    if result:
        return result[1]  # 'real_name' is the second column in the result
    return None

# Функция для форматирования данных в табличный формат
def format_data_for_sheet(user_data):
    formatted_data = []
    for record in user_data:
        date = record[2]  # Предполагается, что дата находится на индексе 2
        start_time = record[3]
        end_time = record[4]
        leads = record[5]

        # Форматирование даты и времени
        date_str = date.strftime('%d/%m/%Y')
        month_str = date.strftime('%B')
        month_str_ru = MONTHS_EN_TO_RU.get(month_str, month_str)  # Перевод месяца на русский
        start_time_str = start_time.strftime('%H:%M') if start_time else ''
        end_time_str = end_time.strftime('%H:%M') if end_time else ''

        formatted_data.append([
            month_str_ru,
            date_str,
            start_time_str,
            end_time_str,
            leads
        ])
    return formatted_data

def execute_with_retry(func, retries=5, initial_delay=60, delay_on_quota=True):
    for attempt in range(retries):
        try:
            func()
            break  # Если успешно, выходим из цикла
        except gspread.exceptions.APIError as e:
            status = e.response.status_code
            if status == 429:
                print(f"Quota exceeded. Waiting for {initial_delay} seconds before retrying...")
                time.sleep(initial_delay)
                if delay_on_quota:
                    initial_delay *= 2  # Экспоненциальная задержка
            else:
                print(f"An API error occurred: {e}")
                raise
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise
    else:
        print("Max retries exceeded.")
        raise Exception("Failed to execute function after retries.")

def update_hidden_data_sheet(all_data):
    client = authorize_google_sheets()
    spreadsheet = client.open(GOOGLE_SHEET)

    # Создаём или выбираем скрытый лист "Data"
    try:
        data_sheet = spreadsheet.worksheet('Data')
    except gspread.exceptions.WorksheetNotFound:
        data_sheet = spreadsheet.add_worksheet(title='Data', rows="1000", cols="10")
        # Скрываем лист
        data_sheet.hide()

    # Подготовка заголовков
    headers = ['Manager', 'Month', 'Date', 'Start Time', 'End Time', 'Leads', 'Year']

    # Очищаем существующие данные и обновляем
    execute_with_retry(lambda: data_sheet.clear())

    # Обработка данных для удаления пробелов и обеспечения пустых ячеек
    data_with_year = []
    for row in all_data:
        # Добавляем год к каждой строке данных
        date_str = row[2]  # Предполагается, что дата находится на индексе 2
        date_obj = datetime.strptime(date_str, '%d/%m/%Y')
        year = date_obj.year

        # Заменяем "пустые" пробелы на реальные пустые строки
        cleaned_row = [cell.strip() if isinstance(cell, str) else cell for cell in row]
        data_with_year.append(cleaned_row + [str(year)])

    # Обновляем лист очищенными данными
    execute_with_retry(lambda: data_sheet.update([headers] + data_with_year))

def update_manager_sheet(manager_name, months, years):
    client = authorize_google_sheets()
    spreadsheet = client.open(GOOGLE_SHEET)

    # Создаём или выбираем лист менеджера
    try:
        manager_sheet = spreadsheet.worksheet(manager_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{manager_name}' not found. Creating new worksheet.")
        def add_worksheet():
            spreadsheet.add_worksheet(title=manager_name, rows="1000", cols="26")
        execute_with_retry(add_worksheet, retries=5, initial_delay=60)
        manager_sheet = spreadsheet.worksheet(manager_name)

    # Очищаем лист менеджера
    execute_with_retry(lambda: manager_sheet.clear())

    # Устанавливаем имя менеджера в A1
    execute_with_retry(lambda: manager_sheet.update('A1', [[manager_name]]))

    # Определяем текущий месяц и год
    current_datetime = datetime.now()
    current_month_en = current_datetime.strftime('%B')  # Название месяца на английском
    current_year = str(current_datetime.year)

    # Переводим текущий месяц на русский
    current_month_ru = MONTHS_EN_TO_RU.get(current_month_en, current_month_en)

    # Приводим месяцы к нижнему регистру для сравнения
    months_lower = [m.lower() for m in months]

    # Проверяем, есть ли данные для текущего месяца и года
    if current_month_ru.lower() in months_lower:
        default_month = current_month_ru.capitalize()
    else:
        default_month = months[-1].capitalize() if months else ''

    if current_year in years:
        default_year = current_year
    else:
        default_year = years[-1] if years else ''

    # Устанавливаем значения по умолчанию в B2 и D2
    execute_with_retry(lambda: manager_sheet.update('B2', [[default_month]]))
    execute_with_retry(lambda: manager_sheet.update('D2', [[default_year]]))

    # Создаём список месяцев с заглавной буквы для выпадающего списка
    months_capitalized = [month.capitalize() for month in months]

    # Устанавливаем проверку данных в B2 и D2 с списком месяцев и годов
    sheet_id = manager_sheet._properties['sheetId']
    requests = [
        {
            'setDataValidation': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,    # Row 2
                    'endRowIndex': 2,      # Row 3
                    'startColumnIndex': 1, # Column B
                    'endColumnIndex': 2    # Column C
                },
                'rule': {
                    'condition': {
                        'type': 'ONE_OF_LIST',
                        'values': [{'userEnteredValue': month} for month in months_capitalized]
                    },
                    'showCustomUi': True
                }
            }
        },
        {
            'setDataValidation': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,    # Row 2
                    'endRowIndex': 2,
                    'startColumnIndex': 3, # Column D
                    'endColumnIndex': 4    # Column E
                },
                'rule': {
                    'condition': {
                        'type': 'ONE_OF_LIST',
                        'values': [{'userEnteredValue': str(year)} for year in years]
                    },
                    'showCustomUi': True
                }
            }
        }
    ]
    body = {'requests': requests}
    execute_with_retry(lambda: spreadsheet.batch_update(body))

    # Устанавливаем метки в A2:A6
    labels = [['Месяц'], ['Дата'], ['Время работы'], ['Лидов получено'], ['Лидов за месяц итого']]
    execute_with_retry(lambda: manager_sheet.update('A2:A6', labels))

    # Формула для даты (B3)
    date_formula = '''=IFERROR(
  TRANSPOSE(
    UNIQUE(
      FILTER(Data!C2:C,
        (TRIM(Data!A2:A)=TRIM($A$1)) *
        (LOWER(TRIM(Data!B2:B))=LOWER(TRIM($B$2))) *
        (TRIM(Data!G2:G)=TRIM($D$2))
      )
    )
  ),
  "Нет данных"
)'''
    execute_with_retry(lambda: manager_sheet.update('B3', [[date_formula]], value_input_option='USER_ENTERED'))

    # Обновлённая формула для "Время работы" (B4)
    working_time_formula = '''=ARRAYFORMULA(
        IF(
            ISBLANK(B3:ZZ3),
            "",
            MAP(B3:ZZ3,
            LAMBDA(date,
                IF(
                ISBLANK(date),
                "",
                IFERROR(
                    IF(
                    COUNTA(FILTER(
                        Data!D2:D,
                        (TRIM(Data!A2:A) = TRIM($A$1)) *
                        (LOWER(TRIM(Data!B2:B)) = LOWER(TRIM($B$2))) *
                        (TRIM(Data!G2:G) = TRIM($D$2)) *
                        (Data!C2:C = date)
                    )) = 0,
                    "н/д",
                    LET(
                        start_end,
                        JOIN(CHAR(10),
                        FILTER(
                            IF(LEN(TRIM(Data!D2:D)) = 0, "н/д", Data!D2:D) & "-" &
                            IF(LEN(TRIM(Data!E2:E)) = 0, "н/д", Data!E2:E),
                            (TRIM(Data!A2:A) = TRIM($A$1)) *
                            (LOWER(TRIM(Data!B2:B)) = LOWER(TRIM($B$2))) *
                            (TRIM(Data!G2:G) = TRIM($D$2)) *
                            (Data!C2:C = date)
                        )
                        ),
                        IF(
                        start_end = "н/д-н/д",
                        "н/д за день",
                        start_end
                        )
                    )
                    ),
                    "н/д"
                )
                )
            )
            )
        )
        )
        '''
    execute_with_retry(lambda: manager_sheet.update('B4', [[working_time_formula]], value_input_option='USER_ENTERED'))

    # Формула для "Лидов получено" (B5)
    leads_formula = '''=ARRAYFORMULA(
  IF(
    ISBLANK(B3:ZZ3),
    "",
    MAP(B3:ZZ3,
      LAMBDA(date,
        IF(
          ISBLANK(date),
          "",
          IFERROR(
            SUM(
              FILTER(
                Data!F2:F,
                (TRIM(Data!A2:A)=TRIM($A$1)) *
                (LOWER(TRIM(Data!B2:B))=LOWER(TRIM($B$2))) *
                (TRIM(Data!G2:G)=TRIM($D$2)) *
                (Data!C2:C=date)
              )
            ),
            0
          )
        )
      )
    )
  )
)'''
    execute_with_retry(lambda: manager_sheet.update('B5', [[leads_formula]], value_input_option='USER_ENTERED'))

    # Формула для "Лидов за месяц итого" (B6)
    total_leads_formula = '''=IFERROR(
  SUM(
    FILTER(Data!F2:F,
      (TRIM(Data!A2:A)=TRIM($A$1)) *
      (LOWER(TRIM(Data!B2:B))=LOWER(TRIM($B$2))) *
      (TRIM(Data!G2:G)=TRIM($D$2))
    )
  ),
  0
)'''
    execute_with_retry(lambda: manager_sheet.update('B6', [[total_leads_formula]], value_input_option='USER_ENTERED'))

    # Применяем форматирование
    apply_formatting(manager_sheet)

def apply_formatting(worksheet):
    sheet_id = worksheet._properties['sheetId']
    last_col_index = 26  # Настройте при необходимости

    requests = [
        # Установка ширины столбца A до 200 пикселей
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,  # Column A
                    "endIndex": 1     # До Column A
                },
                "properties": {
                    "pixelSize": 200
                },
                "fields": "pixelSize"
            }
        },
        # Установка ширины столбцов B и далее
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 1,  # Column B
                    "endIndex": last_col_index
                },
                "properties": {
                    "pixelSize": 100  # Настройте при необходимости
                },
                "fields": "pixelSize"
            }
        },
        # Форматирование ячейки A1 (Имя менеджера)
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red": 0.678,
                            "green": 0.847,
                            "blue": 0.902
                        },
                        "textFormat": {
                            "fontSize": 12,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat"
            }
        },
        # Форматирование меток в A2:A6
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,  # Row 2
                    "endRowIndex": 6,    # Row 6
                    "startColumnIndex": 0,
                    "endColumnIndex": 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red": 0.8,
                            "green": 1,
                            "blue": 0.8
                        },
                        "textFormat": {
                            "bold": True
                        },
                        "verticalAlignment": "MIDDLE"  # Вертикальное выравнивание по центру для A4 и остальных
                    }
                },
                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.verticalAlignment"
            }
        },
        # Центрирование с перенесом текста начиная с B2 для всех последующих ячеек
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,  # Row 2
                    "endRowIndex": 1000, # Настройте при необходимости
                    "startColumnIndex": 1,  # Column B
                    "endColumnIndex": last_col_index,
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "wrapStrategy": "WRAP"
                    }
                },
                "fields": "userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy"
            }
        }
    ]

    # Применение цветов фона к определённым строкам (3-6)
    row_colors = {
        '3': (0.851, 0.918, 0.827),  # Light green
        '4': (0.918, 0.82, 0.863),   # Light pink
        '5': (0.918, 0.82, 0.863),   # Light pink
        '6': (0.757, 0.482, 0.627),  # Dark pink
    }
    for row_num_str, color_tuple in row_colors.items():
        row_num = int(row_num_str) - 1  # Zero-indexed
        color = {'red': color_tuple[0], 'green': color_tuple[1], 'blue': color_tuple[2]}
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row_num,
                    "endRowIndex": row_num + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": last_col_index
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": color
                    }
                },
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

    # Применение границ к диапазону данных (A2 - ...)
    num_rows = 6
    requests.append({
        "updateBorders": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,  # Row 2
                "endRowIndex": num_rows,
                "startColumnIndex": 0,
                "endColumnIndex": last_col_index
            },
            "top": {"style": "SOLID"},
            "bottom": {"style": "SOLID"},
            "left": {"style": "SOLID"},
            "right": {"style": "SOLID"},
            "innerHorizontal": {"style": "SOLID"},
            "innerVertical": {"style": "SOLID"},
        }
    })

    # ========= Новые запросы для объединения и форматирования B2-C2 и D2-E2 ==========

    # Объединение ячеек B2-C2
    requests.append({
        "mergeCells": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,   # Row 2
                "endRowIndex": 2,     # Row 3
                "startColumnIndex": 1,# Column B
                "endColumnIndex": 3   # Column C
            },
            "mergeType": "MERGE_ALL"
        }
    })

    # Объединение ячеек D2-E2
    requests.append({
        "mergeCells": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,   # Row 2
                "endRowIndex": 2,     # Row 3
                "startColumnIndex": 3,# Column D
                "endColumnIndex": 5   # Column E
            },
            "mergeType": "MERGE_ALL"
        }
    })

    # Форматирование объединённой области B2-C2
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,   # Row 2
                "endRowIndex": 2,     # Row 3
                "startColumnIndex": 1,# B
                "endColumnIndex": 3   # C
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {
                        "red": 0.8,
                        "green": 1,
                        "blue": 0.8
                    },
                    "textFormat": {
                        "fontSize": 12,
                        "bold": True
                    }
                }
            },
            "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat"
        }
    })

    # Форматирование объединённой области D2-E2
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,   # Row 2
                "endRowIndex": 2,     # Row 3
                "startColumnIndex": 3,# D
                "endColumnIndex": 5   # E
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {
                        "red": 0.8,
                        "green": 1,
                        "blue": 0.8
                    },
                    "textFormat": {
                        "fontSize": 12,
                        "bold": True
                    }
                }
            },
            "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat"
        }
    })
    # ==========================================================================

    # Выполнение всех запросов форматирования за один раз
    execute_with_retry(lambda: worksheet.spreadsheet.batch_update({'requests': requests}))

    # Замораживание первых двух строк и первого столбца
    worksheet.freeze(rows=2, cols=1)


def update_main_sheet(manager_names, all_months, all_years):
    client = authorize_google_sheets()
    spreadsheet = client.open(GOOGLE_SHEET)

    # Создаём или выбираем лист 'Основная страница'
    try:
        main_sheet = spreadsheet.worksheet('Основная страница')
    except gspread.exceptions.WorksheetNotFound:
        print("Worksheet 'Основная страница' not found. Creating new worksheet.")
        def add_worksheet():
            spreadsheet.add_worksheet(title='Основная страница', rows="1000", cols="10")
        execute_with_retry(add_worksheet, retries=5, initial_delay=60)
        main_sheet = spreadsheet.worksheet('Основная страница')

    # Очищаем основной лист
    execute_with_retry(lambda: main_sheet.clear())

    # Устанавливаем заголовок в A1
    execute_with_retry(lambda: main_sheet.update('A1', [['Общая информация']]))

    # Определяем текущий месяц и год
    current_datetime = datetime.now()
    current_month_en = current_datetime.strftime('%B')
    current_year = str(current_datetime.year)
    current_month_ru = MONTHS_EN_TO_RU.get(current_month_en, current_month_en)

    # Определяем месяцы и годы для выпадающих списков
    months_list = sorted(all_months, key=lambda m: MONTHS_RU_ORDER.get(m, 0))
    years_list = sorted(all_years)

    # Определяем значения по умолчанию
    if current_month_ru in months_list:
        default_month = current_month_ru
    else:
        default_month = months_list[-1] if months_list else ''

    if current_year in years_list:
        default_year = current_year
    else:
        default_year = years_list[-1] if years_list else ''

    # Устанавливаем значения по умолчанию в B1 и B2
    execute_with_retry(lambda: main_sheet.update('B1', [[default_month]]))
    execute_with_retry(lambda: main_sheet.update('B2', [[default_year]]))


    # Объединяем ячейки A1:A2 и C1:C2
    sheet_id = main_sheet._properties['sheetId']
    merge_requests = [
        {
            "mergeCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,  # Row 1
                    "endRowIndex": 2,    # Row 2
                    "startColumnIndex": 0,  # Column A
                    "endColumnIndex": 1    # Column A
                },
                "mergeType": "MERGE_ALL"
            }
        },
        {
            "mergeCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,  # Row 1
                    "endRowIndex": 2,    # Row 2
                    "startColumnIndex": 2,  # Column C
                    "endColumnIndex": 3    # Column C
                },
                "mergeType": "MERGE_ALL"
            }
        }
    ]
    execute_with_retry(lambda: spreadsheet.batch_update({'requests': merge_requests}))
    
    # Устанавливаем текст "За всё время" в C2
    execute_with_retry(lambda: main_sheet.update('C1', [['За всё время']]))
    
    # Устанавливаем проверку данных в B1 и B2
    validation_requests = [
        {
            'setDataValidation': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,    # Row 1
                    'endRowIndex': 1,      # Row 2
                    'startColumnIndex': 1, # Column B
                    'endColumnIndex': 2    # Column B
                },
                'rule': {
                    'condition': {
                        'type': 'ONE_OF_LIST',
                        'values': [{'userEnteredValue': month} for month in months_list]
                    },
                    'showCustomUi': True
                }
            }
        },
        {
            'setDataValidation': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,    # Row 2
                    'endRowIndex': 2,      # Row 3
                    'startColumnIndex': 1, # Column B
                    'endColumnIndex': 2    # Column B
                },
                'rule': {
                    'condition': {
                        'type': 'ONE_OF_LIST',
                        'values': [{'userEnteredValue': str(year)} for year in years_list]
                    },
                    'showCustomUi': True
                }
            }
        }
    ]
    execute_with_retry(lambda: spreadsheet.batch_update({'requests': validation_requests}))

    # Записываем имена менеджеров в столбец A начиная с A3
    data = [[manager_name] for manager_name in manager_names]
    print(f"Updating range A3 with data: {data}")
    execute_with_retry(lambda: main_sheet.update('A3', data))

    # Устанавливаем формулы в столбец B для подсчёта лидов за выбранный месяц и год
    num_rows = len(manager_names) + 2  # Включая заголовки
    formulas_b = []
    for idx in range(len(manager_names)):
        row = idx + 3  # Начиная с строки 3
        formula = f"=IFERROR(SUM(FILTER(Data!F:F, (Data!A:A=A{row})*(Data!B:B=B$1)*(Data!G:G=B$2))), 0)"
        formulas_b.append([formula])

    execute_with_retry(lambda: main_sheet.update('B3', formulas_b, value_input_option='USER_ENTERED'))

    # Устанавливаем формулы в столбец C для подсчёта лидов за всё время
    formulas_c = []
    for idx in range(len(manager_names)):
        row = idx + 3  # Начиная с строки 3
        formula = f"=IFERROR(SUMIF(Data!A:A, A{row}, Data!F:F), 0)"
        formulas_c.append([formula])

    execute_with_retry(lambda: main_sheet.update('C3', formulas_c, value_input_option='USER_ENTERED'))

    # Применяем форматирование
    apply_main_sheet_formatting(main_sheet, num_rows)

def apply_main_sheet_formatting(main_sheet, num_rows):
    sheet_id = main_sheet._properties['sheetId']

    # Устанавливаем ширину столбцов
    requests = [
        # Ширина столбца A
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,  # Column A
                    "endIndex": 1     # До Column A
                },
                "properties": {
                    "pixelSize": 200
                },
                "fields": "pixelSize"
            }
        },
        # Ширина столбца B и C
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 1,  # Column B
                    "endIndex": 3     # До Column C
                },
                "properties": {
                    "pixelSize": 150
                },
                "fields": "pixelSize"
            }
        },
    ]

    # Выравнивание столбцов
    # Столбец A - по левому краю
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": num_rows,
                "startColumnIndex": 0,
                "endColumnIndex": 1
            },
            "cell": {
                "userEnteredFormat": {
                    "horizontalAlignment": "LEFT"
                }
            },
            "fields": "userEnteredFormat.horizontalAlignment"
        }
    })

    # Столбцы B и C - по центру
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": num_rows,
                "startColumnIndex": 1,
                "endColumnIndex": 3
            },
            "cell": {
                "userEnteredFormat": {
                    "horizontalAlignment": "CENTER"
                }
            },
            "fields": "userEnteredFormat.horizontalAlignment"
        }
    })

    # Установка шрифта
    # Строки 1 и 2 - размер 12, жирный
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 2,
                "startColumnIndex": 0,
                "endColumnIndex": 3
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "fontSize": 12,
                        "bold": True
                    }
                }
            },
            "fields": "userEnteredFormat.textFormat"
        }
    })

    # Строки 3 и ниже - размер 11
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 2,
                "endRowIndex": num_rows,
                "startColumnIndex": 0,
                "endColumnIndex": 3
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "fontSize": 11
                    }
                }
            },
            "fields": "userEnteredFormat.textFormat"
        }
    })

    # Установка границ для диапазона A1:C{num_rows}
    requests.append({
        "updateBorders": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": num_rows,
                "startColumnIndex": 0,
                "endColumnIndex": 3
            },
            "top": {"style": "SOLID"},
            "bottom": {"style": "SOLID"},
            "left": {"style": "SOLID"},
            "right": {"style": "SOLID"},
            "innerHorizontal": {"style": "SOLID"},
            "innerVertical": {"style": "SOLID"},
        }
    })

    # Заливка столбца B - light green 3
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": num_rows,
                "startColumnIndex": 1,  # Column B
                "endColumnIndex": 2
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {
                        "red": 0.8,
                        "green": 1,
                        "blue": 0.8
                    }
                }
            },
            "fields": "userEnteredFormat.backgroundColor"
        }
    })

    # Заливка столбца C - "#D9EAD2"
    rgb = (217/255, 234/255, 210/255)  # Преобразование HEX в RGB
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": num_rows,
                "startColumnIndex": 2,  # Column C
                "endColumnIndex": 3
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {
                        "red": rgb[0],
                        "green": rgb[1],
                        "blue": rgb[2]
                    }
                }
            },
            "fields": "userEnteredFormat.backgroundColor"
        }
    })

    # Вертикальное выравнивание по центру для объединённых ячеек A1:A2 и C1:C2
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 2,
                "startColumnIndex": 0,  # Column A
                "endColumnIndex": 1
            },
            "cell": {
                "userEnteredFormat": {
                    "verticalAlignment": "MIDDLE"
                }
            },
            "fields": "userEnteredFormat.verticalAlignment"
        }
    })
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 2,
                "startColumnIndex": 2,  # Column C
                "endColumnIndex": 3
            },
            "cell": {
                "userEnteredFormat": {
                    "verticalAlignment": "MIDDLE"
                }
            },
            "fields": "userEnteredFormat.verticalAlignment"
        }
    })

    # Выполнение всех запросов форматирования за один раз
    execute_with_retry(lambda: main_sheet.spreadsheet.batch_update({'requests': requests}))

    # Замораживание первых двух строк
    main_sheet.freeze(rows=2)


async def update_user_data():
    print('Было запущено обновление даных страниц.')
    user_ids = session.query(user_info_table.c.user_id).distinct().all()
    # Собираем все данные и записываем в скрытый лист
    all_data = []
    manager_months = {}  # Для отслеживания месяцев для каждого менеджера
    manager_years = {}   # Для отслеживания годов для каждого менеджера
    manager_names = []

    for user_id_tuple in user_ids:
        user_id = user_id_tuple[0]
        real_name = get_user_name(user_id)
        if real_name:
            user_data = fetch_user_data(user_id)
            data = format_data_for_sheet(user_data)
            if data:
                for row in data:
                    all_data.append([real_name] + row)
                # Собираем уникальные месяцы и годы для менеджера
                months = [row[0].strip() for row in data]  # row[0] - месяц на русском
                unique_months = sorted(set(months), key=lambda m: MONTHS_RU_ORDER.get(m, 0))
                manager_months[real_name] = unique_months

                years = [row[1].split('/')[-1] for row in data]  # Извлекаем год из даты
                unique_years = sorted(set(years))
                manager_years[real_name] = unique_years

                manager_names.append(real_name)

    # Обновляем скрытый лист с данными всех пользователей
    update_hidden_data_sheet(all_data)
    print('Было закончено обновление даных страниц.')

async def main():
    user_ids = session.query(user_info_table.c.user_id).distinct().all()

    # Собираем все данные и записываем в скрытый лист
    all_data = []
    manager_months = {}  # Для отслеживания месяцев для каждого менеджера
    manager_years = {}   # Для отслеживания годов для каждого менеджера
    manager_names = []

    for user_id_tuple in user_ids:
        user_id = user_id_tuple[0]
        real_name = get_user_name(user_id)
        if real_name:
            user_data = fetch_user_data(user_id)
            data = format_data_for_sheet(user_data)
            if data:
                for row in data:
                    all_data.append([real_name] + row)
                # Собираем уникальные месяцы и годы для менеджера
                months = [row[0].strip() for row in data]  # row[0] - месяц на русском
                unique_months = sorted(set(months), key=lambda m: MONTHS_RU_ORDER.get(m, 0))
                manager_months[real_name] = unique_months

                years = [row[1].split('/')[-1] for row in data]  # Извлекаем год из даты
                unique_years = sorted(set(years))
                manager_years[real_name] = unique_years

                manager_names.append(real_name)

    # Обновляем скрытый лист с данными всех пользователей
    update_hidden_data_sheet(all_data)

    # Собираем все месяцы и годы для основного листа
    all_months = set()
    all_years = set()
    for months in manager_months.values():
        all_months.update(months)
    for years in manager_years.values():
        all_years.update(years)

    # Обновляем основной лист
    update_main_sheet(manager_names, all_months, all_years)

    # Обновляем листы каждого менеджера
    for manager_name in manager_names:
        months = manager_months.get(manager_name, [])
        years = manager_years.get(manager_name, [])
        update_manager_sheet(manager_name, months, years)
        # Добавляем задержку между обновлениями листов менеджеров
        time.sleep(1)  # Настройте при необходимости

if __name__ == "__main__":
    asyncio.run(main())
