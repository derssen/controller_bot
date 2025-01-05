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

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()
metadata.reflect(bind=engine)

names_table = metadata.tables['names']
user_info_table = metadata.tables['user_info']

def authorize_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_FILE, scope)
    client = gspread.authorize(creds)
    return client

def fetch_user_data(user_id):
    rows = session.execute(
        select(user_info_table).where(user_info_table.c.user_id == user_id)
    ).fetchall()
    return rows

def get_user_name(user_id):
    row = session.execute(
        select(names_table).where(names_table.c.real_user_id == user_id)
    ).fetchone()
    if row:
        # Предполагаем, что real_name в row[1]
        return row[1]
    return None

def get_user_rank(user_id):
    row = session.execute(
        select(names_table).where(names_table.c.real_user_id == user_id)
    ).fetchone()
    if row:
        # Предполагаем rank в row[5]
        return row[5]
    return None

def format_data_for_sheet(user_data):
    formatted = []
    for record in user_data:
        # user_info: (id, user_id, date, start_time, end_time, leads, has_photo, started)
        date_obj = record[2]
        start_time = record[3]
        end_time = record[4]
        leads = record[5]
        has_photo = record[6]

        date_str = date_obj.strftime('%d/%m/%Y')
        month_str_en = date_obj.strftime('%B')
        month_str_ru = MONTHS_EN_TO_RU.get(month_str_en, month_str_en)

        start_str = start_time.strftime('%H:%M') if start_time else ''
        end_str = end_time.strftime('%H:%M') if end_time else ''
        photo_str = '+' if has_photo == 1 else '-'

        formatted.append([
            month_str_ru,
            date_str,
            start_str,
            end_str,
            leads,
            photo_str
        ])
    return formatted

def execute_with_retry(func, retries=5, initial_delay=60, delay_on_quota=True):
    import gspread
    import time
    for attempt in range(retries):
        try:
            func()
            break
        except gspread.exceptions.APIError as e:
            status = e.response.status_code
            if status == 429:
                print(f"Quota exceeded. Waiting for {initial_delay} seconds before retrying...")
                time.sleep(initial_delay)
                if delay_on_quota:
                    initial_delay *= 2
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
    try:
        data_sheet = spreadsheet.worksheet('Data')
    except gspread.exceptions.WorksheetNotFound:
        data_sheet = spreadsheet.add_worksheet(title='Data', rows="1000", cols="10")
        data_sheet.hide()

    headers = ['UserName', 'Month', 'Date', 'Start Time', 'End Time', 'Leads', 'Year', 'Photo']
    execute_with_retry(lambda: data_sheet.clear())

    data_for_sheet = [headers]
    for row in all_data:
        # row = [real_name, month_ru, date_str, start_time, end_time, leads, photo]
        real_name = row[0]
        month_str_ru = row[1]
        date_str = row[2]
        st_time = row[3]
        e_time = row[4]
        leads = row[5]
        photo = row[6]

        date_obj = datetime.strptime(date_str, '%d/%m/%Y')
        year = date_obj.year
        data_for_sheet.append([real_name, month_str_ru, date_str, st_time, e_time, leads, str(year), photo])

    execute_with_retry(lambda: data_sheet.update(data_for_sheet))

def apply_formatting(worksheet):
    sheet_id = worksheet._properties['sheetId']
    last_col_index = 26

    requests = [
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": 1
                },
                "properties": {
                    "pixelSize": 200
                },
                "fields": "pixelSize"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 1,
                    "endIndex": last_col_index
                },
                "properties": {
                    "pixelSize": 100
                },
                "fields": "pixelSize"
            }
        }
    ]
    client = authorize_google_sheets()
    execute_with_retry(lambda: worksheet.spreadsheet.batch_update({'requests': requests}))
    worksheet.freeze(rows=2, cols=1)

def update_manager_sheet(manager_name, months, years):
    client = authorize_google_sheets()
    spreadsheet = client.open(GOOGLE_SHEET)
    try:
        manager_sheet = spreadsheet.worksheet(manager_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{manager_name}' not found. Creating new one.")
        def add_worksheet():
            spreadsheet.add_worksheet(title=manager_name, rows="1000", cols="26")
        execute_with_retry(add_worksheet, retries=5, initial_delay=60)
        manager_sheet = spreadsheet.worksheet(manager_name)

    execute_with_retry(lambda: manager_sheet.clear())
    execute_with_retry(lambda: manager_sheet.update('A1', [[manager_name]]))

    current_datetime = datetime.now()
    current_month_en = current_datetime.strftime('%B')
    current_year = str(current_datetime.year)
    current_month_ru = MONTHS_EN_TO_RU.get(current_month_en, current_month_en)
    months_lower = [m.lower() for m in months]

    if current_month_ru.lower() in months_lower:
        default_month = current_month_ru.capitalize()
    else:
        default_month = months[-1].capitalize() if months else ''

    if current_year in years:
        default_year = current_year
    else:
        default_year = years[-1] if years else ''

    execute_with_retry(lambda: manager_sheet.update('B2', [[default_month]]))
    execute_with_retry(lambda: manager_sheet.update('D2', [[default_year]]))

    sheet_id = manager_sheet._properties['sheetId']
    months_capitalized = [m.capitalize() for m in months]
    requests = [
        {
            'setDataValidation': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,
                    'endRowIndex': 2,
                    'startColumnIndex': 1,
                    'endColumnIndex': 2
                },
                'rule': {
                    'condition': {
                        'type': 'ONE_OF_LIST',
                        'values': [{'userEnteredValue': m} for m in months_capitalized]
                    },
                    'showCustomUi': True
                }
            }
        },
        {
            'setDataValidation': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,
                    'endRowIndex': 2,
                    'startColumnIndex': 3,
                    'endColumnIndex': 4
                },
                'rule': {
                    'condition': {
                        'type': 'ONE_OF_LIST',
                        'values': [{'userEnteredValue': str(y)} for y in years]
                    },
                    'showCustomUi': True
                }
            }
        }
    ]
    body = {'requests': requests}
    execute_with_retry(lambda: spreadsheet.batch_update(body))

    labels = [['Месяц'], ['Дата'], ['Время работы'], ['Лидов получено'], ['Лидов за месяц итого']]
    execute_with_retry(lambda: manager_sheet.update('A2:A6', labels))

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

    total_leads_formula = '''=IFERROR(
SUM(
FILTER(
Data!F2:F,
(TRIM(Data!A2:A)=TRIM($A$1)) *
(LOWER(TRIM(Data!B2:B))=LOWER(TRIM($B$2))) *
(TRIM(Data!G2:G)=TRIM($D$2))
)
),
0
)'''
    execute_with_retry(lambda: manager_sheet.update('B6', [[total_leads_formula]], value_input_option='USER_ENTERED'))

    apply_formatting(manager_sheet)

def update_validators_sheet(validators_data):
    sheet_title = 'Валидаторы'
    client = authorize_google_sheets()
    spreadsheet = client.open(GOOGLE_SHEET)
    try:
        val_sheet = spreadsheet.worksheet(sheet_title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{sheet_title}' not found. Creating new.")
        def add_worksheet():
            spreadsheet.add_worksheet(title=sheet_title, rows="1000", cols="26")
        execute_with_retry(add_worksheet, retries=5, initial_delay=60)
        val_sheet = spreadsheet.worksheet(sheet_title)

    execute_with_retry(lambda: val_sheet.clear())
    execute_with_retry(lambda: val_sheet.update('A1', [['Валидаторы']]))

    # Уникальные валидаторы
    validator_names = sorted(set([row[0] for row in validators_data]))
    # Собираем все месяцы и годы для валидаторов
    months_all = []
    years_all = []
    for row in validators_data:
        # row: [real_name, month_ru, date, start, end, leads, photo]
        months_all.append(row[1].strip())
        date_str = row[2]
        y = date_str.split('/')[-1]
        years_all.append(y)
    unique_months = sorted(set(months_all), key=lambda m: MONTHS_RU_ORDER.get(m, 0))
    unique_years = sorted(set(years_all))

    # Определим значения по умолчанию (месяц и год)
    current_datetime = datetime.now()
    current_month_en = current_datetime.strftime('%B')
    current_year = str(current_datetime.year)
    current_month_ru = MONTHS_EN_TO_RU.get(current_month_en, current_month_en)

    if current_month_ru in unique_months:
        default_month = current_month_ru
    else:
        default_month = unique_months[-1] if unique_months else ''
    if current_year in unique_years:
        default_year = current_year
    else:
        default_year = unique_years[-1] if unique_years else ''

    # A2 - выпадающий список валидаторов
    sheet_id = val_sheet._properties['sheetId']
    validator_validation = {
        'setDataValidation': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 1,
                'endRowIndex': 2,
                'startColumnIndex': 0,
                'endColumnIndex': 1
            },
            'rule': {
                'condition': {
                    'type': 'ONE_OF_LIST',
                    'values': [{'userEnteredValue': name} for name in validator_names]
                },
                'showCustomUi': True
            }
        }
    }

    # B2 - выпадающий список месяцев
    month_validation = {
        'setDataValidation': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 1,
                'endRowIndex': 2,
                'startColumnIndex': 1, # B
                'endColumnIndex': 2
            },
            'rule': {
                'condition': {
                    'type': 'ONE_OF_LIST',
                    'values': [{'userEnteredValue': m} for m in unique_months]
                },
                'showCustomUi': True
            }
        }
    }

    # D2 - выпадающий список годов
    year_validation = {
        'setDataValidation': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 1,
                'endRowIndex': 2,
                'startColumnIndex': 3, # D
                'endColumnIndex': 4
            },
            'rule': {
                'condition': {
                    'type': 'ONE_OF_LIST',
                    'values': [{'userEnteredValue': str(y)} for y in unique_years]
                },
                'showCustomUi': True
            }
        }
    }

    body = {'requests': [validator_validation, month_validation, year_validation]}
    execute_with_retry(lambda: spreadsheet.batch_update(body))

    # Устанавливаем значения по умолчанию:
    # Первый валидатор
    if validator_names:
        execute_with_retry(lambda: val_sheet.update('A2', [[validator_names[0]]]))
    # Месяц
    if default_month:
        execute_with_retry(lambda: val_sheet.update('B2', [[default_month]]))
    # Год
    if default_year:
        execute_with_retry(lambda: val_sheet.update('D2', [[default_year]]))

    # Подписи:
    # A3: "Дата"
    # A4: "Время работы"
    # A5: "Отчет"
    execute_with_retry(lambda: val_sheet.update('A3', [['Дата']]))
    execute_with_retry(lambda: val_sheet.update('A4', [['Время работы']]))
    execute_with_retry(lambda: val_sheet.update('A5', [['Отчет']]))

    # Формулы по аналогии с менеджерами:
    # Дата (B3): даты, соответствующие валидатору (A2), месяцу (B2), году (D2)
    date_formula = '''=IFERROR(
  TRANSPOSE(
    UNIQUE(
      FILTER(Data!C2:C,
        (TRIM(Data!A2:A)=TRIM($A$2)) *
        (LOWER(TRIM(Data!B2:B))=LOWER(TRIM($B$2))) *
        (TRIM(Data!G2:G)=TRIM($D$2))
      )
    )
  ),
  "Нет данных"
)'''
    execute_with_retry(lambda: val_sheet.update('B3', [[date_formula]], value_input_option='USER_ENTERED'))

    # Время работы (B4) - по аналогии с менеджерами, только A$2 вместо A$1
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
COUNTA(
FILTER(
Data!D2:D,
(TRIM(Data!A2:A)=TRIM($A$2)) *
(LOWER(TRIM(Data!B2:B))=LOWER(TRIM($B$2))) *
(TRIM(Data!G2:G)=TRIM($D$2)) *
(Data!C2:C=date)
)
)=0,
"н/д",
JOIN(CHAR(10),
FILTER(
IF(LEN(TRIM(Data!D2:D))=0,"н/д",Data!D2:D)&"-"&IF(LEN(TRIM(Data!E2:E))=0,"н/д",Data!E2:E),
(TRIM(Data!A2:A)=TRIM($A$2)) *
(LOWER(TRIM(Data!B2:B))=LOWER(TRIM($B$2))) *
(TRIM(Data!G2:G)=TRIM($D$2)) *
(Data!C2:C=date)
)
)
),
"н/д"
)
)
)
)
)
'''
    execute_with_retry(lambda: val_sheet.update('B4', [[working_time_formula]], value_input_option='USER_ENTERED'))

    # Отчет (B5): нужно вывести '+' если хотя бы в одной записи есть '+', иначе '-'
    # Можно использовать MAX() или COUNTIF. Если фото хотя бы раз '+', показать '+'
    # Так как Photo в колонке H?
    # Photo в Data - это 8-й столбец (H)
    # Проверим наличие '+' в колонке H для данного валидатора/месяца/года/даты
    # Если нет дат - пусто, если есть - если хотя бы один '+'

    report_formula = '''=ARRAYFORMULA(
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
COUNTIF(
FILTER(
Data!H2:H,
(TRIM(Data!A2:A)=TRIM($A$2)) *
(LOWER(TRIM(Data!B2:B))=LOWER(TRIM($B$2))) *
(TRIM(Data!G2:G)=TRIM($D$2)) *
(Data!C2:C=date)
),"+")>0,
"+","-"
),
"-"
)
)
)
)
)'''
    execute_with_retry(lambda: val_sheet.update('B5', [[report_formula]], value_input_option='USER_ENTERED'))

    apply_formatting(val_sheet)

def update_main_sheet(manager_names, all_months, all_years):
    client = authorize_google_sheets()
    spreadsheet = client.open(GOOGLE_SHEET)
    try:
        main_sheet = spreadsheet.worksheet('Основная страница')
    except gspread.exceptions.WorksheetNotFound:
        print("Worksheet 'Основная страница' not found. Creating new.")
        def add_worksheet():
            spreadsheet.add_worksheet(title='Основная страница', rows="1000", cols="10")
        execute_with_retry(add_worksheet, retries=5, initial_delay=60)
        main_sheet = spreadsheet.worksheet('Основная страница')

    execute_with_retry(lambda: main_sheet.clear())
    execute_with_retry(lambda: main_sheet.update('A1', [['Общая информация']]))

    current_datetime = datetime.now()
    current_month_en = current_datetime.strftime('%B')
    current_year = str(current_datetime.year)
    current_month_ru = MONTHS_EN_TO_RU.get(current_month_en, current_month_en)

    months_list = sorted(all_months, key=lambda m: MONTHS_RU_ORDER.get(m, 0))
    years_list = sorted(all_years)

    if current_month_ru in months_list:
        default_month = current_month_ru
    else:
        default_month = months_list[-1] if months_list else ''

    if current_year in years_list:
        default_year = current_year
    else:
        default_year = years_list[-1] if years_list else ''

    execute_with_retry(lambda: main_sheet.update('B1', [[default_month]]))
    execute_with_retry(lambda: main_sheet.update('B2', [[default_year]]))

    sheet_id = main_sheet._properties['sheetId']
    merge_requests = [
        {
            "mergeCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 2,
                    "startColumnIndex": 0,
                    "endColumnIndex": 1
                },
                "mergeType": "MERGE_ALL"
            }
        },
        {
            "mergeCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 2,
                    "startColumnIndex": 2,
                    "endColumnIndex": 3
                },
                "mergeType": "MERGE_ALL"
            }
        }
    ]
    execute_with_retry(lambda: spreadsheet.batch_update({'requests': merge_requests}))
    execute_with_retry(lambda: main_sheet.update('C1', [['За всё время']]))

    validation_requests = [
        {
            'setDataValidation': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': 1,
                    'endColumnIndex': 2
                },
                'rule': {
                    'condition': {
                        'type': 'ONE_OF_LIST',
                        'values': [{'userEnteredValue': m} for m in months_list]
                    },
                    'showCustomUi': True
                }
            }
        },
        {
            'setDataValidation': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,
                    'endRowIndex': 2,
                    'startColumnIndex': 1,
                    'endColumnIndex': 2
                },
                'rule': {
                    'condition': {
                        'type': 'ONE_OF_LIST',
                        'values': [{'userEnteredValue': str(y)} for y in years_list]
                    },
                    'showCustomUi': True
                }
            }
        }
    ]
    execute_with_retry(lambda: spreadsheet.batch_update({'requests': validation_requests}))

    data = [[manager_name] for manager_name in manager_names]
    print(f"Updating range A3 with data: {data}")
    execute_with_retry(lambda: main_sheet.update('A3', data))

    num_rows = len(manager_names) + 2
    formulas_b = []
    for idx in range(len(manager_names)):
        row = idx + 3
        formula = f"=IFERROR(SUM(FILTER(Data!F:F,(Data!A:A=A{row})*(Data!B:B=B$1)*(Data!G:G=B$2))),0)"
        formulas_b.append([formula])
    execute_with_retry(lambda: main_sheet.update('B3', formulas_b, value_input_option='USER_ENTERED'))

    formulas_c = []
    for idx in range(len(manager_names)):
        row = idx + 3
        formula = f"=IFERROR(SUMIF(Data!A:A, A{row}, Data!F:F),0)"
        formulas_c.append([formula])
    execute_with_retry(lambda: main_sheet.update('C3', formulas_c, value_input_option='USER_ENTERED'))

    apply_main_sheet_formatting(main_sheet, num_rows)

def apply_main_sheet_formatting(main_sheet, num_rows):
    sheet_id = main_sheet._properties['sheetId']
    requests = [
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": 1
                },
                "properties": {
                    "pixelSize": 200
                },
                "fields": "pixelSize"
            }
        },
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 1,
                    "endIndex": 3
                },
                "properties": {
                    "pixelSize": 150
                },
                "fields": "pixelSize"
            }
        },
        {
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
        },
        {
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
        },
        {
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
        },
        {
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
        },
        {
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
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": num_rows,
                    "startColumnIndex": 1,
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
        }
    ]

    rgb = (217/255, 234/255, 210/255)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": num_rows,
                "startColumnIndex": 2,
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
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 2,
                "startColumnIndex": 0,
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
                "startColumnIndex": 2,
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

    client = authorize_google_sheets()
    execute_with_retry(lambda: main_sheet.spreadsheet.batch_update({'requests': requests}))
    main_sheet.freeze(rows=2)

async def update_all_data():
    from sqlalchemy.sql import func
    user_ids = session.query(user_info_table.c.user_id).distinct().all()

    all_data = []
    manager_names = []
    manager_months = {}
    manager_years = {}
    validator_data = []

    for (u_id,) in user_ids:
        real_name = get_user_name(u_id)
        rank = get_user_rank(u_id)
        if not real_name or rank is None:
            continue
        user_data = fetch_user_data(u_id)
        formatted = format_data_for_sheet(user_data)
        for fd in formatted:
            all_data.append([real_name] + fd)

        if rank == 1:
            months = [r[0] for r in formatted]
            unique_months = sorted(set(months), key=lambda m: MONTHS_RU_ORDER.get(m, 0))
            years_list = [r[1].split('/')[-1] for r in formatted]
            unique_years = sorted(set(years_list))
            manager_names.append(real_name)
            manager_months[real_name] = unique_months
            manager_years[real_name] = unique_years
        elif rank == 2:
            for fd in formatted:
                validator_data.append([real_name] + fd)
        # rank=3 (РОП) игнорируем

    update_hidden_data_sheet(all_data)

    # Основная страница
    all_months = set()
    all_years = set()
    for mm in manager_months.values():
        all_months.update(mm)
    for yv in manager_years.values():
        all_years.update(yv)
    update_main_sheet(manager_names, all_months, all_years)

    # Валидаторы
    update_validators_sheet(validator_data)

    # Страницы менеджеров
    for real_name in manager_names:
        m_list = manager_months.get(real_name, [])
        y_list = manager_years.get(real_name, [])
        update_manager_sheet(real_name, m_list, y_list)
        time.sleep(1)

    print("Обновление Google Sheet завершено.")


async def update_user_data():
    """
    Обновляем лист Data (скрытый), «Основная страница» (только для менеджеров rank=1),
    индивидуальные листы менеджеров, и общий лист «Валидаторы» (rank=2).
    """
    print("Запущено обновление данных.")
    from sqlalchemy.sql import func
    user_ids = session.query(user_info_table.c.user_id).distinct().all()

    all_data = []
    manager_names = []
    manager_months = {}
    manager_years = {}
    validator_data = []

    for (u_id,) in user_ids:
        real_name = get_user_name(u_id)
        rank = get_user_rank(u_id)
        if not real_name or rank is None:
            continue
        user_data = fetch_user_data(u_id)
        formatted = format_data_for_sheet(user_data)
        # Для Data:
        for fd in formatted:
            # fd = [month_ru, date_str, start_str, end_str, leads, photo]
            all_data.append([real_name] + fd)

        # Сортируем по рангу
        if rank == 1:
            # Менеджер
            months = [r[0] for r in formatted]  # r[0] = month_ru
            unique_months = sorted(set(months), key=lambda m: MONTHS_RU_ORDER.get(m, 0))
            years_list = [r[1].split('/')[-1] for r in formatted]
            unique_years = sorted(set(years_list))
            manager_names.append(real_name)
            manager_months[real_name] = unique_months
            manager_years[real_name] = unique_years
        elif rank == 2:
            # Валидатор
            for fd in formatted:
                validator_data.append([real_name] + fd)
        # rank=3 (РОП) не отображаем

    # 1) Обновляем скрытый лист Data
    update_hidden_data_sheet(all_data)


    print("Обновление Google Sheet завершено.")

async def update_all_data():
    """
    Обновляем лист Data (скрытый), «Основная страница» (только для менеджеров rank=1),
    индивидуальные листы менеджеров, и общий лист «Валидаторы» (rank=2).
    """
    print("Запущено обновление данных.")
    from sqlalchemy.sql import func
    user_ids = session.query(user_info_table.c.user_id).distinct().all()

    all_data = []
    manager_names = []
    manager_months = {}
    manager_years = {}
    validator_data = []

    for (u_id,) in user_ids:
        real_name = get_user_name(u_id)
        rank = get_user_rank(u_id)
        if not real_name or rank is None:
            continue
        user_data = fetch_user_data(u_id)
        formatted = format_data_for_sheet(user_data)
        # Для Data:
        for fd in formatted:
            # fd = [month_ru, date_str, start_str, end_str, leads, photo]
            all_data.append([real_name] + fd)

        # Сортируем по рангу
        if rank == 1:
            # Менеджер
            months = [r[0] for r in formatted]  # r[0] = month_ru
            unique_months = sorted(set(months), key=lambda m: MONTHS_RU_ORDER.get(m, 0))
            years_list = [r[1].split('/')[-1] for r in formatted]
            unique_years = sorted(set(years_list))
            manager_names.append(real_name)
            manager_months[real_name] = unique_months
            manager_years[real_name] = unique_years
        elif rank == 2:
            # Валидатор
            for fd in formatted:
                validator_data.append([real_name] + fd)
        # rank=3 (РОП) не отображаем

    # 1) Обновляем скрытый лист Data
    update_hidden_data_sheet(all_data)

    # 2) Основная страница (только менеджеры)
    all_months = set()
    all_years = set()
    for mm in manager_months.values():
        all_months.update(mm)
    for yv in manager_years.values():
        all_years.update(yv)
    update_main_sheet(manager_names, all_months, all_years)

    # 4) Общая страница «Валидаторы»
    update_validators_sheet(validator_data)

    # 3) Страницы менеджеров
    for real_name in manager_names:
        m_list = manager_months.get(real_name, [])
        y_list = manager_years.get(real_name, [])
        update_manager_sheet(real_name, m_list, y_list)
        time.sleep(1)

    print("Обновление Google Sheet завершено.")

async def main():
    await update_all_data()

if __name__ == "__main__":
    asyncio.run(main())
