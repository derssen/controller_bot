import gspread
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor

from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, DateTime, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from datetime import datetime
from gspread_formatting import (
    format_cell_range, CellFormat, TextFormat, Color, Borders, Border
)
from config import JSON_FILE, GOOGLE_SHEET, MONTHS_EN_TO_RU, DATABASE_URL, MONTHS_RU_ORDER

executor = ThreadPoolExecutor()

# Настройка подключения к базе данных
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()

# Определяем таблицы вручную
names_table = Table('names', metadata,
    Column('real_user_id', Integer, nullable=False),
    Column('real_name', String, nullable=False),
    Column('username', String),
    Column('group_id', Integer, nullable=False, default=1),
    Column('amocrm_id', Integer, nullable=False, default=1),
    Column('language', String, nullable=False, default='ru'),
    Column('rank', Integer, nullable=False, default=2),
    Column('rop_username', String, nullable=False, default='katetym4enko'),
)

user_info_table = Table('user_info', metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer),
    Column('date', DateTime),
    Column('start_time', DateTime),
    Column('end_time', DateTime),
    Column('leads', Integer),
    Column('has_photo', Integer, default=0),
    Column('started', Boolean, default=False)
)

def authorize_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_FILE, scope)
    client = gspread.authorize(creds)
    return client

def fetch_user_data(user_id):
    # Используем mappings() для получения словарей, а не кортежей
    query = select(
        user_info_table.c.date,
        user_info_table.c.start_time,
        user_info_table.c.end_time,
        user_info_table.c.leads
    ).where(user_info_table.c.user_id == user_id)
    user_data = session.execute(query).mappings().all()
    return user_data

def get_user_name(user_id):
    query = select(names_table.c.real_name).where(names_table.c.real_user_id == user_id)
    result = session.execute(query).mappings().one_or_none()
    if result:
        return result['real_name']
    return None

def format_data_for_sheet(user_data):
    formatted_data = []
    for record in user_data:
        date = record['date']
        start_time = record['start_time']
        end_time = record['end_time']
        leads = record['leads']

        date_str = date.strftime('%d/%m/%Y')
        month_str = date.strftime('%B')
        month_str_ru = MONTHS_EN_TO_RU.get(month_str, month_str)
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

    headers = ['Manager', 'Month', 'Date', 'Start Time', 'End Time', 'Leads', 'Year']
    execute_with_retry(lambda: data_sheet.clear())

    data_with_year = []
    for row in all_data:
        date_str = row[2]
        date_obj = datetime.strptime(date_str, '%d/%m/%Y')
        year = date_obj.year
        cleaned_row = [cell.strip() if isinstance(cell, str) else cell for cell in row]
        data_with_year.append(cleaned_row + [str(year)])

    execute_with_retry(lambda: data_sheet.update([headers] + data_with_year))

def update_manager_sheet(manager_name, months, years):
    client = authorize_google_sheets()
    spreadsheet = client.open(GOOGLE_SHEET)

    try:
        manager_sheet = spreadsheet.worksheet(manager_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{manager_name}' not found. Creating new worksheet.")
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

    months_capitalized = [month.capitalize() for month in months]
    sheet_id = manager_sheet._properties['sheetId']
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
                    'startRowIndex': 1,
                    'endRowIndex': 2,
                    'startColumnIndex': 3,
                    'endColumnIndex': 4
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

    labels = [['Месяц'], ['Дата'], ['Время работы'], ['Лидов получено'], ['Лидов за месяц итого']]
    execute_with_retry(lambda: manager_sheet.update('A2:A6', labels))

    date_formula = '''=IFERROR(
  TRANSPOSE(
    UNIQUE(
      FILTER(Data!C2:C,
        (TRIM(Data!A2:A)=TRIM($A$1)) *
        (LOWER(TRIM(Data!B2:B))=LOWER(TRIM($B$2))) *
        (TRIM(Data!G2:G)=TRIM($D$2)) *
        (
          ((LEN(TRIM(Data!D2:D))>0) +
           (LEN(TRIM(Data!E2:E))>0) +
           (Data!F2:F>0)
          ) > 0
        )
      )
    )
  ),
  "Нет данных"
)
'''
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
        )'''
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
    FILTER(Data!F2:F,
      (TRIM(Data!A2:A)=TRIM($A$1)) *
      (LOWER(TRIM(Data!B2:B))=LOWER(TRIM($B$2))) *
      (TRIM(Data!G2:G)=TRIM($D$2))
    )
  ),
  0
)'''
    execute_with_retry(lambda: manager_sheet.update('B6', [[total_leads_formula]], value_input_option='USER_ENTERED'))

    apply_formatting(manager_sheet)

def apply_formatting(worksheet):
    # Код форматирования остаётся без изменений
    # ...
    # (Содержимое apply_formatting без изменений)
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
        },
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
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": 6,
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
                        "verticalAlignment": "MIDDLE"
                    }
                },
                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.verticalAlignment"
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": 1000,
                    "startColumnIndex": 1,
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

    row_colors = {
        '3': (0.851, 0.918, 0.827),
        '4': (0.918, 0.82, 0.863),
        '5': (0.918, 0.82, 0.863),
        '6': (0.757, 0.482, 0.627),
    }
    for row_num_str, color_tuple in row_colors.items():
        row_num = int(row_num_str) - 1
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

    num_rows = 6
    requests.append({
        "updateBorders": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
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

    requests.append({
        "mergeCells": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": 2,
                "startColumnIndex": 1,
                "endColumnIndex": 3
            },
            "mergeType": "MERGE_ALL"
        }
    })

    requests.append({
        "mergeCells": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": 2,
                "startColumnIndex": 3,
                "endColumnIndex": 5
            },
            "mergeType": "MERGE_ALL"
        }
    })

    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": 2,
                "startColumnIndex": 1,
                "endColumnIndex": 3
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

    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": 2,
                "startColumnIndex": 3,
                "endColumnIndex": 5
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

    execute_with_retry(lambda: worksheet.spreadsheet.batch_update({'requests': requests}))
    worksheet.freeze(rows=2, cols=1)


def update_main_sheet(manager_names, all_months, all_years):
    client = authorize_google_sheets()
    spreadsheet = client.open(GOOGLE_SHEET)

    try:
        main_sheet = spreadsheet.worksheet('Основная страница')
    except gspread.exceptions.WorksheetNotFound:
        print("Worksheet 'Основная страница' not found. Creating new worksheet.")
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
                    'startRowIndex': 1,
                    'endRowIndex': 2,
                    'startColumnIndex': 1,
                    'endColumnIndex': 2
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

    data = [[manager_name] for manager_name in manager_names]
    print(f"Updating range A3 with data: {data}")
    execute_with_retry(lambda: main_sheet.update('A3', data))

    num_rows = len(manager_names) + 2
    formulas_b = []
    for idx in range(len(manager_names)):
        row = idx + 3
        formula = f"=IFERROR(SUM(FILTER(Data!F:F, (Data!A:A=A{row})*(Data!B:B=B$1)*(Data!G:G=B$2))), 0)"
        formulas_b.append([formula])

    execute_with_retry(lambda: main_sheet.update('B3', formulas_b, value_input_option='USER_ENTERED'))

    formulas_c = []
    for idx in range(len(manager_names)):
        row = idx + 3
        formula = f"=IFERROR(SUMIF(Data!A:A, A{row}, Data!F:F), 0)"
        formulas_c.append([formula])

    execute_with_retry(lambda: main_sheet.update('C3', formulas_c, value_input_option='USER_ENTERED'))

    apply_main_sheet_formatting(main_sheet, num_rows)

def apply_main_sheet_formatting(main_sheet, num_rows):
    # Код форматирования для основной страницы остается без изменений
    # ...
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

    execute_with_retry(lambda: main_sheet.spreadsheet.batch_update({'requests': requests}))
    main_sheet.freeze(rows=2)


async def update_user_data():
    print('Было запущено обновление даных страниц.')
    user_ids = session.query(user_info_table.c.user_id).distinct().all()
    all_data = []
    manager_months = {}
    manager_years = {}
    manager_names = []

    for user_id_tuple in user_ids:
        user_id = user_id_tuple[0]
        real_name = get_user_name(user_id)
        if real_name:
            user_data = fetch_user_data(user_id)  # Тут user_data - список словарей
            data = format_data_for_sheet(user_data)
            if data:
                for row in data:
                    all_data.append([real_name] + row)
                months = [row[0].strip() for row in data]
                unique_months = sorted(set(months), key=lambda m: MONTHS_RU_ORDER.get(m, 0))
                manager_months[real_name] = unique_months

                years = [r[1].split('/')[-1] for r in data]
                unique_years = sorted(set(years))
                manager_years[real_name] = unique_years

                manager_names.append(real_name)

    update_hidden_data_sheet(all_data)
    print('Было закончено обновление даных страниц.')

async def main():
    user_ids = session.query(user_info_table.c.user_id).distinct().all()
    all_data = []
    manager_months = {}
    manager_years = {}
    manager_names = []

    for user_id_tuple in user_ids:
        user_id = user_id_tuple[0]
        real_name = get_user_name(user_id)
        if real_name:
            user_data = fetch_user_data(user_id)  # словари
            data = format_data_for_sheet(user_data)
            if data:
                for row in data:
                    all_data.append([real_name] + row)
                months = [row[0].strip() for row in data]
                unique_months = sorted(set(months), key=lambda m: MONTHS_RU_ORDER.get(m, 0))
                manager_months[real_name] = unique_months

                years = [r[1].split('/')[-1] for r in data]
                unique_years = sorted(set(years))
                manager_years[real_name] = unique_years

                manager_names.append(real_name)

    update_hidden_data_sheet(all_data)

    all_months = set()
    all_years = set()
    for m in manager_months.values():
        all_months.update(m)
    for y in manager_years.values():
        all_years.update(y)

    update_main_sheet(manager_names, all_months, all_years)

    for manager_name in manager_names:
        m = manager_months.get(manager_name, [])
        y = manager_years.get(manager_name, [])
        update_manager_sheet(manager_name, m, y)
        time.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())