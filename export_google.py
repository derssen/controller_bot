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

# Setup database connection
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()

# Reflect tables from the database
names_table = Table('names', metadata, autoload_with=engine)
user_info_table = Table('user_info', metadata, autoload_with=engine)

# Setup Google Sheets API
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

# Function to format data into a tabular format
def format_data_for_sheet(user_data):
    formatted_data = []
    for record in user_data:
        date = record[2]  # Assuming date is at index 2
        start_time = record[3]
        end_time = record[4]
        leads = record[5]

        # Format date and times
        date_str = date.strftime('%d/%m/%Y')
        month_str = date.strftime('%B')
        month_str_ru = MONTHS_EN_TO_RU.get(month_str, month_str)  # Translate month to Russian
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
            break  # If successful, exit the loop
        except gspread.exceptions.APIError as e:
            status = e.response.status_code
            if status == 429:
                print(f"Quota exceeded. Waiting for {initial_delay} seconds before retrying...")
                time.sleep(initial_delay)
                if delay_on_quota:
                    initial_delay *= 2  # Exponential backoff
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

    # Create or select the hidden data sheet
    try:
        data_sheet = spreadsheet.worksheet('Data')
    except gspread.exceptions.WorksheetNotFound:
        data_sheet = spreadsheet.add_worksheet(title='Data', rows="1000", cols="10")
        # Hide the sheet
        data_sheet.hide()

    # Prepare headers
    headers = ['Manager', 'Month', 'Date', 'Start Time', 'End Time', 'Leads', 'Year']

    # Clear existing data and update
    execute_with_retry(lambda: data_sheet.clear())

    # Process data to remove spaces and ensure real empty cells
    data_with_year = []
    for row in all_data:
        # Add year to each data row
        date_str = row[2]  # Assuming date is at index 2
        date_obj = datetime.strptime(date_str, '%d/%m/%Y')
        year = date_obj.year

        # Replace any "empty" spaces with real empty strings
        cleaned_row = [cell.strip() if isinstance(cell, str) else cell for cell in row]
        data_with_year.append(cleaned_row + [str(year)])

    # Update the sheet with cleaned data
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
    execute_with_retry(lambda: manager_sheet.update([[manager_name]], 'A1'))

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
    execute_with_retry(lambda: manager_sheet.update([[default_month]], 'B2'))
    execute_with_retry(lambda: manager_sheet.update([[default_year]], 'D2'))

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
    execute_with_retry(lambda: manager_sheet.update(labels, 'A2:A6'))

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
    execute_with_retry(lambda: manager_sheet.update([[date_formula]], 'B3', value_input_option='USER_ENTERED'))

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
    execute_with_retry(lambda: manager_sheet.update([[working_time_formula]], 'B4', value_input_option='USER_ENTERED'))

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
    execute_with_retry(lambda: manager_sheet.update([[leads_formula]], 'B5', value_input_option='USER_ENTERED'))

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
    execute_with_retry(lambda: manager_sheet.update([[total_leads_formula]], 'B6', value_input_option='USER_ENTERED'))

    # Применяем форматирование
    apply_formatting(manager_sheet)

    # Добавляем задержку между обновлениями листов менеджеров
    time.sleep(60)  # Настройте при необходимости


def apply_formatting(worksheet):
    # Set column widths
    sheet_id = worksheet._properties['sheetId']
    last_col_index = 26  # Adjust as needed

    requests = [
        # Set width for column A to 200 pixels
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,  # Column A
                    "endIndex": 1     # Up to Column B
                },
                "properties": {
                    "pixelSize": 200
                },
                "fields": "pixelSize"
            }
        },
        # Set width for columns B onwards
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 1,  # Column B
                    "endIndex": last_col_index
                },
                "properties": {
                    "pixelSize": 100  # Adjust as needed
                },
                "fields": "pixelSize"
            }
        },
    ]

    # Format cell A1 (Manager's Name)
    requests.append({
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
            "fields": "userEnteredFormat(backgroundColor,textFormat)"
        }
    })

    # Format labels in A2:A7 (including A7)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,  # Row 2
                "endRowIndex": 6,    # Row 7
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
                    }
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat)"
        }
    })

    # Merge cells B2 and C2 for Month selection
    requests.append({
        "mergeCells": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,    # Row 2
                "endRowIndex": 2,      # Row 3
                "startColumnIndex": 1, # Column B
                "endColumnIndex": 3    # Column C
            },
            "mergeType": "MERGE_ALL"
        }
    })

    # Merge cells D2 and E2 for Year selection
    requests.append({
        "mergeCells": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,    # Row 2
                "endRowIndex": 2,      # Row 3
                "startColumnIndex": 3, # Column D
                "endColumnIndex": 5    # Column E
            },
            "mergeType": "MERGE_ALL"
        }
    })

    # Format merged cell B2:C2 (Month Selection)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,    # Row 2
                "endRowIndex": 2,      # Row 3
                "startColumnIndex": 1, # Column B
                "endColumnIndex": 3    # Column C
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {
                        "red": 0.851,
                        "green": 0.918,
                        "blue": 0.827
                    },
                    "textFormat": {
                        "fontSize": 12,
                        "bold": True
                    }
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat)"
        }
    })

    # Format merged cell D2:E2 (Year Selection)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,    # Row 2
                "endRowIndex": 2,      # Row 3
                "startColumnIndex": 3, # Column D
                "endColumnIndex": 5    # Column E
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {
                        "red": 0.851,
                        "green": 0.918,
                        "blue": 0.827
                    },
                    "textFormat": {
                        "fontSize": 12,
                        "bold": True
                    }
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat)"
        }
    })

    # Center alignment from B2 onwards
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,  # Row 2
                "endRowIndex": 1000, # Adjust as needed
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
            "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,wrapStrategy)"
        }
    })

    # Apply background colors to rows 3-7
    row_colors = {
        '3': (0.851, 0.918, 0.827),  # Light green for "Дата"
        '4': (0.918, 0.82, 0.863),   # Light pink for "Время старта"
        '5': (0.918, 0.82, 0.863),   # Light pink for "Время финиша"
        '6': (0.757, 0.482, 0.627),  # Dark pink for "Лидов за месяц итого"
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
                "fields": "userEnteredFormat(backgroundColor)"
            }
        })

    # Apply borders to the data range
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
            "top": {"style": "SOLID", "width": 1},
            "bottom": {"style": "SOLID", "width": 1},
            "left": {"style": "SOLID", "width": 1},
            "right": {"style": "SOLID", "width": 1},
            "innerHorizontal": {"style": "SOLID", "width": 1},
            "innerVertical": {"style": "SOLID", "width": 1},
        }
    })

    # Execute all formatting requests in a single batch_update
    execute_with_retry(lambda: worksheet.spreadsheet.batch_update({'requests': requests}))

    # Apply formats that cannot be batched, like freezing panes
    worksheet.freeze(rows=0, cols=1)  # Freeze first two columns



def update_main_sheet(manager_names):
    client = authorize_google_sheets()
    spreadsheet = client.open(GOOGLE_SHEET)

    # Create or select the main sheet named 'Основная страница'
    try:
        main_sheet = spreadsheet.worksheet('Основная страница')
    except gspread.exceptions.WorksheetNotFound:
        print("Worksheet 'Основная страница' not found. Creating new worksheet.")
        # Wrap the add_worksheet call with execute_with_retry
        def add_worksheet():
            spreadsheet.add_worksheet(title='Основная страница', rows="1000", cols="10")
        execute_with_retry(add_worksheet, retries=5, initial_delay=60)
        # After adding the worksheet, get a reference to it
        main_sheet = spreadsheet.worksheet('Основная страница')

    # Clear the main sheet
    execute_with_retry(lambda: main_sheet.clear())

    # Set up header in A1 and merge A1:B1
    execute_with_retry(lambda: main_sheet.update([['Общая информация по менеджерам']], 'A1'))
    sheet_id = main_sheet._properties['sheetId']
    merge_request = {
        "mergeCells": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,   # Row 1
                "endRowIndex": 1,     # Row 2
                "startColumnIndex": 0,  # Column A
                "endColumnIndex": 2    # Column B
            },
            "mergeType": "MERGE_ALL"
        }
    }

    # Apply specified header_format_A1 to cell A1
    header_format_A1 = CellFormat(
        backgroundColor=Color(0.678, 0.847, 0.902),  # Голубой цвет (light blue)
        textFormat=TextFormat(fontSize=12, bold=True)
    )
    execute_with_retry(lambda: format_cell_range(main_sheet, 'A1', header_format_A1))

    # Set up headers in A2:B2
    execute_with_retry(lambda: main_sheet.update([['Менеджер', 'Количество лидов']], 'A2:B2'))

    # Write manager names in column A starting from A3
    data = [[manager_name] for manager_name in manager_names]
    execute_with_retry(lambda: main_sheet.update(data, 'A3'))

    # Set formula in column B to calculate total leads from Data sheet
    num_rows = len(manager_names) + 2  # Including header rows
    formulas = []
    for idx in range(len(manager_names)):
        row = idx + 3  # Starting from row 3
        formula = f"=SUMIF(Data!A:A, A{row}, Data!F:F)"
        formulas.append([formula])

    execute_with_retry(lambda: main_sheet.update(formulas, f'B3', value_input_option='USER_ENTERED'))

    # Prepare formatting requests
    requests = [merge_request]

    # Adjust column widths
    requests += [
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,  # Column A
                    "endIndex": 1     # Up to Column B
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
                    "startIndex": 1,  # Column B
                    "endIndex": 2     # Up to Column C
                },
                "properties": {
                    "pixelSize": 150
                },
                "fields": "pixelSize"
            }
        },
    ]

    # Set background color for all cells with text (from A1 to B{num_rows})
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,          # Include header
                "endRowIndex": num_rows,     # Last row with data
                "startColumnIndex": 0,
                "endColumnIndex": 2,         # Columns A and B
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
            "fields": "userEnteredFormat(backgroundColor)"
        }
    })

    # Align column A to the left
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 2,      # From row 3 onwards
                "endRowIndex": num_rows,
                "startColumnIndex": 0,   # Column A
                "endColumnIndex": 1
            },
            "cell": {
                "userEnteredFormat": {
                    "horizontalAlignment": "LEFT"
                }
            },
            "fields": "userEnteredFormat(horizontalAlignment)"
        }
    })

    # Center alignment for column B
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 2,      # From row 3 onwards
                "endRowIndex": num_rows,
                "startColumnIndex": 1,   # Column B
                "endColumnIndex": 2
            },
            "cell": {
                "userEnteredFormat": {
                    "horizontalAlignment": "CENTER"
                }
            },
            "fields": "userEnteredFormat(horizontalAlignment)"
        }
    })

    # Apply borders to the data range
    requests.append({
        "updateBorders": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,  # Row 2
                "endRowIndex": num_rows,
                "startColumnIndex": 0,
                "endColumnIndex": 2
            },
            "top": {"style": "SOLID", "width": 1},
            "bottom": {"style": "SOLID", "width": 1},
            "left": {"style": "SOLID", "width": 1},
            "right": {"style": "SOLID", "width": 1},
            "innerHorizontal": {"style": "SOLID", "width": 1},
            "innerVertical": {"style": "SOLID", "width": 1},
        }
    })

    # Execute all formatting requests in a single batch_update
    execute_with_retry(lambda: spreadsheet.batch_update({'requests': requests}))

    # Freeze the first two rows
    main_sheet.freeze(rows=2)



async def main():
    user_ids = session.query(user_info_table.c.user_id).distinct().all()

    # Collect all data and write to hidden data sheet
    all_data = []
    manager_months = {}  # To keep track of months for each manager
    manager_years = {}   # To keep track of years for each manager
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
                # Collect unique months and years for the manager
                months = [row[0].strip() for row in data]  # row[0] is month in Russian
                unique_months = sorted(set(months), key=lambda m: MONTHS_RU_ORDER.get(m, 0))
                manager_months[real_name] = unique_months

                years = [row[1].split('/')[-1] for row in data]  # Extract year from date
                unique_years = sorted(set(years))
                manager_years[real_name] = unique_years

                manager_names.append(real_name)

    # Update the hidden data sheet with all users' data
    update_hidden_data_sheet(all_data)

    # Update each manager's sheet
    for manager_name in manager_names:
        months = manager_months.get(manager_name, [])
        years = manager_years.get(manager_name, [])
        update_manager_sheet(manager_name, months, years)
        # Add a delay between manager updates
        time.sleep(1)  # Adjust as needed

    # Update the main sheet
    update_main_sheet(manager_names)

def update_one_sheet(manager_name):
    manager_name = get_user_name(manager_name)
    user_ids = session.query(user_info_table.c.user_id).distinct().all()

    all_data = []
    manager_months = {}  # To keep track of months for each manager
    manager_years = {}   # To keep track of years for each manager
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
                # Collect unique months and years for the manager
                months = [row[0].strip() for row in data]  # row[0] is month in Russian
                unique_months = sorted(set(months), key=lambda m: MONTHS_RU_ORDER.get(m, 0))
                manager_months[real_name] = unique_months

                years = [row[1].split('/')[-1] for row in data]  # Extract year from date
                unique_years = sorted(set(years))
                manager_years[real_name] = unique_years

                manager_names.append(real_name)
    update_manager_sheet(manager_name, months, years)
    
    

if __name__ == "__main__":
    asyncio.run(main())
