import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from datetime import datetime
from gspread_formatting import set_frozen, format_cell_range, CellFormat, TextFormat, Color, Borders, Border
from gspread_formatting.dataframe import format_with_dataframe
from config import JSON_FILE

# Маппинг месяцев с английского на русский
MONTHS_EN_TO_RU = {
    'January': 'Январь', 'February': 'Февраль', 'March': 'Март', 'April': 'Апрель', 'May': 'Май', 'June': 'Июнь',
    'July': 'Июль', 'August': 'Август', 'September': 'Сентябрь', 'October': 'Октябрь', 'November': 'Ноябрь', 'December': 'Декабрь'
}

# Setup database connection
DATABASE_URL = 'sqlite:///database.db'
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
    #for i in user_data:
    #    print(f"Fetched user data: {i}")
    return user_data

def get_user_name(user_id):
    query = select(names_table).where(names_table.c.real_user_id == user_id)
    result = session.execute(query).fetchone()
    #print(f"Fetched user name: {result[1] if result else 'None'}")
    if result:
        return result[1]  # 'real_name' is the second column in the result
    return None

def format_data_for_sheet(user_data):
    date_to_data = {}
    start_time_idx = 3
    end_time_idx = 4
    leads_idx = 5

    # Group data by date
    for record in user_data:
        start_time = record[start_time_idx]
        end_time = record[end_time_idx] if record[end_time_idx] else None
        leads = record[leads_idx]

        if isinstance(start_time, datetime):
            date_str = start_time.strftime('%d/%m')  # Format date as DD/MM
            month_str = start_time.strftime('%B')    # Format month as full month name
            month_str_ru = MONTHS_EN_TO_RU.get(month_str, month_str)  # Translate month to Russian
        else:
            print(f"Unexpected start_time type: {type(start_time)}")
            continue
        
        if date_str not in date_to_data:
            date_to_data[date_str] = {'start_times': [], 'finish_times': [], 'leads': 0, 'month': month_str_ru}
        
        date_to_data[date_str]['start_times'].append(start_time)
        if end_time:
            date_to_data[date_str]['finish_times'].append(end_time)
        date_to_data[date_str]['leads'] += leads
    
    # Sort dates, prioritizing the month, and then the day
    sorted_dates = sorted(date_to_data.keys(), key=lambda d: (datetime.strptime(d, '%d/%m').month, datetime.strptime(d, '%d/%m').day))
    leads_total = sum([date_to_data[date]['leads'] for date in sorted_dates])

    # Adding month row above date row
    months = [date_to_data[date]['month'] for date in sorted_dates]
    data = [["Месяц"] + months,
            ["Дата"] + sorted_dates,
            ["Время старта"] + [min(date_to_data[date]['start_times']).strftime('%H:%M') for date in sorted_dates],
            ["Время финиша"] + [max(date_to_data[date]['finish_times']).strftime('%H:%M') if date_to_data[date]['finish_times'] else '' for date in sorted_dates],
            ["Лидов получено"] + [date_to_data[date]['leads'] for date in sorted_dates],
            ["Лидов за месяц итого", leads_total]]

    return data

def update_main_sheet():
    client = authorize_google_sheets()
    spreadsheet = client.open("BALI LOVERS")
    worksheet = spreadsheet.worksheet('Основная страница')

    # Установить текст в ячейке A1
    worksheet.update('A1', [["Общая информация по менеджерам"]])

    # Получить всех пользователей и их общее количество лидов
    user_leads = {}
    user_ids = session.query(user_info_table.c.user_id).distinct().all()
    
    for user_id_tuple in user_ids:
        user_id = user_id_tuple[0]
        real_name = get_user_name(user_id)
        if real_name:
            user_data = fetch_user_data(user_id)
            total_leads = sum(record[5] for record in user_data)  # Предполагается, что количество лидов в 7-й колонке
            user_leads[real_name] = total_leads
    
    # Подготовить данные для Google Sheets
    data = [["Менеджер", "Количество лидов"]] + [[real_name, total_leads] for real_name, total_leads in user_leads.items()]

    # Обновить лист начиная с ячейки A2
    try:
        worksheet.update('A2', data)
        
        # Форматирование заголовков
        header_format_A1 = CellFormat(
            backgroundColor=Color(0.678, 0.847, 0.902),  # Голубой цвет
            textFormat=TextFormat(fontSize=12, bold=True)
        )
        format_cell_range(worksheet, 'A1', header_format_A1)

        header_format_A2_B2 = CellFormat(
            backgroundColor=Color(0.8, 1, 0.8),  # Светло-зеленый цвет для заголовков
            textFormat=TextFormat(bold=True)
        )
        format_cell_range(worksheet, 'A2:B2', header_format_A2_B2)

        # Объединение ячеек A1 и B1
        worksheet.merge_cells('A1:B1')

        # Применение формата границ ко всем ячейкам
        border_format = CellFormat(
            borders=Borders(
                top=Border('SOLID', width=1),
                bottom=Border('SOLID', width=1),
                left=Border('SOLID', width=1),
                right=Border('SOLID', width=1)
            )
        )
        last_col = 'B'
        num_rows = len(data) + 1  # +1, так как начинается с A2
        format_cell_range(worksheet, f'A2:{last_col}{num_rows}', border_format)

        # Настройка ширины колонок
        sheet_id = worksheet._properties['sheetId']
        requests = [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,  # Первый столбец 'A'
                        "endIndex": 1     # Второй столбец 'B'
                    },
                    "properties": {
                        "pixelSize": 125
                    },
                    "fields": "pixelSize"
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 1,  # Второй столбец 'B'
                        "endIndex": 2     # Третий столбец (C)
                    },
                    "properties": {
                        "pixelSize": 175
                    },
                    "fields": "pixelSize"
                }
            }
        ]
        spreadsheet.batch_update({"requests": requests})

        print("Main sheet updated successfully.")
    except gspread.exceptions.APIError as e:
        print(f"API Error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


# Функция для обновления таблицы
def update_sheet(real_name, data):
    client = authorize_google_sheets()
    spreadsheet = client.open("BALI LOVERS")
    worksheet = spreadsheet.worksheet(real_name)

    try:
        # Проверка данных
        if not data or not isinstance(data, list) or not all(isinstance(row, list) for row in data):
            raise ValueError("Invalid data format")
        num_rows = len(data)
        num_cols = max(len(row) for row in data)
        last_col = chr(ord('A') + num_cols - 1)
        data_range = f'A2:{last_col}{num_rows + 1}'

        # Обновление диапазона ячеек
        worksheet.update(range_name=data_range, values=data)
        print(f"Data updated for {real_name}: {data}")

        # Установка ширины первого столбца в 150 пикселей через batch_update
        sheet_id = worksheet._properties['sheetId']
        requests = [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,  # Индекс 0 означает первый столбец 'A'
                        "endIndex": 1     # До второго столбца 'B' (но не включая)
                    },
                    "properties": {
                        "pixelSize": 185
                    },
                    "fields": "pixelSize"
                }
            }
        ]
        spreadsheet.batch_update({"requests": requests})

        # Закрашивание ячейки A1 в голубой цвет, текст 12, жирный
        header_format_A1 = CellFormat(
            backgroundColor=Color(0.678, 0.847, 0.902),  # Голубой цвет
            textFormat=TextFormat(fontSize=12, bold=True)
        )
        format_cell_range(worksheet, 'A1', header_format_A1)


        # Объединение ячеек с одинаковым текстом в строке 2
        start_col = None
        for i in range(1, num_cols):  # начинаем с 1, так как 0 - это первая колонка
            if data[0][i] == data[0][i-1]:  # если текст в ячейке равен предыдущему
                if start_col is None:
                    start_col = chr(ord('A') + i - 1)
            else:
                if start_col:
                    end_col = chr(ord('A') + i - 1)
                    merge_range = f'{start_col}2:{end_col}2'
                    worksheet.merge_cells(merge_range)
                    print(f"Merged cells with the same text in range {merge_range}")
                    start_col = None

        # Если объединение должно происходить в последнем столбце
        if start_col:
            end_col = chr(ord('A') + num_cols - 1)
            merge_range = f'{start_col}2:{end_col}2'
            worksheet.merge_cells(merge_range)
            print(f"Merged cells with the same text in range {merge_range}")

        # Заморозить строки с заголовками для удобства
        print("Freezing header rows")
        set_frozen(worksheet, cols=1)

        # Применение цветового форматирования и выравнивание по центру для заголовков
        header_format = CellFormat(
            backgroundColor=Color(0.8, 1, 0.8),  # Светло-зеленый цвет для заголовков
            textFormat=TextFormat(bold=True),
            horizontalAlignment='CENTER',
            borders=Borders(
                top=Border('SOLID', width=1),
                bottom=Border('SOLID', width=1),
                left=Border('SOLID', width=1),
                right=Border('SOLID', width=1)
            )
        )
        header_range = f'A2:{last_col}2'
        #print(f"Applying header formatting to range {header_range}")
        format_cell_range(worksheet, header_range, header_format)

        # Применение выравнивания по центру для всех ячеек кроме первого столбца
        center_alignment_format = CellFormat(horizontalAlignment='CENTER')
        center_alignment_range = f'B3:{last_col}{num_rows + 1}'
        format_cell_range(worksheet, center_alignment_range, center_alignment_format)

        # Форматирование ячейки "Лидов за месяц итого" и её значения
        total_leads_format = CellFormat(
            textFormat=TextFormat(fontSize=11, bold=True),
        )
        format_cell_range(worksheet, f'A{num_rows + 1}:{last_col}{num_rows + 1}', total_leads_format)

        # Форматирование строк с временем старта и финиша, лидов и итогов
        row_formats = {
            '3': CellFormat(backgroundColor=Color(0.851, 0.918, 0.827)),  # Светло-зеленый для времени старта
            '4': CellFormat(backgroundColor=Color(0.918, 0.82, 0.863)),  # Тёмно-розовый для времени финиша
            '5': CellFormat(backgroundColor=Color(0.918, 0.82, 0.863)),  # Тёмно-розовый для лидов получено
            '6': CellFormat(backgroundColor=Color(0.918, 0.82, 0.863)),  # Тёмно-розовый для итогового количества лидов
            '7': CellFormat(backgroundColor=Color(0.757, 0.482, 0.627)),  # Тёмно-розовый для итогового количества лидов
        }
        
        for row, fmt in row_formats.items():
            row_range = f'A{row}:{last_col}{row}'
            #print(f"Applying row formatting to range {row_range}")
            format_cell_range(worksheet, row_range, fmt)

        # Применение формата границ ко всем ячейкам
        border_format = CellFormat(
            borders=Borders(
                top=Border('SOLID', width=1),
                bottom=Border('SOLID', width=1),
                left=Border('SOLID', width=1),
                right=Border('SOLID', width=1)
            )
        )
        border_range = f'A2:{last_col}{num_rows + 1}'
        #print(f"Applying border formatting to range {border_range}")
        format_cell_range(worksheet, border_range, border_format)

        print(f"Sheet for {real_name} updated successfully.")
    except gspread.exceptions.APIError as e:
        print(f"API Error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")



def main():
    user_ids = session.query(user_info_table.c.user_id).distinct().all()
    
    for user_id_tuple in user_ids:
        user_id = user_id_tuple[0]
        real_name = get_user_name(user_id)
        if real_name:
            user_data = fetch_user_data(user_id)
            data = format_data_for_sheet(user_data)
            update_sheet(real_name, data)
    update_main_sheet()

if __name__ == "__main__":
    main()

#TODO 1) первая строчка, отступ 2) выравние везде по центру