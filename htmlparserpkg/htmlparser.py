import requests
from bs4 import BeautifulSoup
import pyodbc
from datetime import datetime
import logging

# Конфигурация логгера для HTML-парсера
html_logger = logging.getLogger('html_parser')
html_logger.setLevel(logging.INFO)
html_handler = logging.FileHandler('html_parser.log')
html_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
html_handler.setFormatter(html_formatter)
html_logger.addHandler(html_handler)

def get_covid_data():
    try:
        url = 'https://horosho-tam.ru/rossiya/coronavirus'
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            date_cells = soup.find('tr').find_all('th')      
            current_date = None

            if len(date_cells) > 1:
                # Используем текущий год и текущий месяц
                date_cell = date_cells[1]
                date_text = date_cell.get_text(strip=True)

                # Извлекаем числовую часть из текста
                day_str = ''.join(filter(str.isdigit, date_text))

                if day_str:
                 current_date = datetime(datetime.now().year, datetime.now().month, int(day_str))

            rows = soup.find_all('tr', {'class': ['tb_counter_odd', 'tb_counter_even']})

            country_name = ''
            active_cases = 0
            deaths = 0

            for row in rows:
                title_element = row.find('strong')
                value_element = row.find('b', {'class': 'tb_counter_sum'})

                if title_element and value_element:
                    title_text = title_element.text.strip()
                    value_text = value_element.text.strip()

                    if 'title_active' in title_element.get('class', []):
                        country_name = title_text
                        active_cases = int(value_text.replace('+', '').replace(' ', ''))
                    elif 'title_deaths' in title_element.get('class', []):
                        deaths = int(value_text.replace(' ', ''))

            if country_name and active_cases is not None and deaths is not None:
                html_logger.info("Данные успешно получены.")
                return {
                    'country': country_name,
                    'date': current_date,
                    'active_cases': active_cases,
                    'deaths': deaths
                }
            else:
                html_logger.warning("Данные для России не найдены на странице.")
                return None
        else:
            html_logger.error(f"Ошибка при получении данных. Код статуса: {response.status_code}")
            return None
    except Exception as e:
        html_logger.error(f"Ошибка при получении данных: {e}")
        return None

def get_last_update_date(cursor):
    cursor.execute('SELECT MAX(date) FROM Covid_stats')
    last_update_date = cursor.fetchone()[0]

    if last_update_date:
        if isinstance(last_update_date, datetime):
            last_update_date = last_update_date.date()
        elif isinstance(last_update_date, str):
            last_update_date = datetime.strptime(last_update_date, '%Y-%m-%d').date()

    return last_update_date


def save_to_database(data, cursor):
    try:
        cursor.execute('''
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Covid_stats')
            CREATE TABLE Covid_stats (
                id INT PRIMARY KEY IDENTITY(1,1),
                date DATE,
                country NVARCHAR(255),
                cases INT,
                deaths INT,
                source NVARCHAR(255)
            )
        ''')

        last_update_date = get_last_update_date(cursor)

        if not last_update_date or (datetime.now().date() - last_update_date).days >= 7:
            data['country'] = 'Russia'
            cursor.execute('SELECT 1 FROM Covid_stats WHERE date = ?', (data['date'],))
            duplicate_exists = cursor.fetchone()

            if not duplicate_exists:
                cursor.execute('''
                    INSERT INTO Covid_stats (date, country, cases, deaths, source)
                    VALUES (?, ?, ?, ?, 'from_html')
                ''', (data['date'], data['country'], data['active_cases'], data['deaths']))

                cursor.commit()
                html_logger.info("Данные успешно добавлены.")
            else:
                 html_logger.warning(f"Дубликат данных для даты {data['date']} обнаружен. Пропускаем.")
        else:
            html_logger.info("Недостаточно времени прошло с последнего обновления.")
    except Exception as e:
        html_logger.error(f"Ошибка при сохранении данных в базу: {e}")


def display_covid_data(cursor):
    cursor.execute('SELECT * FROM Covid_stats ORDER BY date DESC')
    data = cursor.fetchone()

    if data:
        print("Данные из базы данных:")
        for index, field in enumerate(data):
            field_name = cursor.description[index][0]
            print(f"{field_name}: {field}")

def main():
    try:
        # Параметры подключения к SQL Server
        server = r'MY-NOTEBOOK-00\SQLEXPRESS'
        database = 'CovidData'
        username = 'MY-NOTEBOOK-00\Я'

        # Строка подключения
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};Trusted_Connection=yes;'

        # Подключение к базе данных
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        covid_data = get_covid_data()

        if covid_data:
            save_to_database(covid_data, cursor)
        else:
           html_logger.warning("Не удалось получить данные.")

        # Закрытие соединения
        conn.close()
    except Exception as e:
        html_logger.error(f"Ошибка при выполнении программы: {e}")

if __name__ == '__main__':
    main()
