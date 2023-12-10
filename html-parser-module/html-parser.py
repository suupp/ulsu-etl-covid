import requests
from bs4 import BeautifulSoup
import pyodbc
from datetime import datetime, timedelta
from datetime import date


def get_covid_data():
    url = 'https://horosho-tam.ru/rossiya/coronavirus'
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        russia_data = soup.find('tr', {'class': 'tr_mark'})

        if russia_data:
            country_name = russia_data.find('td', {'class': 'td_country'}).text.strip()
            current_date = datetime(datetime.now().year, datetime.now().month, datetime.now().day)
            cases = int(russia_data.find('td', {'class': 'td_cases'}).text.replace(' ', '').strip())
            deaths = int(russia_data.find('td', {'class': 'td_deaths'}).text.strip())

            return {
                'country': country_name,
                'date': current_date,
                'cases': cases,
                'deaths': deaths
            }
        else:
            print("Данные для России не найдены на странице.")
            return None
    else:
        print(f"Ошибка при получении данных. Код статуса: {response.status_code}")
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
    # Проверяем существование таблицы при открытии соединения
    cursor.execute('''
        IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Covid_stats')
        CREATE TABLE Covid_stats (
            id INT PRIMARY KEY IDENTITY(1,1),
            date DATE,
            country NVARCHAR(255),
            cases INT,
            deaths INT
        )
    ''')
    
    # Получаем дату последнего обновления
    last_update_date = get_last_update_date(cursor)

    if not last_update_date or (datetime.now().date() - last_update_date).days >= 7:
        # Добавляем данные
        cursor.execute('''
            INSERT INTO Covid_stats (date, country, cases, deaths)
            VALUES (?, ?, ?, ?)
        ''', (data['date'], data['country'], data['cases'], data['deaths']))

        cursor.commit()
        print("Данные успешно добавлены.")
    else:
        print("Недостаточно времени прошло с последнего обновления.")

def display_covid_data(cursor):
    cursor.execute('SELECT TOP 1 * FROM Covid_stats ORDER BY date DESC')
    data = cursor.fetchone()

    if data:
        print("Данные из базы данных:")
        for index, field in enumerate(data):
            field_name = cursor.description[index][0]
            print(f"{field_name}: {field}")

def main():
    # Параметры подключения к SQL Server
    server = r'MY-NOTEBOOK-00\SQLEXPRESS'
    database = 'CovidData'
    username = 'MY-NOTEBOOK-00\Я'

    # Строка подключения
    connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};Trusted_Connection=yes;'

    # Подключение к базе данных
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    covid_data = get_covid_data()

    if covid_data:
        save_to_database(covid_data, cursor)
        display_covid_data(cursor)
    else:
        print("Не удалось получить данные.")

    # Закрытие соединения
    conn.close()

if __name__ == '__main__':
    main()
