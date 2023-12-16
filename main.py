from csvparserpkg.csvparser import parse_dated_csvfile
from csvparserpkg.csvparser import rename_keys
from csvparserpkg.csvparser import save_csv_to_database,add_csv_data_to_database,add_csv_data_to_database_2
from htmlparserpkg.htmlparser import get_covid_data, get_last_update_date, save_to_database, display_covid_data
import pyodbc
from datetime import datetime



def main():
    # Параметры подключения к SQL Server
    server = r'MY-NOTEBOOK-00\SQLEXPRESS'
    database = 'CovidData'
    username = 'MY-NOTEBOOK-00\Я'

    # Строка подключения
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};Trusted_Connection=yes;'

    # Подключение к базе данных
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # Получение данных из HTML-парсера
    covid_data_html = get_covid_data()

    # Сохранение данных в базу данных
    if covid_data_html:
        save_to_database(covid_data_html, cursor)
        display_covid_data(cursor)
    else:
        print("Не удалось получить данные из HTML.")


     # Добавление данных из CSV в базу данных
    csv_filepath = 'data.csv'
    add_csv_data_to_database(cursor, csv_filepath, 'dateRep', 'countriesAndTerritories', 'cases', 'deaths')

    csv_filepath_2 = 'owid-covid-data.csv'
    add_csv_data_to_database(cursor, csv_filepath_2, 'date', 'location', 'new_cases', 'new_deaths')
    #add_csv_data_to_database_2(csv_filepath_2, cursor)
    # Отображение данных
    display_covid_data(cursor)

    # Закрытие соединения
    conn.close()

if __name__ == '__main__':
    main()