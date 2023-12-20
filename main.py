from csvparserpkg.csvparser import add_csv_data_to_database
from htmlparserpkg.htmlparser import get_covid_data, save_to_database
import pyodbc

def connect_to_database():
    # Параметры подключения к SQL Server
    server = r'MY-NOTEBOOK-00\SQLEXPRESS'
    database = 'CovidData'
    username = 'MY-NOTEBOOK-00\Я'

    # Строка подключения
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};Trusted_Connection=yes;'

    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        return conn, cursor
    except pyodbc.Error as e:
        print(f"Ошибка при подключении к базе данных: {e}")
        raise

def close_database_connection(conn):
    try:
        conn.close()
    except pyodbc.Error as e:
        print(f"Ошибка при закрытии соединения с базой данных: {e}")

def process_csv_file(cursor, filepath, date_column, location_column, cases_column, deaths_column):
    try:
        add_csv_data_to_database(cursor, filepath, date_column, location_column, cases_column, deaths_column)
    except Exception as e:
        print(f"Ошибка при обработке CSV-файла {filepath}: {e}")

def main():
    conn = None
    try:
        conn, cursor = connect_to_database()
        # Обработка CSV-файлов
        process_csv_file(cursor, 'data.csv', 'dateRep', 'countriesAndTerritories', 'cases', 'deaths')
        process_csv_file(cursor, 'owid-covid-data.csv', 'date', 'location', 'new_cases', 'new_deaths')

        # Обработка HTML-данных
        covid_data_html = get_covid_data()

        # Сохранение HTML-данных в базе данных
        if covid_data_html:
            save_to_database(covid_data_html, cursor)
        else:
            print("Не удалось получить данные из HTML.")
    finally:
        if conn:
            close_database_connection(conn)

if __name__ == '__main__':
    main()