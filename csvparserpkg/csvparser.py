import csv
from datetime import datetime
from dateutil.parser import parse
import logging

# Конфигурация логгера для CSV-парсера
csv_logger = logging.getLogger('csv_parser')
csv_logger.setLevel(logging.INFO)
csv_handler = logging.FileHandler('csv_parser.log')
csv_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
csv_handler.setFormatter(csv_formatter)
csv_logger.addHandler(csv_handler)

def columns_to_parse(*args):
    return args

# Use this function to parse practically any .csv file that has date and country column column in it
# Dates automatically formatted into datetime.date(y, m, d), also function appends only records 
# where country is Russia:)
def parse_dated_csvfile(filepath, date_column_name, country_column_name, *other_columns):
    plist = []

    try:
        with open(filepath, newline='') as csvfile:
            read = csv.DictReader(csvfile, delimiter=',', quotechar='|')

            for row in read:
                for k, v in row.items():
                    try:
                        v.isnumeric()
                    except:
                        row[k] = v[0]

                good_keys = [date_column_name, country_column_name, *other_columns]
                bad_keys = [key for key in row.keys() if key not in good_keys]

                for key in bad_keys:
                    del row[key]

                row = {k: float(v) if v.replace('.', '').isnumeric() else v for k, v in row.items()}
                row[date_column_name] = parse(row[date_column_name], dayfirst=True, yearfirst=True).date()

                if row[country_column_name] == 'Russia':
                    plist.append(row)

            plist.sort(key=lambda x: x[date_column_name])
        csv_logger.info("Данные успешно получены.")
    except Exception as e:
        csv_logger.error(f"Произошла ошибка при чтении CSV-файла: {e}")
        return None

    return plist

def rename_keys(record, *cols):
    newcols = ['date', 'cases', 'deaths', 'country']
    for i in range(len(newcols)):
        record[newcols[i]] = record.pop(cols[i])
    return record


def save_csv_to_database(data_list, cursor):
    try:
        # Проверяем существование таблицы при открытии соединения
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

        # Итерируемся по данным
        for data in data_list:
            # Проверяем наличие дубликатов по дате
            cursor.execute('SELECT 1 FROM Covid_stats WHERE date = ?', (data['date'],))
            duplicate_exists = cursor.fetchone()

            if not duplicate_exists:
                cases = int(float(data['cases']))
                deaths = int(float(data['deaths']))
                # Добавляем данные, если нет дубликатов
                cursor.execute('''
                    INSERT INTO Covid_stats (date, country, cases, deaths, source)
                    VALUES (?, ?, ?, ?, 'from_csv')
                ''', (data['date'], data['country'], cases, deaths))
            else:
                csv_logger.warning(f"Дубликат данных для даты {data['date']} обнаружен. Пропускаем.")

        cursor.commit()
        csv_logger.info("Данные успешно добавлены.")

    except Exception as e:
        csv_logger.error(f"Произошла ошибка при сохранении данных в базу данных: {e}")

# cols must be in order like 'date', 'country', 'cases', 'deaths'
def add_csv_data_to_database(cursor, csv_filepath, *cols):
    try:
        # Загрузка данных из CSV
        date_column_name = cols[0]
        country_column_name = cols[1]
        other_columns = [cols[2], cols[3]]

        covid_data_csv = parse_dated_csvfile(csv_filepath, date_column_name, country_column_name, *other_columns)

        if covid_data_csv is not None:
            covid_data_csv = [rename_keys(rec, date_column_name, other_columns[0], other_columns[1], country_column_name) for rec in covid_data_csv]
            print(covid_data_csv)
            # Добавление данных в базу данных
            save_csv_to_database(covid_data_csv, cursor)

    except Exception as e:
        csv_logger.error(f"Произошла ошибка при обработке данных из CSV: {e}")