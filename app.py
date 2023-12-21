from flask import Flask, jsonify, request, render_template
from config import API_KEYS
from functools import wraps
from utils.utils import get_covid_data_util
import pyodbc
import logging

app = Flask(__name__)

# Конфигурация логгера для Flask-приложения
logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


 # Параметры подключения к SQL Server
server = r'MY-NOTEBOOK-00\SQLEXPRESS'
database = 'CovidData'
username = 'MY-NOTEBOOK-00\Я'

# Строка подключения
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};Trusted_Connection=yes;'
conn = pyodbc.connect(connection_string)

def require_api_key(view_function):
    @wraps(view_function)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('Api-Key')
        if api_key and api_key in API_KEYS.values():
            # Логирование успешного доступа
            logging.info(f'Successful access with API key: {api_key}')
            return view_function(*args, **kwargs)
        else:
            error_message = 'Unauthorized access'
            logging.warning(error_message)
            return jsonify({'error': error_message}), 401
    return decorated_function


# API routes
@app.route('/covid-data', methods=['GET'])
@require_api_key
def get_covid_data():
    try:
        data_records = get_covid_data_util()
        if data_records:
            # Логирование успешного получения данных
            logging.info('Successfully retrieved COVID data.')
            return jsonify(data_records)
        else:
            # Логирование отсутствия данных
            logging.warning('No COVID data available.')
            return jsonify({'error': 'No data'})
    except Exception as e:
        # Логирование ошибки
        error_message = f'Error : {e}'
        logging.error(error_message)
        return jsonify({'error': error_message}), 500

# API routes
@app.route('/covid-data-page', methods=['GET'])
def get_covid_data_html():
    try:
        data_records = get_covid_data_util()
        if data_records:
            logging.info('HTML page successfully rendered with data.')
            return render_template('records.html', records=data_records, colnames=data_records[0].keys())
        else:
            logging.warning('No data available.')
            return jsonify({'error': 'No data'})
    except Exception as e:
        error_message = f'Error rendering HTML page: {e}'
        logging.error(error_message)
        return jsonify({'error': error_message}), 500
    
# Новый маршрут для получения данных о конкретном элементе по ID
@app.route('/covid-data/<int:item_id>', methods=['GET'])
def get_covid_data_by_id(item_id):
    cursor = conn.cursor()
    
    try:
        # Подготовка и выполнение запроса
        query = f"SELECT * FROM Covid_stats WHERE id = {item_id}"
        cursor.execute(query)
        data = cursor.fetchone()

        # Проверка наличия данных
        if data:
            result = {
                'id': data.id,
                'date': data.date.strftime('%Y-%m-%d'),
                'location': data.country,
                'cases': data.cases,
                'deaths': data.deaths
            }
            logging.info(f'Successfully retrieved data for item with ID {item_id}.')
            return jsonify(result)
        else:
            error_message = f'Item with ID {item_id} not found.'
            logging.warning(error_message)
            return jsonify({'error': error_message}), 404
    except Exception as e:
        error_message = f'Error retrieving data for item with ID {item_id}: {e}'
        logging.error(error_message)
        return jsonify({'error': error_message}), 500
    
app.run(debug=True)
