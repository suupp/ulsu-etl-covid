from flask import Flask, jsonify, request, render_template
from config import API_KEYS
from functools import wraps
from utils.utils import get_covid_data_util
import pyodbc

app = Flask(__name__)

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
            return view_function(*args, **kwargs)
        else:
            return jsonify({'error': 'Unauthorized'}), 401
    return decorated_function


# API routes
@app.route('/covid-data', methods=['GET'])
@require_api_key
def get_covid_data():
    return jsonify(get_covid_data_util())


@app.route('/covid-data-page', methods=['GET'])
def get_covid_data_html():
    data_records = get_covid_data_util()
    if data_records:
        return render_template('records.html', records=data_records, colnames=data_records[0].keys())
    else:
        return jsonify({'error': 'No data'})
    
# Новый маршрут для получения данных о конкретном элементе по ID
@app.route('/covid-data/<int:item_id>', methods=['GET'])
def get_covid_data_by_id(item_id):
    cursor = conn.cursor()
    
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
        return jsonify(result)
    else:
        return jsonify({'error': 'Item not found'}), 404

app.run(debug=True)
