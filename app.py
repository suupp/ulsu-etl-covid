from flask import Flask, jsonify, request
from config import API_KEYS
from functools import wraps
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
    cursor = conn.cursor()
    
    # Get request parameters for filtering
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    min_cases = request.args.get('min_cases')
    max_cases = request.args.get('max_cases')
    min_deaths = request.args.get('min_deaths')
    max_deaths = request.args.get('max_deaths')
    # Construct query based on filters
    query = "SELECT * FROM Covid_stats"
    filters = []
    if start_date:
        filters.append(f"DATE >= '{start_date}'")
    if end_date:
        filters.append(f"DATE <= '{end_date}'")
    if min_cases:
        filters.append(f"CASES >= '{min_cases}'")
    if max_cases:
        filters.append(f"CASES <= '{max_cases}'")
    if min_deaths:
        filters.append(f"DEATHS >= '{min_deaths}'")
    if max_deaths:
        filters.append(f"DEATHS <= '{max_deaths}'")
    
    if filters:
        query += " WHERE " + " AND ".join(filters)
    
    # Execute query
    cursor.execute(query)
    data = cursor.fetchall()
    
    # Convert data to JSON format
    result = []
    for row in data:
        result.append({
            'id': row.id,
            'date': row.date.strftime('%Y-%m-%d'),
            'location': row.country,
            'cases': row.cases,
            'deaths': row.deaths
        })
    
    return jsonify(result)

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
#if __name__ == '__main__':
#    app.run(debug=True)