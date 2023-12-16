from flask import Flask, jsonify, request
import pyodbc

app = Flask(__name__)

 # Параметры подключения к SQL Server
server = r'MY-NOTEBOOK-00\SQLEXPRESS'
database = 'CovidData'
username = 'MY-NOTEBOOK-00\Я'

# Строка подключения
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};Trusted_Connection=yes;'
conn = pyodbc.connect(connection_string)

# API routes
@app.route('/covid-data', methods=['GET'])
def get_covid_data():
    cursor = conn.cursor()
    
    # Get request parameters for filtering
    country = request.args.get('country')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    # Construct query based on filters
    query = "SELECT * FROM Covid_stats"
    filters = []
    if country:
        filters.append(f"country = '{country}'")
    if start_date:
        filters.append(f"date >= '{start_date}'")
    if end_date:
        filters.append(f"date <= '{end_date}'")
    
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