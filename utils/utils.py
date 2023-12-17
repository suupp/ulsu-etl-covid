from flask import request, jsonify
import pyodbc

 # Параметры подключения к SQL Server
server = r'MY-NOTEBOOK-00\SQLEXPRESS'
database = 'CovidData'
username = 'MY-NOTEBOOK-00\Я'

# Строка подключения
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};Trusted_Connection=yes;'
conn = pyodbc.connect(connection_string)

def get_covid_data_util():
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
    
    return result