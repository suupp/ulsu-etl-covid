from flask import Flask, jsonify, request
import pyodbc

app = Flask(__name__)

# Database connection parameters
server = 'serv name'
database = 'db name'
username = 'your_username'
password = 'your_password'  #можно без него по идее, добавить trusted connection в строке подключения к базке
driver= '{ODBC Driver 17 for SQL Server}'

# Database connection
conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)

# API routes
@app.route('/covid-data', methods=['GET'])
def get_covid_data():
    cursor = conn.cursor()
    
    # Get request parameters for filtering
    country = request.args.get('country')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    # Construct query based on filters
    query = "SELECT * FROM covid_data"
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
            'date': row['date'],
            'location': row['country'],
            'cases': row['cases'],
            'deaths': row['deaths']
        })
    
    return jsonify(result)

app.run(debug=True)
#if __name__ == '__main__':
#    app.run(debug=True)