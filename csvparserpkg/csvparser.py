import csv
from datetime import datetime

def columns_to_parse(*args):
    return args

# Use this function to parse practically any .csv file that has date and country column column in it
# Dates automatically formatted into datetime.date(y, m, d), also function appends only records 
# where country is Russia:)
def parse_dated_csvfile(filepath, date_column_name, country_column_name, *other_columns):
    plist = []
    with open(filepath, newline = '') as csvfile:
        read = csv.DictReader(csvfile, delimiter=',', quotechar='|')
        for row in read:
            for k, v in row.items():
                try:
                    v.isnumeric()
                except:
                    #print(v, 'badtype', type(v), end = " !! ")
                    row[k] = v[0]
            row = {k: float(v) if v.isnumeric() else v for k, v in row.items()}
            good_keys = [date_column_name, country_column_name, *other_columns]
            bad_keys = [key for key in row.keys() if key not in good_keys]
            for key in bad_keys:
                del row[key]
            row[date_column_name] = datetime.strptime(row[date_column_name], "%d/%m/%Y").date()
            if row[country_column_name] == 'Russia': plist.append(row)
            #rename_keys(row)
    return plist


def rename_keys(record):
    record['date'] = record.pop('dateRep')
    record['cases'] = record.pop('cases')
    record['deaths'] = record.pop('deaths')
    record['country'] = record.pop('countriesAndTerritories')
    return record

#print(parse_dated_csvfile('csv-parser-module/data.csv', "dateRep", 'countriesAndTerritories', 'cases', 'deaths')[:2])