from csvparserpkg.csvparser import parse_dated_csvfile
from csvparserpkg.csvparser import rename_keys


# Sample
a = parse_dated_csvfile('data.csv', "dateRep", 'countriesAndTerritories', 'cases', 'deaths')
a = list(map(rename_keys, a))
print(a[:2])
# /Sample