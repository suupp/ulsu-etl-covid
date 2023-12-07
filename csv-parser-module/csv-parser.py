import csv
arr = []
with open("csv-parser-module/hw_200.csv", newline = '') as csvfile:
    read = csv.reader(csvfile, delimiter=',', quotechar='|')
    for row in read:
        row = list(map(lambda x: x.strip(), row))
        arr.append(tuple(map(lambda e: float(e) if e.replace('.','',1).isnumeric() else e, row)))
print(arr)