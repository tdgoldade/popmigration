import csv
with open('countyinflow1011.csv', 'rb') as csvfile:
    inflowreader = csv.reader(csvfile, delimiter=',')
    for row in inflowreader:
        print row

