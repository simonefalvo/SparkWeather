from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import *
import Constants
import re
import run


def generateTuple(line, cities):
    '''
    :param line: csv line
    :param cities: csv header
    :return: a list of key-value pairs (city-yyyy-mm-dd, weather description)
    '''
    mylist = []
    date = line[0:15]               # yyyy-mm-dd
    descriptions = line.split(",")
    del descriptions[0]             # remove non-description info

    for idx, my_city in enumerate(cities):
        if descriptions[idx] == 'sky is clear':
            val = 1
        else:
            val = 0
        t = Row(city=my_city, year=date[0:4], month=date[5:7], day=date[8:10], h=date[11:13], sunny=val)
        mylist.append(t)

    return mylist






def main():

    sc = SparkContext("local", "Query 1")

    rawWeather, weatherHeader, cities = run.getRDDFromCSV(sc, Constants.WEATHER_DESCRIPTION_FILE)

    weatherDescription = rawWeather \
        .subtract(weatherHeader) \
        .filter(lambda l: re.search('^\d{4}-03|^\d{4}-04|^\d{4}-05', l))  # month filter

    daysOfMonth = weatherDescription \
        .flatMap(lambda line: generateTuple(line, cities))

    sqlc = SQLContext(sc)

    df = sqlc.createDataFrame(daysOfMonth)
    df.show()
    df.createOrReplaceTempView("dati")
    query1 = "SELECT city, year, month, day, sum(sunny) as n_sunny_h FROM dati GROUP BY city, year, month, day"
    df2 = sqlc.sql(query1)
    df2.show()

    #applicazione regola sunny day (75%)
    df2.createOrReplaceTempView("dati")
    query2 = "SELECT city, year, month, day FROM dati where n_sunny_h >13"
    df3 = sqlc.sql(query2)
    df3.show()

    #almeno 15 gg sereno al mese
    df3.createOrReplaceTempView("dati")
    query2 = "SELECT city, year, month, count(*) AS n_day FROM dati GROUP BY year,city, month"
    df4 = sqlc.sql(query2)
    df4.show()

    #filtra n_giorni
    df4.createOrReplaceTempView("dati")
    query2 = "SELECT city, year, month, n_day FROM dati where n_day>=15"
    df5 = sqlc.sql(query2)
    df5.show()

    # filtra n mesi = 3
    df5.createOrReplaceTempView("dati")
    query2 = "SELECT city, year, count(*) AS n_month FROM dati GROUP BY city, year "
    df6 = sqlc.sql(query2)
    df6.show()

    df6.createOrReplaceTempView("dati")
    query2 = "SELECT city, year FROM dati WHERE n_month = 3 "
    df7 = sqlc.sql(query2)
    df7.show()

if __name__ == '__main__':
    main()
