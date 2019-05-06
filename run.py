from pyspark import SparkContext
import datetime
import re
import Constants

# prendere citta+anno che diviene chiave
# valore e' il campo description
# TODO: controllare come si comporta con i campi vuoti
def generateTuple(line, cities):
    mylist = []
    date = line[0:10]
    descriptions = line.split(",")
    del descriptions[0]
    i=0

    for city in cities:

        k = city+' '+date
        t = (k, descriptions[i])
        mylist.append(t)
        i = i+1

    return mylist

def evaluateDay(elementList):
    k, vList = elementList
    #la nuova chiave è citta anno mese
    n_k = k[:-3]
    sky_clear = 0
    #sky_not_clear = 0

    #la regola di validazione della giornata va inserita qui
    for description in vList:
        if description == 'sky is clear':
            sky_clear += 1
        #else:
         #   sky_not_clear += 1

    if sky_clear / len(vList) >= 0.75:
        sky_clear = 1
    else:
        sky_clear = 0

    return n_k, sky_clear
    # return k, [sky_clear, sky_not_clear]

sc = SparkContext("local", "Simple App")

print(datetime.datetime.now())


rawWeather = sc.textFile(Constants.WEATHER_DESCRIPTION_FILE)

# Header RDD
weatherHeader = rawWeather.filter(lambda l: "datetime" in l).flatMap(lambda line: line.split(","))
cites = weatherHeader.collect()
del cites[0]

# elimina dall'rdd l'header e filtra i dati di interesse in base ai mesi

weatherDescription = rawWeather.\
                        subtract(weatherHeader).\
                        filter(lambda l: re.search('^\d{4}-03|^\d{4}-04|^\d{4}-05', l))
print(weatherDescription.take(5))

#gli elementi dell'rdd sono una tupla di stringa, lista ('Philadelphia 2013-04-01', 'sky is clear, scattered clouds ecc')

daysOfMonthHaveSkyClear= weatherDescription.flatMap(lambda line: generateTuple(line, cites)).\
                         groupByKey().mapValues(list).map(evaluateDay)

#print(daysOfMonthHaveSkyClear.take(15))

resultQuery = daysOfMonthHaveSkyClear.reduceByKey(lambda x, y: x+y).\
                            filter(lambda t: t[1] >= 15)

#print(resultQuery.take(15))

#(citta anno-mese, #gg sky_is_clear) -> (citta anno, mese) -> (citta_anno, [ elenco mesi] )
#  -> elenco citta tre mesi sky clear -> elenco citta anno
printableResult = resultQuery.map(lambda t: (t[0][:-3], t[0][-2:])).groupByKey().mapValues(list).\
                        filter(lambda t: len(t[1]) == 3).map(lambda t: t[0]).collect()

print(printableResult)
print(datetime.datetime.now())

