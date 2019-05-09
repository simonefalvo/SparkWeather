from pyspark import SparkContext
import datetime
import re
import Constants


# TODO: controllare come si comporta con i campi vuoti
def generateTuple(line, cities):
    '''
    :param line: csv line
    :param cities: csv header
    :return: a list of key-value pairs (city-yyyy-mm-dd, weather description)
    '''
    mylist = []
    date = line[0:10]               # yyyy-mm-dd
    descriptions = line.split(",")
    del descriptions[0]             # remove non-description info

    i=0
    for city in cities:
        t = (city + ' ' + date, descriptions[i])
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




def main():

    sc = SparkContext("local", "Simple App")

    print(datetime.datetime.now())

    '''  read data '''

    rawWeather= sc.textFile(Constants.WEATHER_DESCRIPTION_FILE)

    # Header RDD
    weatherHeader = rawWeather.filter(lambda l: "datetime" in l).flatMap(lambda line: line.split(","))
    cities = weatherHeader.collect()
    del cities[0]

    '''
        @input: rdd contenente le righe del file csv
        @output: rdd contenente le righe del file csv dei soli mesi di interesse
         elimina dall'rdd l'header e filtra i dati di interesse in base ai mesi   
    '''
    weatherDescription = rawWeather \
                        .subtract(weatherHeader) \
                        .filter(lambda l: re.search('^\d{4}-03|^\d{4}-04|^\d{4}-05', l)) # month filter

    print("after month filter: ", weatherDescription.take(5))

    '''
        @input: rdd contenente le righe del file csv dei soli mesi di interesse
        @output: genera per ogni rdd più tuple (citta anno-mese, weather desript)        
    '''
    # gli elementi dell'rdd sono una tupla di
    # stringa, lista del tipo  ('Philadelphia 2013-04-01', 'sky is clear, scattered clouds ecc')
    daysOfMonth = weatherDescription \
                 .flatMap(lambda line: generateTuple(line, cities)) \


    print("after process month data: ", daysOfMonth.take(15))

    '''
        @input: tuple del tipo (citta anno-mese, weather description)
        @output: tuple del tipo (citta anno-mese, num di giorni valutati come sereni)
    '''
    daysOfMonthHaveSkyClear = daysOfMonth \
                             .groupByKey()\
                             .mapValues(list).map(evaluateDay)

    print("after evaluate day: ", daysOfMonthHaveSkyClear.take(15))

    '''
        @input: tuple del tipo (citta anno-mese, num di giorni valutati come sereni)
        @output: restituisce le citta che hanno piu di 15 gg del dato mese di tempo sereno
    '''
    resultQuery = daysOfMonthHaveSkyClear\
                .reduceByKey(lambda x, y: x + y) \
                .filter(lambda t: t[1] >= 15)

    print("after treshold filter: ", resultQuery.take(15))

    # (city yyyy-mm, #gg sky_is_clear) -> (city yyyy, mm) -> (citta_anno, [ elenco mesi] )
    #  -> elenco citta tre mesi sky clear -> elenco citta anno
    '''
        @input: tuple del tipo (citta anno-mese, num di giorni valutati come sereni >15)
        @output: tuple del tipo (anno, lista di citta che hanno registrato nei 3 mesi almeno 15 giorni sereni)
    '''
    printableResult = resultQuery.map(lambda t: (t[0][:-3], t[0][-2:])).groupByKey().mapValues(list) \
        .filter(lambda t: len(t[1]) == 3).map(lambda t: (t[0][-4:], t[0][:-5])).groupByKey().mapValues(list) \
        .sortByKey().collect()

    print(printableResult)
    print(datetime.datetime.now())


if __name__ == '__main__':
    main()
