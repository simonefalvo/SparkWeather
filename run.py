from pyspark import SparkContext
from pyspark.sql import SparkSession
import datetime
import time
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

    for idx, city in enumerate(cities):
        t = (city + ' ' + date, descriptions[idx])
        mylist.append(t)

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

def getRDDFromCSV(sc, nameFile):

    fileRDD = sc.textFile(nameFile)

    # Header RDD
    weatherHeader = fileRDD.filter(lambda l: "datetime" in l)
    cities = weatherHeader.flatMap(lambda line: line.split(",")).collect()
    del cities[0]

    rawWeather = fileRDD.subtract(weatherHeader)

    return rawWeather, weatherHeader, cities


def main():

    sc = SparkContext("local", "Simple App")

    print(datetime.datetime.now())

    rawWeather, weatherHeader, cities = getRDDFromCSV(sc, Constants.WEATHER_DESCRIPTION_FILE)


    '''
        @input: rdd contenente le righe del file csv
        @output: rdd contenente le righe del file csv dei soli mesi di interesse
         elimina dall'rdd l'header e filtra i dati di interesse in base ai mesi   
    '''
    weatherDescription = rawWeather \
                        .subtract(weatherHeader) \
                        .filter(lambda l: re.search('^\d{4}-03|^\d{4}-04|^\d{4}-05', l)) # month filter


    '''
        @input: rdd contenente le righe del file csv dei soli mesi di interesse
        @output: genera per ogni rdd più tuple (citta anno-mese, weather desript)        
        
        gli elementi dell'rdd sono una tupla di stringa, lista del tipo
          ('Philadelphia 2013-04-01', 'sky is clear, scattered clouds ecc')
        
        
    '''

    daysOfMonth = weatherDescription \
                 .flatMap(lambda line: generateTuple(line, cities)) \


    # print("after process month data: ", daysOfMonth.take(15))

    '''
        @input: tuple del tipo (citta anno-mese, weather description)
        @output: tuple del tipo (citta anno-mese, num di giorni valutati come sereni)
        
        transform each value into a list
        combine lists: ([1,2,3] + [4,5]) becomes [1,2,3,4,5]7
        In questo modo evitiamo una groupByKey meno efficente
    '''

    daysOfMonthWithWeather = daysOfMonth \
                            .map(lambda nameTuple: (nameTuple[0], [nameTuple[1]])) \
                            .reduceByKey(lambda a, b: a + b)


    daysOfMonthHaveSkyClear = daysOfMonthWithWeather \
                            .map(evaluateDay)

    # print("after evaluate day: ", daysOfMonthHaveSkyClear.take(12))



    '''
        @input: tuple del tipo (citta anno-mese, num di giorni valutati come sereni)
        @output: restituisce le citta che hanno piu di 15 gg del dato mese di tempo sereno
    '''
    resultQuery = daysOfMonthHaveSkyClear\
                .reduceByKey(lambda x, y: x + y) \
                .filter(lambda t: t[1] >= 15)

    # print("after treshold filter: ", resultQuery.take(15))

    '''
        @input: tuple del tipo (citta anno-mese, num di giorni valutati come sereni >15)
        @output: tuple del tipo (anno, lista di citta che hanno registrato nei 3 mesi almeno 15 giorni sereni)
        
        Trasforma le coppie (Citta anno-mese, #ggSereno) in (Citta Anno, [mese] )
        Riduce per chiave (citta anno, lista mesi )
        Seleziona le coppie che hanno 3 mesi nella lista
        Genera le coppie (anno, [citta]) e li riduce per chiave
        Infine li ordina per chiave (anno) e li stampa
        
    '''
    result = resultQuery.map(lambda t: (t[0][:-3], [ t[0][-2:] ])).reduceByKey(lambda a, b: a + b) \
        .filter(lambda t: len(t[1]) == 3).map(lambda t: (t[0][-4:], [t[0][:-5]] )).reduceByKey(lambda a, b: a + b) \
        .sortByKey()

    '''
           Save data in HDFS
    '''
    current_milli_time = int(round(time.time() * 1000))
    path_out = Constants.QUERY1_OUTPUT_FILE + str(current_milli_time) + ".json"

    spark = SparkSession.builder.appName('print').getOrCreate()
    df = spark.createDataFrame(result, ['year', 'cities'])
    df.coalesce(1).write.format("json").save(path_out)

    print(result.collect())
    print(datetime.datetime.now())


if __name__ == '__main__':
    main()
