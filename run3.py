from pyspark import SparkContext
import re
import Constants
import run2


def mean_temp(rdd):

    # ("nation ; city", [t1,...,tn])
    temp = rdd \
        .flatMap(lambda l: generateTupleWithIndex(l, nat_cities, 1)) \
        .groupByKey() \
        .mapValues(lambda daily_temps: run2.correctErrors(list(daily_temps)))

    # ("nation ; city", mean temperature)
    return temp.map(lambda t: (t[0], sum(t[1]) / len(t[1])))


def generateTupleWithIndex(line, cities, tipoChiave):
    """
    genera tuple con la chiave che non contiene la data
    """

    mylist = []
    temperature = line.split(",")
    del temperature[0]

    i=0
    h = line[11:13]

    for city in cities:

        '''
            La nuova chiave è: 
                per la query 2 lo stato
                per la query 3 la citta e lo stato
        '''
        if tipoChiave == 1:
            k = city
        else:
            k = city.split(";")[0]

        try:
            temp = float(temperature[i].strip())
            ''' provo a recuperare il dato con virgola mancante
                ricostruito il dato controllo che sia in range 
            '''
            if temp > Constants.MAX_TEMPERATURE:
                #print("pre processing = "+str(temp))
                temp = temperature[i].strip()
                temp = temp[:3]+'.'+temp[4:]
                temp = float(temp)
                #print("after processing = " + str(temp))

            if temp < Constants.MAX_TEMPERATURE and temp > Constants.MIN_TEMPERATURE:
                v = temp
                t = (k, (h, v))
                mylist.append(t)



        except ValueError:
            print("error float conv")

        i = i+1

    return mylist





if __name__ == '__main__':

    sc = SparkContext("local", "Simple App")

    rawCSV = sc.textFile(Constants.TEMPERATURE_FILE)

    # remove header
    tempHeader = rawCSV.filter(lambda l: "datetime" in l).flatMap(lambda line: line.split(","))
    cities = tempHeader.collect()[1:]
    rawtemp = rawCSV.subtract(tempHeader)

    nat_cities = run2.generateKey(cities, sc)

    # get nations' list of the involved cities
    nations = []
    for nat_city in nat_cities:
        tempNation = nat_city.split(" ; ")[0]
        if tempNation not in nations:
            nations.append(tempNation)

    season = rawtemp.filter(lambda l: re.search('^2017-06|^2017-07|^2017-08|^2017-09', l))
    summerMeanTemp = mean_temp(season)  # ("nation ; city", 2017 summer mean temperature)
    # print("summer 2017 mean temperature per city: ", summerMeanTemp.take(10))

    season = rawtemp.filter(lambda l: re.search('^2017-01|^2017-02|^2017-03|^2017-04', l))
    winterMeanTemp = mean_temp(season)  # ("nation ; city", 2017 winter mean temperature)
    # print("winter 2017 mean temperature per city: ", winterMeanTemp.take(10))

    # ("nation ; city", 2017 summer-winter mean temperature difference)
    tempDiff = summerMeanTemp.join(winterMeanTemp).mapValues(lambda temps: abs(temps[0] - temps[1])).cache()
    # print("2017 temperature difference per city: ", tempDiff.take(10))

    season = rawtemp.filter(lambda l: re.search('^2016-06|^2017-07|^2017-08|^2017-09', l))
    summerMeanTemp = mean_temp(season)  # ("nation ; city", 2016 summer mean temperature)
    # print("summer 2016 mean temperature per city: ", summerMeanTemp.take(10))

    season = rawtemp.filter(lambda l: re.search('^2016-01|^2017-02|^2017-03|^2017-04', l))
    winterMeanTemp = mean_temp(season)  # ("nation ; city", 2017 winter mean temperature)
    # print("winter 2016 mean temperature per city: ", winterMeanTemp.take(10))

    # ("nation ; city", 2016 summer-winter mean temperature difference)
    prevTempDiff = summerMeanTemp.join(winterMeanTemp).mapValues(lambda temps: abs(temps[0] - temps[1])).cache()
    # print("2016 temperature difference per city: ", prevTempDiff.take(10))

    for nation in nations:
        nationTemp = tempDiff.filter(lambda value: nation in value[0])
        prevNationTemp = prevTempDiff.filter(lambda value: nation in value[0]) \
            .sortBy(keyfunc=lambda x: x[1], ascending=False) \
            .map(lambda x: x[0]) \
            .zipWithIndex()
        tempChart = nationTemp.join(prevNationTemp).takeOrdered(15, lambda x: -x[1][0])
        # TODO: ridurre l'rdd ai soli primi 15 elementi ordinati per alleggerire la join

        print(nation)
        for i, x in enumerate(tempChart):
            print("{}-{}: {}, (2016 position: {})"
                  .format(i + 1,
                          x[0].split(" ; ")[1],
                          x[1][0],
                          x[1][1] + 1))
        print("-----------------------------------------------------------------------\n")
