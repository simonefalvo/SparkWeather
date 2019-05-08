from pyspark import SparkContext
import statistics
import json
import Constants


def generateTupleWithIndex(line, cities):

    mylist = []
    date = line[0:10]
    temperature = line.split(",")
    del temperature[0]

    i=0
    h = line[11:13]

    for city in cities:

        k = city+' '+date
        '''
            non gestisco l'else poichè la casistica di errori non è prevedibili
                - valori senza virgola
                - valori con virgola ma cmq fuori range
                - valori fuori range 
        '''

        try:
            temp = float(temperature[i].strip())
            if temp < Constants.MAX_TEMPERATURE and temp > Constants.MIN_TEMPERATURE:
                v = temp
                t = (k, (h, v))
                mylist.append(t)

        except ValueError:
            print("error float conv")

        i = i+1

    return mylist

# _ | _ | 23 | 23 | _ | 24
def correctErrors(myList):
    #ordino i dati in base all'ora (chiave delle tuple)

    myList.sort()
   # print(myList)
    firstTime = True
    finalList = []

    # caso di lista con un solo elemento
    if len(myList) == 1:
        print("Unica misurazione")
        v = float(myList[0][1])
        for i in range(0,24):
            finalList.append(v)

    # o ho dati corretti oppure ho dei buchi
    else:
       for item in myList:

            if firstTime:
                s = item
                v = float(s[1])
                # se il primo elemento non e' dell'informazione oraria mezza notte propago il primo dato fino al primo ho
                if int(s[0]) > 0:
                    # propago il primo valore che ho nei buchi orari precedenti
                    #print("Propago valore "+str(int(s[0]))+" volte ")
                    for n in range(0, int(s[0]), 1):
                        # finalList.append((n, v))
                        finalList.append(v)

                # archivio il valore attuale nella lista finale
                finalList.append(float(s[1]))
                firstTime = False

            else:
                # l'info oraria che possiedo e' consecutiva
                v = float(item[1])
                if int(s[0])+1 == int(item[0]):
                    s = item
                    # finalList.append((int(item[0]), item[1]))
                    finalList.append(v)
                else:  # se gli elementi non sono consecutivi

                    v = (float(s[1]) + float(item[1]))/2
                    #print("Propago media " + str(int(item[0])-int(s[0]))+" volte ")
                    for n in range(int(s[0])+1, int(item[0]), 1):
                        # finalList.append((n, v))
                        finalList.append(v)

                    # finalList.append((int(item[0]), item[1]))
                    finalList.append(v)
                    s = item

    return finalList

''' la media, la deviazione standard, il minimo, il massimo '''


def computeStatisticsOfMonth(myList):

    monthStatistics = {
        "avg" : statistics.mean(myList),
        "max" : max(myList),
        "min" : min(myList),
        "std" : statistics.stdev(myList)
    }
    return monthStatistics



def main():
    ''' Temperature '''
    # Analiziamo statistica di ogni citta per poi aggregarle per stato

    sc = SparkContext("local", "Query 2")

    rddFileTemperature = sc.textFile(Constants.TEMPERATURE_FILE)

    ''' ottieni header del file, ovvero elenco citta di cui ho i dati '''
    temperatureHeader = rddFileTemperature.filter(lambda l: "datetime" in l).flatMap(lambda line: line.split(","))
    cites = temperatureHeader.collect()
    del cites[0]

    # calcola rdd con elenco di 12 temperature, eventualmente ricavate, di ogni giorno di ogni città
    temperature = rddFileTemperature. \
                                    subtract(temperatureHeader). \
                                    flatMap(lambda line: generateTupleWithIndex(line, cites)).\
                                    groupByKey(). \
                                    mapValues(list)

    print ("prima stampa")
    print(temperature.take(10))


    fixedTemperature = temperature.mapValues(correctErrors)

    print("seconda stampa")
    print(fixedTemperature.take(1))

    # raggruppa per citta e mese
    temperatureMonth = fixedTemperature.\
                                    map(lambda t: (t[0][:-3], t[1])).\
                                    reduceByKey(lambda x, y: x+y).\
                                    mapValues(list).\
                                    mapValues(computeStatisticsOfMonth)

    ''' todo: prima di computare le statistiche aggregare per nazione'''

    file = open("output.txt", "w")
    file.write(json.dumps(temperatureMonth.collect()))
    file.close()
    #print(temperatureMonth.collect())

if __name__ == '__main__':
    main()

