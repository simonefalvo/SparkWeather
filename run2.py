from pyspark import SparkContext
import statistics
import json
import datetime
import Constants
import utils.locutils

debug = False

'''
    @input: spark context
    @output: preleva le citta dal file e genera lista di elementi del tipo: Stato ; Citta 
'''


def generateStateFromCity(sc):

    cityDatas = sc.textFile(Constants.CITY_ATTRIBUTES_FILE)
    header = cityDatas.filter(lambda l: "Latitude" in l)

    cityInfo = cityDatas\
        .subtract(header)\
        .map(lambda line: (line.split(",")[0], line.split(",")[1:]) )\
        .map(lambda myTuple: utils.locutils.country(float(myTuple[1][0]), float(myTuple[1][1]))
                             + " ; " + myTuple[0]
                             + " ; " + utils.locutils.get_timezone(float(myTuple[1][0]), float(myTuple[1][1])))



    cityInfoD = cityInfo.collect()
    return cityInfoD


'''
    @input: Riga del file csv, 
            lista delle citta, nel formato "Nazione ; Città"
            tipoChiave, specifica se nell'output si vuole solo la città (valore 1) o la nazione e la citta
    
    @output: lista di tuple del tipo (chiave, (ora, temperatura))
            Chiave a seconda di tipoChiave sarà     "Nazione ; Citta Data"
                                           oppure   "Citta Data"
    
    Durante il processo si cerca di correggere eventuali errori nel campo temperatura (inserimento punto) e 
    si validano controllando che rientrino in un range di validità 

'''
def generateTupleWithHoursAndCorrectData(line, cities, tipoChiave, val_max, val_min, dotPosition, utc_convert):

    mylist = []
    date = line[0:19]
    temperature = line.split(",")
    del temperature[0]
    i = 0

    for city in cities:
        if utc_convert:
            date_offset = utils.locutils.convert_timezone(date, city.split(";")[2])
            h = date_offset[11:13]
            date_offset = date_offset[0:10]
        else:
            h = date[11:13]
            date_offset = date[0:10]

        if tipoChiave == 1:
            k = city+' '+date_offset
        else: # """ probabilmente non lo usero mai """
            k = city.split(";")[0]+' '+date_offset

        try:
            temp = float(temperature[i].strip())
            # print("temperatura da dataset = " + str(temp))

            # provo a recuperare il dato con virgola mancante
            if temp > val_max:
                tmp = temperature[i].strip()
                tmp = tmp[:dotPosition]+'.'+tmp[(dotPosition+1):]
                temp = float(tmp)

            if temp in range(val_min, val_max):
                t = (k, [(h, temp)])
                mylist.append(t)

            i = i + 1

        except ValueError:
            i = i + 1
            #print("error converting in float:"+temperature[i]+".")

    return mylist


# _ | _ | 23 | 23 | _ | 24
def fillEmptyData(my_list):

    N = 24

    my_list = [(int(k), v) for k, v in my_list]
    my_list.sort()

    final_list = []

    k_prev, v_prev = my_list[0][0] - 1, my_list[0][1]   # go to if k == k_prev + 1: at first run

    # lost first data
    if my_list[0][0] != 0:
        for i in range(0, k_prev+1):
            final_list.append(v_prev)

    for my_tuple in my_list:
        k, v = my_tuple

        if k == k_prev + 1:  # consecutive data
            final_list.append(v)

        if k > k_prev + 1:  # no consecutive data
            avg = (v_prev + v)/2
            for i in range(k_prev, k-1):
                final_list.append(avg)

            final_list.append(v)

        if k == my_list[-1][0] and k < N:  # lost last data
            for i in range(k+1, N):
                final_list.append(v)

        k_prev, v_prev = k, v

    return final_list


def computeStatisticsOfMonth(myList):

    monthStatistics = {
        "avg": statistics.mean(myList),
        "max": max(myList),
        "min": min(myList),
        "std": statistics.stdev(myList)
    }
    return monthStatistics


def query2(sc, file, val_max, val_min, dotPosition):

    rddFileData = sc.textFile(file)

    ''' ottieni header del file, ovvero elenco citta  '''
    dataHeader = rddFileData\
        .filter(lambda l: "datetime" in l)

    cites = generateStateFromCity(sc)


    ''''
        @input: file intero in formato RDD
        @output: tuple del tipo ( Stato ; Citta aaaa-mm-gg , [(ora, temperatura)...]
        
        calcola rdd con elenco delle temperature (al piu 12) di ogni giorno di ogni città
        
    '''

    data = rddFileData \
        .subtract(dataHeader) \
        .flatMap(lambda line: generateTupleWithHoursAndCorrectData(line, cites, 1,
                                                                   val_max, val_min, dotPosition, True)) \
        .reduceByKey(lambda x, y: x+y)\

    '''
                                    reduceByKey(lambda x,y: x+y ).\
                                    sortByKey()

    '''
    if debug:
        print("prima stampa")
        print(data.take(10))
        print("fine prima stampa")

    '''
        Inserisce le misurazioni orarie giornaliere mancanti        
    '''

    fixedData = data.mapValues(fillEmptyData)

    if debug:
        print("seconda stampa")
        print(fixedData.take(2))
        print("fine seconda stampa")

    '''
        @Input: Tuple del tipo (Stato ; citta aaa-mm-gg, Liste di temperature
        @Output: Per ogni stato, per ogni mese tuple del tipo
                 (Stato aaaa-mm , statistiche)

        Con la map genero le nuove chiavi lasciando inalterato il contenuto
    '''
    # raggruppa per stato e mese
    # TODO: togliere sortbykey, mi serve in fase di test per avere output ordinato
    dataMonth = fixedData \
        .map(lambda t: (t[0].split(";")[0] + t[0].split(";")[1][-10:-3], t[1])) \
        .reduceByKey(lambda x, y: x + y) \
        .mapValues(computeStatisticsOfMonth) # \
        #.sortByKey()

    result = dataMonth.collect()
    if debug:
        print("terza stampa")
        print(result)

    return result


def main():

    sc = SparkContext("local", "Query 2")
    start = datetime.datetime.now()
    print(start)

    resultTemp = query2(sc,
                        Constants.TEMPERATURE_FILE,
                        Constants.MAX_TEMPERATURE,
                        Constants.MIN_TEMPERATURE,
                        Constants.POINT_WHERE_CUT_TEMP)

    file = open("output_temp.json", "w")
    file.write(json.dumps(resultTemp))
    file.close()
    del resultTemp

    resultHum = query2(sc,
                       Constants.HUMIDITY_FILE,
                       Constants.MAX_HUMIDITY,
                       Constants.MIN_HUMIDITY,
                       Constants.POINT_WHERE_CUT_HUM)

    file = open("output_hum.json", "w")
    file.write(json.dumps(resultHum))
    file.close()
    del resultHum

    resultPress = query2(sc,
                         Constants.PRESSURE_FILE,
                         Constants.MAX_PRESSURE,
                         Constants.MIN_PRESSURE,
                         Constants.POINT_WHERE_CUT_PRES)



    file = open("output_press.json", "w")
    file.write(json.dumps(resultPress))
    file.close()
    del resultPress

    end = datetime.datetime.now()
    print(end)
    print(end-start)


if __name__ == '__main__':
    main()

