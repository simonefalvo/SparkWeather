from pyspark import SparkContext
from pyspark.sql import SparkSession
import statistics
import datetime
import Constants
import time
import common.weather as weather
import json

debug = False


def get_position(header):

    header_str = str(header.collect())
    # rimuovo le quadre a inizio e fine stringa
    header_str = header_str[1:]
    header_str = header_str[:-2]
    my_list = header_str.split(",")

    del my_list[0]

    my_map = {}
    i = 0

    for city in my_list:
        city = str(city).strip().replace("_", " ")
        my_map[city] = i
        i += 1
    return my_map


def compute_statistics_of_month(my_list):

    month_statistics = {
        'avg': statistics.mean(my_list),
        'max': max(my_list),
        'min': min(my_list),
        'std': statistics.stdev(my_list)
    }
    return month_statistics


def query2(sc, file_in_name, file_out_name):

    rdd_file_data = sc.textFile(file_in_name)

    ''' ottieni header del file, ovvero elenco citta  '''
    data_header = rdd_file_data\
        .filter(lambda l: "datetime" in l)

    cites = weather.gen_city_keys(sc)

    header_position = get_position(data_header)


    ''''
        @input: file intero in formato RDD
        @output: tuple del tipo ( Stato ; Citta aaaa-mm-gg , [(ora, temperatura)...]
        
        calcola rdd con elenco delle temperature (al piu 12) di ogni giorno di ogni città
        
    '''
    #TODO: togliere sortByKey
    data = rdd_file_data \
        .subtract(data_header) \
        .flatMap(lambda line: weather.hourly_temps(header_position, line, cites, add_date=True, convert_utc=False)) \
        .reduceByKey(lambda x, y: x+y)\
        .sortByKey()

    if debug:
        print("prima stampa")
        print(data.top(20))
        print("fine prima stampa")

    '''
        @Input: Tuple del tipo (Stato ; citta aaa-mm-gg, Liste di temperature
        @Output: Per ogni stato, per ogni mese tuple del tipo
                 (Stato aaaa-mm , statistiche)

        Con la map genero le nuove chiavi lasciando inalterato il contenuto
    '''
    # raggruppa per stato e mese
    # TODO: togliere sortbykey, mi serve in fase di test per avere output ordinato
    data_month = data \
        .map(lambda t: (t[0].split(";")[0] + t[0].split(";")[2][-10:-3], t[1]))\
        .sortByKey()

    if debug:
        print("seconda stampa")
        print(data_month.top(20))
        print("fine seconda stampa")

    statistics_data = data_month\
        .reduceByKey(lambda x, y: x + y) \
        .mapValues(compute_statistics_of_month)\
        .sortByKey()

    if debug:
        result = json.dumps(statistics_data.collect())
        print("terza stampa")
        print(result)

    '''
        Save data in HDFS
    '''
    spark = SparkSession.builder.appName('print').getOrCreate()
    df = spark.createDataFrame(statistics_data, ['ID', 'value'])
    df.coalesce(1).write.format("json").save(file_out_name)



def main():

    sc = SparkContext("local", "Query 2")
    start = datetime.datetime.now()
    print(start)

    current_milli_time = int(round(time.time() * 1000))

    path_out = Constants.TEMPERATURE_QUERY2_OUTPUT_FILE+str(current_milli_time)+".json"
    query2(sc,
           Constants.TEMPERATURE_FILE,
           path_out)

    current_milli_time = int(round(time.time() * 1000))
    path_out = Constants.HUMIDITY_QUERY2_OUTPUT_FILE + str(current_milli_time) + ".json"

    query2(sc, Constants.HUMIDITY_FILE, path_out)

    current_milli_time = int(round(time.time() * 1000))
    path_out = Constants.PRESSURE_QUERY2_OUTPUT_FILE + str(current_milli_time) + ".json"

    query2(sc, Constants.PRESSURE_FILE, path_out)

    end = datetime.datetime.now()
    print(end)
    print(end-start)


if __name__ == '__main__':
    main()

