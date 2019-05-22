from pyspark import SparkContext
import statistics
import datetime
import Constants
import utils.locutils
import time

debug = True

'''
    @input: spark context
    @output: preleva le citta dal file e genera lista di elementi del tipo: Stato ; Citta 
'''


def generate_state_from_city(sc):
    """
    Read the city coordinates file, remove the header and for each city
    coordinates couple generate the related country and timezone in the format: 'country ; city ; timezone',
    for example 'United States ; Portland ; America/Los_Angeles'.
    :param sc: spark context
    :return: a string 'country ; city ; timezone'
    """

    city_data = sc.textFile(Constants.CITY_ATTRIBUTES_FILE)
    header = city_data.filter(lambda line: "Latitude" in line)

    city_info = city_data \
        .subtract(header) \
        .map(lambda line: (line.split(",")[0], line.split(",")[1:])) \
        .map(lambda my_tuple: utils.locutils.country(float(my_tuple[1][0]), float(my_tuple[1][1]))
             + " ; " + my_tuple[0]
             + " ; " + utils.locutils.get_timezone(float(my_tuple[1][0]), float(my_tuple[1][1])))

    city_info_d = city_info.collect()

    return city_info_d


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


def generate_tuple_with_hours_utc(line, cities, key_type, val_max, val_min, utc_convert):

    my_list = []
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

        if key_type == 1:
            k = city+' '+date_offset
        else:  # """ probabilmente non lo usero mai """
            k = city.split(";")[0]+' '+date_offset

        try:
            temp = float(temperature[i].strip())
            # print("temperatura da dataset = " + str(temp))

            if temp in range(val_min, val_max):
                # t = (k, [(h, temp)])
                t = (k, [temp])
                my_list.append(t)

            i = i + 1

        except ValueError:
            i = i + 1
            #print("error converting in float:"+temperature[i]+".")

    return my_list


def compute_statistics_of_month(my_list):

    month_statistics = {
        "avg": statistics.mean(my_list),
        "max": max(my_list),
        "min": min(my_list),
        "std": statistics.stdev(my_list)
    }
    return month_statistics


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





def query2(sc, file_in_name, val_max, val_min, file_out_name):

    rdd_file_data = sc.textFile(file_in_name)

    ''' ottieni header del file, ovvero elenco citta  '''
    data_header = rdd_file_data\
        .filter(lambda l: "datetime" in l)

    cites = generate_state_from_city(sc)

    ''''
        @input: file intero in formato RDD
        @output: tuple del tipo ( Stato ; Citta aaaa-mm-gg , [(ora, temperatura)...]
        
        calcola rdd con elenco delle temperature (al piu 12) di ogni giorno di ogni città
        
    '''

    data = rdd_file_data \
        .subtract(data_header) \
        .flatMap(lambda line: generate_tuple_with_hours_utc(line, cites, 1, val_max, val_min, True)) \
        .reduceByKey(lambda x, y: x+y)
    '''
                                    reduceByKey(lambda x,y: x+y ).\
                                    sortByKey()

    '''
    if debug:
        print("prima stampa")
        print(data.take(10))
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
        .map(lambda t: (t[0].split(";")[0] + t[0].split(";")[2][-10:-3], t[1]))
    print("seconda stampa")
    print(data_month.take(10))
    print("fine seconda stampa")

    statistics_data = data_month\
        .reduceByKey(lambda x, y: x + y) \
        .mapValues(compute_statistics_of_month)  # \
        #.sortByKey()

    statistics_data.saveAsTextFile(file_out_name)

    result = statistics_data.collect()
    if debug:
        print("terza stampa")
        print(result)

    return result


def main():

    sc = SparkContext("local", "Query 2")
    start = datetime.datetime.now()
    print(start)

    current_milli_time = int(round(time.time() * 1000))
    file_path = Constants.TEMPERATURE_QUERY2_OUTPUT_FILE+str(current_milli_time)+".txt"

    result_temp = query2(sc,
                        Constants.TEMPERATURE_FILE,
                        Constants.MAX_TEMPERATURE,
                        Constants.MIN_TEMPERATURE,
                        file_path)

    del result_temp

    exit()

    current_milli_time = int(round(time.time() * 1000))
    file_path = Constants.HUMIDITY_QUERY2_OUTPUT_FILE + str(current_milli_time) + ".txt"

    result_hum = query2(sc,
                       Constants.HUMIDITY_FILE,
                       Constants.MAX_HUMIDITY,
                       Constants.MIN_HUMIDITY,
                       file_path)

    del result_hum

    current_milli_time = int(round(time.time() * 1000))
    file_path = Constants.PRESSURE_QUERY2_OUTPUT_FILE + str(current_milli_time) + ".txt"

    result_press = query2(sc,
                         Constants.PRESSURE_FILE,
                         Constants.MAX_PRESSURE,
                         Constants.MIN_PRESSURE,
                         file_path)

    del result_press

    end = datetime.datetime.now()
    print(end)
    print(end-start)


if __name__ == '__main__':
    main()

