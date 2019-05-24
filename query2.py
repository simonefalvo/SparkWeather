from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import common.weather as weather
import run2
import time
import datetime

import Constants



def generate_tuple(header_pos, line, city_keys):
    """
    Generate a list of tuples (city_key, hourly_temperature)
    :param line: csv line
    :param city_keys: list of city_key keys 'country ; city ; tz'
    :return: a list of row (country, year, month, value)
    """
    my_list = []
    date = line[0:15]  # yyyy-mm-dd
    descriptions = line.split(",")
    del descriptions[0]  # remove non-description info

    temperature = line.split(",")[1:]

    for city, city_key in city_keys.items():

        country = city_key.split(";")[0]

        try:
            v = float(temperature[header_pos[city]])
            t = Row(country=country, year=date[0:4], month=date[5:7], value=v)
            my_list.append(t)

        except ValueError: #KeyError
            print("error converting in float:")
        except KeyError:
            print("Unexpected city in file:"+str(city))
            print(KeyError)
            exit(-1)

    return my_list


def query2(sc, file_in_name, file_out_name):

    rdd_file_data = sc.textFile(file_in_name)

    data_header = rdd_file_data \
        .filter(lambda l: "datetime" in l)

    cites = weather.gen_city_keys(sc)

    header_position = run2.get_position(data_header)

    data = rdd_file_data \
        .subtract(data_header) \
        .flatMap(lambda line: generate_tuple(header_position, line, cites))

    sqlc = SQLContext(sc)

    df = sqlc.createDataFrame(data)
    #df.show()
    df.createOrReplaceTempView("dati")

    query1 = "SELECT country, year, month, " \
             "cast(min(value) as decimal (10,2)) as my_min, " \
             "cast(max(value) as decimal (10,2)) my_max, " \
             "cast(avg(value) as decimal (10,2)) as my_avg," \
             "cast(stddev(value) as decimal (10,2)) as my_std " \
             "FROM dati " \
             "GROUP BY country, year, month"
    df2 = sqlc.sql(query1).orderBy('dati.country', 'dati.year', 'dati.month')
    df2.show()

    '''
            Save data in HDFS
    '''
    df2.coalesce(1).write.format("json").save(file_out_name)


def main():

    sc = SparkContext("local", "Query 2")

    start = datetime.datetime.now()
    print(start)

    current_milli_time = int(round(time.time() * 1000))
    file_out_name = Constants.TEMPERATURE_QUERY2_SQL_OUTPUT_FILE+str(current_milli_time)+".json"
    query2(sc, Constants.TEMPERATURE_FILE, file_out_name)

    current_milli_time = int(round(time.time() * 1000))
    file_out_name = Constants.PRESSURE_QUERY2_SQL_OUTPUT_FILE + str(current_milli_time) + ".json"
    query2(sc, Constants.PRESSURE_FILE, file_out_name)

    current_milli_time = int(round(time.time() * 1000))
    file_out_name = Constants.HUMIDITY_QUERY2_SQL_OUTPUT_FILE + str(current_milli_time) + ".json"
    query2(sc, Constants.HUMIDITY_FILE, file_out_name)

    end = datetime.datetime.now()
    print(end)
    print(end - start)

if __name__ == '__main__':
    main()
