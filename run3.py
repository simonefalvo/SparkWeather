from pyspark import SparkContext
import re
import Constants
import common.weather as weather
import datetime
import utils
import statistics


def main():

    start = datetime.datetime.now()

    sc = SparkContext("local", "Simple App")
    raw_csv = sc.textFile(Constants.TEMPERATURE_FILE)

    # remove header
    temp_header = raw_csv.filter(lambda l: "datetime" in l).flatMap(lambda line: line.split(","))
    raw_temp = raw_csv.subtract(temp_header)

    city_keys = weather.gen_city_keys(sc)
    print(city_keys)

    # get countries' list of the involved cities
    countries = []
    for city_key in city_keys:
        country = city_key.split(" ; ")[0]
        if country not in countries:
            countries.append(country)
    print(countries)

    season = raw_temp.filter(lambda l: re.search('^2017-05-31|^2017-06|^2017-07|^2017-08|^2017-09|^2017-10-1', l))
    season_regex = '^2017-0[6-9]-...1[2-5]'
    summer_mean_temp = mean_temperature(season, city_keys, season_regex)  # ("country ; city", 2017 summer mean temperature)
    #print("summer 2017 mean temperature per city: ", summer_mean_temp.take(10))

    season = raw_temp.filter(lambda l: re.search('^2016-12-31|^2017-01|^2017-02|^2017-03|^2017-04|^2017-05-1', l))
    season_regex = '^2017-0[1-4]-...1[2-5]'
    winter_mean_temp = mean_temperature(season, city_keys, season_regex)  # ("country ; city", 2017 winter mean temperature)
    #print("winter 2017 mean temperature per city: ", winter_mean_temp.take(10))

    # ("country ; city ; tz", 2017 summer-winter mean temperature difference)
    temp_diff = summer_mean_temp.join(winter_mean_temp).mapValues(lambda temps: abs(temps[0] - temps[1])).cache()
    #print("2017 temperature difference per city: ", temp_diff.take(10))

    season = raw_temp.filter(lambda l: re.search('^2016-05-31|^2016-06|^2016-07|^2016-08|^2016-09|^2016-10-1', l))
    season_regex = '^2016-0[6-9]-...1[2-5]'
    summer_mean_temp = mean_temperature(season, city_keys, season_regex)  # ("country ; city", 2016 summer mean temperature)
    #print("summer 2016 mean temperature per city: ", summer_mean_temp.take(10))

    season = raw_temp.filter(lambda l: re.search('^2015-12-31|^2016-01|^2016-02|^2016-03|^2016-04|^2016-05-1', l))
    season_regex = '^2016-0[1-4]-...1[2-5]'
    winter_mean_temp = mean_temperature(season, city_keys, season_regex)  # ("country ; city", 2017 winter mean temperature)
    #print("winter 2016 mean temperature per city: ", winter_mean_temp.take(10))

    # ("country ; city", 2016 summer-winter mean temperature difference)
    prev_temp_diff = summer_mean_temp.join(winter_mean_temp).mapValues(lambda temps: abs(temps[0] - temps[1])).cache()
    #print("2016 temperature difference per city: ", prev_temp_diff.take(10))

    for country in countries:
        # get country's 2017 temperature differences
        country_temp = temp_diff.filter(lambda value: country in value[0])
        # get country's 2017 temperature differences and sort by value in ascending order
        # producing tuples of the form ("country ; city", position)
        prev_country_temp = prev_temp_diff.filter(lambda value: country in value[0]) \
            .sortBy(keyfunc=lambda x: x[1], ascending=False) \
            .map(lambda x: x[0]) \
            .zipWithIndex()
        temp_chart = country_temp.join(prev_country_temp).takeOrdered(15, lambda x: -x[1][0])
        # TODO: ridurre l'rdd ai soli primi 15 elementi ordinati per alleggerire la join

        print(country)
        for i, x in enumerate(temp_chart):
            print("{}-{}: {}, (2016 position: {})"
                  .format(i + 1,
                          x[0].split(" ; ")[1],
                          x[1][0],
                          x[1][1] + 1))
        print("-----------------------------------------------------------------------\n")

    end = datetime.datetime.now()
    print("Processing time: ", end - start)


def local_datetime(rdd_tuple):
    """
    Convert tuple utc into local time
    :param rdd_tuple: ('country ; city ; timezone', temp, yyyy-mm-dd)
    :return:  ('country ; city', temp, yyyy-mm-dd)
    """
    timezone = rdd_tuple[0].split(" ; ")[2]
    utc_time = rdd_tuple[2]
    city_key = rdd_tuple[0][:-(len(timezone) + 3)]
    temperature = rdd_tuple[1]
    local_dt = utils.locutils.convert_timezone(utc_time, timezone)
    return city_key, temperature, local_dt


def mean_temperature(rdd, city_keys, season_regex):
    """
    get a list of hourly temperatures and group them by key ('country ; city'), then compute the mean
    :param rdd: raw lines of a csv
    :param city_keys: list of 'country ; city ; tz' strings
    :param season_regex:
    :return: seasonal mean temperature RDD composed of tuples of the form ('country ; city', mean_temperature)
    """
    # ("country ; city ; tz", [t1,...,tn])
    hourly_utc_temps = rdd.flatMap(lambda l: hourly_temps(l, city_keys))
    hourly_local_temps = hourly_utc_temps.map(lambda t: local_datetime(t)) \
        .filter(lambda t: re.search(season_regex, t[2])) \
        .map(lambda t: (t[0], [t[1]])) \
        .reduceByKey(lambda x, y: x + y)
    # TODO: compute mean with groupByKey
    # ("country ; city", mean temperature)
    return hourly_local_temps.map(lambda t: (t[0], statistics.mean(t[1])))


def hourly_temps(line, keys):
    """
    Generate a list of tuples (country ; city ; tz, hourly_temperature, yyyy-mm-dd hh:mm:ss)
    """
    hourly_temps_list = []
    temperatures = line.split(",")
    utc = temperatures.pop(0)

    for i, key in enumerate(keys):

        try:
            temp = float(temperatures[i].strip())
            t = (key, temp, utc)
            hourly_temps_list.append(t)

        except ValueError:
            print("error float conv")

    return hourly_temps_list


if __name__ == '__main__':
    main()
