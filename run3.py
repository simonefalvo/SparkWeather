from pyspark import SparkContext
import re
import Constants
import run2
import datetime


def main():

    start = datetime.datetime.now()

    sc = SparkContext("local", "Simple App")

    raw_csv = sc.textFile(Constants.TEMPERATURE_FILE)

    # remove header
    temp_header = raw_csv.filter(lambda l: "datetime" in l).flatMap(lambda line: line.split(","))
    raw_temp = raw_csv.subtract(temp_header)

    nat_cities = run2.generate_state_from_city(sc)
    #print(nat_cities)

    # get countries' list of the involved cities
    countries = []
    for nat_city in nat_cities:
        country = nat_city.split(" ; ")[0]
        if country not in countries:
            countries.append(country)
    #print(countries)

    season = raw_temp.filter(lambda l: re.search('^2017-06|^2017-07|^2017-08|^2017-09', l))
    summer_mean_temp = mean_temperature(season, nat_cities)  # ("country ; city", 2017 summer mean temperature)
    #print("summer 2017 mean temperature per city: ", summer_mean_temp.take(10))

    season = raw_temp.filter(lambda l: re.search('^2017-01|^2017-02|^2017-03|^2017-04', l))
    winter_mean_temp = mean_temperature(season, nat_cities)  # ("country ; city", 2017 winter mean temperature)
    #print("winter 2017 mean temperature per city: ", winter_mean_temp.take(10))

    # ("country ; city ; tz", 2017 summer-winter mean temperature difference)
    temp_diff = summer_mean_temp.join(winter_mean_temp).mapValues(lambda temps: abs(temps[0] - temps[1])).cache()
    #print("2017 temperature difference per city: ", temp_diff.take(10))

    season = raw_temp.filter(lambda l: re.search('^2016-06|^2017-07|^2017-08|^2017-09', l))
    summer_mean_temp = mean_temperature(season, nat_cities)  # ("country ; city", 2016 summer mean temperature)
    #print("summer 2016 mean temperature per city: ", summer_mean_temp.take(10))

    season = raw_temp.filter(lambda l: re.search('^2016-01|^2017-02|^2017-03|^2017-04', l))
    winter_mean_temp = mean_temperature(season, nat_cities)  # ("country ; city", 2017 winter mean temperature)
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



def mean_temperature(rdd, nat_cities):
    """
    get a list of hourly temperatures and group them by key ('country ; city ; tz'), then compute the mean
    :param rdd: raw lines of a csv
    :param nat_cities: list of 'country ; city ; tz' strings
    :return: seasonal mean temperature RDD composed of tuples of the form ('country ; city ; tz', mean_temperature)
    """

    # ("country ; city ; tz", [t1,...,tn])
    temp = rdd \
        .flatMap(lambda l: hourly_temps(l, nat_cities)) \
        .groupByKey()
    # TODO: compute mean with groupByKey
    # ("country ; city ; tz", mean temperature)
    return temp.map(lambda t: (t[0], sum(t[1]) / len(t[1])))


# generateTupleWithHoursAndCorrectData(line, cities, tipoChiave, val_max, val_min, dotPosition):
def hourly_temps(line, keys):
    """
    Generate a list of tuples (country ; city, hourly_temperature)
    """

    hourly_temps_list = []
    temperature = line.split(",")[1:]

    for i, key in enumerate(keys):

        try:
            temp = float(temperature[i].strip())
            if temp > Constants.MAX_TEMPERATURE or temp < Constants.MIN_TEMPERATURE:
                print("\n\nALARM: temperature out of range\n\n")
            t = (key, temp)
            hourly_temps_list.append(t)

        except ValueError:
            print("error float conv")

    return hourly_temps_list


if __name__ == '__main__':
    main()
