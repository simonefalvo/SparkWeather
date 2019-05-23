import Constants
import utils.locutils


def gen_city_keys(sc):
    """
    Read the city_key coordinates file, remove the header and for each city_key
    coordinates couple generate the related country and timezone in the format: 'country ; city_key ; timezone',
    for example 'United States ; Portland ; America/Los_Angeles'.
    :param sc: spark context
    :return: a collection of strings 'country ; city_key ; timezone'
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


def hourly_temps(line, city_keys, add_date, convert_utc):
    """
    Generate a list of tuples (city_key, hourly_temperature)
    :param line: csv line
    :param city_keys: list of city_key keys 'country ; city_key ; tz'
    :param add_date: boolean value, if True add date to te key ('country ; city_key ; tz')
    :param convert_utc: boolean value, if True convert utc
    :return: a list of tuples (city_key, hourly_temperature)
    """
    hourly_temps_list = []
    utc_date = line[0:19]
    temperature = line.split(",")[1:]

    for i, city_key in enumerate(city_keys):
        if convert_utc:
            local_datetime = utils.locutils.convert_timezone(utc_date, city_key.split(";")[2])
            local_datetime = local_datetime[0:10]
        else:
            local_datetime = utc_date[0:10]

        if add_date:
            city_key = city_key + ' ' + local_datetime  # 'country ; city ; tz yyyy-mm-dd'

        try:
            t = (city_key, float(temperature[i].strip()))
            hourly_temps_list.append(t)

        except ValueError:
            print("error converting in float:" + temperature[i])

    return hourly_temps_list
