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

    file = Constants.CITY_ATTRIBUTES_FILE
    rdd = sc.textFile(file)

    file_data = rdd.map(lambda line: line).collect()
    start = True
    city_info_d = {}
    for line in file_data:
        if start:
            start = False
        else:
            city = str(line.split(",")[0].strip())
            lat = float(line.split(",")[1])
            lon = float(line.split(",")[2])
            city_info_d[city] = str(utils.locutils.country(lat, lon)) \
                + " ; " + city \
                + " ; " + str(utils.locutils.get_timezone(lat, lon))
    return city_info_d


def hourly_temps(header_pos, line, city_keys, add_date, convert_utc):
    """
    Generate a list of tuples (city_key, hourly_temperature)
    :param line: csv line
    :param city_keys: list of city_key keys 'country ; city ; tz'
    :param add_date: boolean value, if True add date to te key ('country ; city ; tz')
    :param convert_utc: boolean value, if True convert utc
    :return: a list of tuples (city_key, hourly_temperature)
    """
    hourly_temps_list = []
    utc_date = line[0:19]
    temperature = line.split(",")[1:]

    for city, city_key in city_keys.items():

        if convert_utc:
            local_datetime = utils.locutils.convert_timezone(utc_date, city_key.split(";")[2])
            local_datetime = local_datetime[0:10]
        else:
            local_datetime = utc_date[0:10]

        if add_date:
            city_key = city_key + ' ' + local_datetime  # 'country ; city ; tz yyyy-mm-dd'

        try:
            v = float(temperature[header_pos[city]])
            t = (city_key, [v])
            hourly_temps_list.append(t)

        except ValueError: #KeyError
            print("error converting in float:")
        except KeyError:
            print("Unexpected city in file:"+str(city))
            print(KeyError)
            exit(-1)

    return hourly_temps_list
