from pytz import timezone
import pytz
from timezonefinder import TimezoneFinder
from _datetime import datetime

import reverse_geocode

tf = TimezoneFinder(in_memory=True)


def country(lat, lng):
    """
    returns the country name of the given coordinates
    """
    coordinates = [(lat, lng)]
    info = reverse_geocode.search(coordinates)[0]
    return info["country"]


def utcoffset(lat, lng):
    """
    returns a location's time zone offset from UTC.
    """
    target = dict({'lat': lat, 'lng': lng})
    tz_target = timezone(tf.certain_timezone_at(lat=target['lat'], lng=target['lng']))

    print("target timezone: ", tz_target)
    # ATTENTION: tz_target could be None! handle error case
    today_target = tz_target.localize(datetime.now())
    offset = today_target.strftime('%z')[0:3]       # +/-HHMM
    return int(offset)

def get_timezone(lat, lng):
    """
        returns a location's time zone.
    """
    target = dict({'lat': lat, 'lng': lng})
    tz_target = timezone(tf.certain_timezone_at(lat=target['lat'], lng=target['lng']))
    return str(tz_target)

def convert_timezone(my_date, my_timezone):

    my_struct_date = datetime.strptime(my_date, "%Y-%m-%d %H:%M:%S")
    utc_date = pytz.utc.localize(my_struct_date)
    timezone_date = utc_date.astimezone(pytz.timezone(my_timezone.strip()))

    return str(timezone_date)[0:19]



if __name__ == '__main__':

    # Jerusalem
    jlat = 31.769039
    jlng = 35.216331
    # Miami
    mlat = 25.77
    mlng = -80.19
    # San Francisco
    sflat = 37.77
    sflng = -122.419
    # Detroit
    # Google: 42.4206146 -83.7472549
    dlat = 42.331429
    dlng = -83.045753

    print(country(jlat, jlng))
    print(utcoffset(jlat, jlng))

    print(country(mlat, mlng))
    print(utcoffset(mlat, mlng))

    print(country(sflat, sflng))
    print(utcoffset(sflat, sflng))

    print(country(dlat, dlng))
    print(utcoffset(dlat, dlng))
