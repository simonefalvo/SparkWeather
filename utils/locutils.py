from pytz import timezone
from timezonefinder import TimezoneFinder
from datetime import datetime
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
    # print("target timezone: ", tz_target)
    # ATTENTION: tz_target could be None! handle error case
    today_target = tz_target.localize(datetime.now())
    offset = today_target.strftime('%z')[0:3]       # +/-HHMM
    return int(offset)


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

    print(country(jlat, jlng))
    print(utcoffset(jlat, jlng))

    print(country(mlat, mlng))
    print(utcoffset(mlat, mlng))

    print(country(sflat, sflng))
    print(utcoffset(sflat, sflng))
