import os

dir = os.path.dirname(__file__)

CITY_ATTRIBUTES_FILE = dir + "/data/city_attributes.csv"
HUMIDITY_FILE = dir + "/data/humidity.csv"
PRESSURE_FILE = dir + "/data/pressure.csv"
TEMPERATURE_FILE = dir + "/data/temperature.csv"
WEATHER_DESCRIPTION_FILE = dir + "/data/weather_description.csv"

MAX_TEMPERATURE = 330
MIN_TEMPERATURE = 220
POINT_WHERE_CUT_TEMP = 3

MAX_PRESSURE = 1100
MIN_PRESSURE = 0
POINT_WHERE_CUT_PRES = 4

MAX_HUMIDITY = 100
MIN_HUMIDITY = 0
POINT_WHERE_CUT_HUM = 3
#TODO: umidita va da 0 a 100 quindi il punto dove tagliare e problematico
