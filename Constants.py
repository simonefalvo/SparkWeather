
CITY_ATTRIBUTES_FILE = "hdfs://localhost:54310/topics/in/city_attributes.csv"
HUMIDITY_FILE = "hdfs://localhost:54310/topics/in/humidity.csv"
PRESSURE_FILE = "hdfs://localhost:54310/topics/in/pressure.csv"
TEMPERATURE_FILE = "hdfs://localhost:54310/topics/in/temperature.csv"
WEATHER_DESCRIPTION_FILE = "hdfs://localhost:54310/topics/in/weather_description.csv"

HUMIDITY_QUERY2_OUTPUT_FILE = "hdfs://localhost:54310/topics/out/query2/query2_humidity"
PRESSURE_QUERY2_OUTPUT_FILE = "hdfs://localhost:54310/topics/out/query2/query2_pressure"
TEMPERATURE_QUERY2_OUTPUT_FILE = "hdfs://localhost:54310/topics/out/query2/query2_temperature"

HUMIDITY_QUERY2_SQL_OUTPUT_FILE = "hdfs://localhost:54310/topics/out/query2sql/query2_humidity"
PRESSURE_QUERY2_SQL_OUTPUT_FILE = "hdfs://localhost:54310/topics/out/query2sql/query2_pressure"
TEMPERATURE_QUERY2_SQL_OUTPUT_FILE = "hdfs://localhost:54310/topics/out/query2sql/query2_temperature"


QUERY1_OUTPUT_FILE = "hdfs://localhost:54310/topics/out/query1/query1"
QUERY3_OUTPUT_FILE = "hdfs://localhost:54310/topics/out/query3/query3"


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
