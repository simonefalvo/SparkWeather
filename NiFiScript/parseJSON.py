import json
import Constants
import sys


val_max = Constants.MAX_TEMPERATURE
val_min = Constants.MIN_TEMPERATURE
dot_position = Constants.POINT_WHERE_CUT_TEMP

def main():
    json_file = sys.stdin

    data = json.load(json_file)

    for query_result in data:
        for key in query_result.keys():

            if (not isinstance(query_result[key], str)) and (query_result[key] is not None):
                try:
                    temp = float(query_result[key])
                    if query_result[key] > val_max:
                        tmp_s = str(query_result[key])
                        tmp_s = tmp_s[:dot_position] # cut data prevent double point
                        #todo: migliorare
                        temp = float(tmp_s)

                    if temp > val_min and temp < val_max:
                        query_result[key] = temp
                    else:
                        query_result[key] = None
                except ValueError:
                    query_result[key] = None

    string_out = json.dumps(data, sort_keys=False, indent=4)
    sys.stdout.write(string_out)

    return 0


if __name__ == '__main__':
    main()
