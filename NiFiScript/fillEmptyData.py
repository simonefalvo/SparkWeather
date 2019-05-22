import pandas as pd  # sudo -H pip3 install pandas
import numpy as np
import sys


def fix_point(val, val_min, val_max, dot_position):
    if val > val_max:
        tmp = str(val)
        tmp = tmp[:dot_position]
        val = float(tmp)

    if val < val_min or val > val_max:
        val = None

    return val


def main():
    csv_file = sys.stdin

    df = pd.read_csv(csv_file)

    if len(sys.argv) != 4:
        sys.stdout("Usage: min_value, max_value, point_where_cut_outlier")
        return -1
    try:
        val_min = float(sys.argv[1])
        val_max = float(sys.argv[2])
        dot_position = int(sys.argv[3])
    except ValueError:
        sys.stdout("Usage: float min_value, float max_value, int point_where_cut_outlier")
        return -2

    df.columns = [x.strip().replace(' ', '_') for x in df.columns]

    ''' gestione outlier '''
    for i in range(1, len(df.columns)):
        colum_name = df.columns[i]
        df[colum_name] = df[colum_name].apply(lambda val: fix_point(val, val_min, val_max, dot_position))


    ''' gestione missing value '''
    for i in range(1, len(df.columns)):

        colum_name = df.columns[i]
        # print(colum_name+" len: "+str(len(df[colum_name])))

        nan_index = df[colum_name].index[df[colum_name].apply(np.isnan)]
        # print(nan_index)

        df[colum_name] = (df[colum_name].ffill() + df[colum_name].bfill()) / 2
        df[colum_name] = df[colum_name].bfill().ffill()
        nan_index2 = df[colum_name].index[df[colum_name].apply(np.isnan)]
        # print(nan_index2)

        # print("\n******************")

    df.to_csv(sys.stdout, index=None, header=True)


if __name__ == '__main__':
    main()