import numpy as np
import csv
import pandas as pd
from pandas._config import dates
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)

# with open('D:/Downloads/f_v_cstmr.csv', newline='') as csvfile:
    # spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    # for row in spamreader:
    #     print('|'.join(row))

df = pd.read_csv('D:/tmp/python_debug/f_v_cstmr.csv', index_col=0, parse_dates=True)
dfp = pd.read_csv('D:/tmp/python_debug/cstmr_prd.csv', index_col=0)

df = df.iloc[:, 0:1]
dfp = dfp.iloc[:, 0:1]

df = df.convert_dtypes()
dfp = dfp.convert_dtypes()

df1 = pd.merge(df, dfp, how='left', on='cstmr_id')

print(
    df1.query('cstmr_name_y.isna()')


)


# df.groupby('cstmr_id').sum()






















if __name__ == '__main__':
    pass

# import zipfile
# print(zipfile.ZIP_DEFLATED)

# import os
# loc = os.getcwd();
# print(loc)

