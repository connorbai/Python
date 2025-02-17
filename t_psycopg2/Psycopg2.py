import csv
from io import StringIO

import pandas as pd
# import pandas.io.sql as sqlio
import psycopg2
from dotenv import load_dotenv
# from Common_Method import sql_to_dataframe, connect

load_dotenv()
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)

# conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format('127.0.0.1', 5432, 'cmds', 'postgres', 'Win2008'))
# sql = "select * from cmd_owner.m_sales_prd;"
# df = sqlio.read_sql_query(sql, conn)
# conn = None
# print(df)


"""
    Write Data into CSV File
"""
# conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format('127.0.0.1', 5432, 'cmds', 'postgres', 'Win2008'))
conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='cmds', user='postgres', password='Win2008', options=f"-c search_path=cmd_owner")
cur = conn.cursor()
# cur.execute('SELECT * FROM cmd_owner.m_sales_prd;')
cur.execute('SELECT * FROM m_sales_prd;')
data = cur.fetchall()
cols = [elt[0] for elt in cur.description]
df = pd.DataFrame(data=data, columns=cols)
df.set_index('id', drop=True)
print(cur.description, cols, df)
cur.close()
conn.close()
conn = None

# query = '''SELECT * FROM cmd_owner.m_sales_prd'''
# conn = connect()
# df = sql_to_dataframe(conn, query)
# print(df['id'].count())
# df = df.drop(columns='id')
# print('df_head\n', df.head())
# s_buf = StringIO()
# df.to_csv(s_buf, header=False, index=False)
# s_buf.seek(0)
# columns = ', '.join(['"{}"'.format(k) for k in df.columns])
# cur = conn.cursor()
# sql = "COPY {} ({}) FROM STDIN WITH DELIMITER ',' CSV NULL 'NULL'".format('cmd_owner.m_sales_prd_copy1', columns)
# cur.copy_expert(sql=sql, file=s_buf)
# conn.commit()
# cur.close()
# conn.close()



if __name__ == '__main__':
    pass
