# import csv
# from io import StringIO
# import pandas as pd
# import pandas.io.sql as sqlio

import psycopg2
import sys

# 获取命令行参数
arguments = sys.argv

# 输出文件名
print("File name:             ", arguments[0])

# 输出传递的参数
for arg in arguments[1:]:
    print("Argument:               ", arg)
    # print(type(arg))

schema = 'cmd_owner'
tableName = arguments[1]
tName = f'{schema}.{tableName}'
csv_file_path = arguments[2]

# tableName = 'm_prdct_ctgry'
# csv_file_path = 'D:/Project/Nodejs/src/view_qa_sql/query.csv'

print(f'schema:             tName: {tName}, csv_file_path: {csv_file_path}')

if schema is None or tableName is None or csv_file_path is None:
    raise ValueError("schema table csv_file_path None")

host = '127.0.0.1'
# host = '192.168.100.46'
print(host)
conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host, 5432, 'cmds', 'postgres', 'Win2008'))
cur = conn.cursor()


cur.execute(f'TRUNCATE {tName} CASCADE;')
copy_query = f"COPY {tName} FROM '{csv_file_path}' DELIMITER ',' CSV HEADER"
# print(copy_query)
cur.execute(copy_query)
print(f'Synchronize Table:             {tName} Successfully.')

"""
# sql = "COPY {} ({}) FROM STDIN WITH DELIMITER ',' CSV NULL 'NULL'".format(f'{schema}.{tableName}', columns)
# cur.copy_expert(sql=sql, file=s_buf)


data = open(csv_file_path).read()
reader = csv.DictReader(StringIO(data))
print(reader)

# for row in reader:
#     print(row)
# df = pd.read_csv(csv_file_path)
s_buf = StringIO(data)
# df.to_csv(s_buf, header=False, index=False)
s_buf.seek(0)
"""

conn.commit()
cur.close()
conn.close()

if __name__ == '__main__':
    pass
