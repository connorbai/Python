import csv
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

# 从CSV文件读取查询结果
with open(csv_file_path, 'r') as file:
    csv_reader = csv.reader(file)
    rows = list(csv_reader)

# 构建创建表的SQL语句
create_table_sql = f"CREATE TABLE {tName} ("

for row in rows:
    column_name = row[0]
    data_type = row[1]
    character_maximum_length = row[2]
    is_nullable = row[3]

    column_definition = f"{column_name} {data_type}"

    if data_type == "numeric":
        column_definition += "(precision, scale)"

    if character_maximum_length:
        column_definition += f"({character_maximum_length})"

    if not is_nullable:
        column_definition += " NOT NULL"

    create_table_sql += column_definition + ", "

# 去除最后一个逗号和空格
create_table_sql = create_table_sql.rstrip(", ") + ");"

# 连接到PostgreSQL数据库
conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format('127.0.0.1', 5432, 'cmds', 'postgres', 'Win2008'))

# 创建游标对象
cursor = conn.cursor()

# 删除具有相同表名的表
drop_table_sql = f"DROP TABLE IF EXISTS {tName};"
cursor.execute(drop_table_sql)

# 执行创建表的SQL语句
cursor.execute(create_table_sql)

# 提交事务
conn.commit()

# 关闭游标和数据库连接
cursor.close()
conn.close()