import psycopg2

print('==============connecting==================')

conn = psycopg2.connect(
    host='dify-db.clzbxkoauro6.ap-southeast-1.rds.amazonaws.com',
    port=5432,
    dbname='postgres',
    user='postgres',
    password='wRm3EXzI6SxlVuICuBtL',
    # options=f"-c search_path=cmd_owner"
)
cur = conn.cursor()
cur.execute('select * from public.meeting_detail limit 1')
data = cur.fetchall()
print('data: ', data)

print('==============done==================')

if __name__ == '__main__':
    pass
