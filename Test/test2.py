import pandas as pd

start_date_1 = '2021-01-01'
end_date_1 = '2021-01-15'

start_date_2 = '2021-01-10'
end_date_2 = '2021-01-31'

d1 = pd.date_range(start_date_1, end_date_1, freq='D')
d2 = pd.date_range(start_date_2, end_date_2, freq='D')
print(d1.intersection(d2))


df = pd.DataFrame({'A': 'a b a b'.split(), 'B': [1, 2, 3, 4]})
#    A  B
# 0  a  1
# 1  b  2
# 2  a  3
# 3  b  4

a=df.groupby('A').pipe(lambda x: x.max() - x.min())
print(a)



if __name__ == '__main__':
    pass

