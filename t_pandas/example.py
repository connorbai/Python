import pandas as pd
import numpy as np

'''
df[col]         Select column Series
df.loc[label]   Select row by label Series
df.iloc[loc]    Select row by integer location Series
df[5:10]        Slice rows DataFrame
df[bool_vec]    Select rows by boolean vector DataFrame
# print(pd.show_versions())
'''
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)

df = pd.read_csv('cmds-glue/input/algnmnt/2023-04-20/i_ads_cust_algnmnt.csv', usecols=['ALGNMNT_ID', 'CUST_ID', 'CUST_ALGNMNT_STRT_DT', 'CUST_ALGNMNT_END_DT'])
df = df.rename(columns={'ALGNMNT_ID': 'cn', 'CUST_ID': 'dctr', 'CUST_ALGNMNT_STRT_DT': 's_dt', 'CUST_ALGNMNT_END_DT': 'e_dt'})
# df.columns = [x.lower() for x in df.columns]

MAX_DATE = pd.Timestamp.floor(pd.Timestamp.max, freq='D')
df['s_dt'] = pd.to_datetime(df['s_dt'], errors='coerce').fillna(MAX_DATE)
df['e_dt'] = pd.to_datetime(df['e_dt'], errors='coerce').fillna(MAX_DATE)

# print(df['cn'].count())
# print(pd.Series([True,False,True,False]) & pd.Series([False,True,False,True]))
# df = df[(df['s_dt'] < pd.Timestamp('now')) & (df['e_dt'] > pd.Timestamp('now'))]
now = pd.Timestamp('now')
df = df.query("s_dt < @now < e_dt")
df = df[['cn', 'dctr']]

print(df['cn'].count())
# df['dulicated'] = df.duplicated(['cn', 'dctr'], keep=False)
df = df.sort_values(by=['cn', 'dctr'])
df = df.reset_index(drop=True)



df1 = pd.read_csv('cmds-glue/input/algnmnt/2023-04-20/i_ads_cust_algnmnt_tier_vw.csv', usecols=['ACTL_TIER', 'ALGNMNT_ID', 'CUST_ID', 'CUST_TIER_END_DT', 'CUST_TIER_STRT_DT'])
df1 = df1.rename(columns={'ACTL_TIER': 'tier', 'ALGNMNT_ID': 'cn', 'CUST_ID': 'dctr', 'CUST_TIER_END_DT': 'e_dt', 'CUST_TIER_STRT_DT': 's_dt'})
df1['s_dt'] = pd.to_datetime(df1['s_dt'], errors='coerce').fillna(MAX_DATE)
df1['e_dt'] = pd.to_datetime(df1['e_dt'], errors='coerce').fillna(MAX_DATE)
df1 = df1.query("(s_dt < @now < e_dt) and tier==5")
df1 = df1[['cn', 'dctr', 'tier']]
df1 = df1.reset_index(drop=True)


df3 = df.merge(df1, on=['cn', 'dctr'], how='inner')
df3.index.name = 'bob'

r = df3.query("cn=='CN69427' and dctr=='CN-300128148HCP'")
# print(df1.dtypes)
# print(df.loc[1])
# print('df.query', '*'*100, '\n', df.count())
# print('df.query', '*'*100, '\n', df.query("s_dt <= '2023-04-20' <= e_dt").count())
# print('df.query', '*'*100, '\n', df.query("s_dt > '2023-04-20'").count())

# print(df[['s_dt', 'e_dt']].apply(lambda x: x.idxmax()))
# print(df['s_dt'].idxmax(axis=0))
# p2 = pds[]
# df1 = df.groupby(['ALGNMNT_ID', 'CUST_ID']).count()


# df2 = pd.DataFrame({
#         "A": 1.0,
#         "B": pd.Timestamp("20130102"),
#         "C": pd.Series(1, index=list(range(4)), dtype="float32"),
#         "D": np.array([3] * 4, dtype="int32"),
#         "E": pd.Categorical(["test", "train", "test", "train"]),
#         "F": "foo",
#     })

print('*'*80, '\n', r, '\n', '*'*80, r.count())

if __name__ == '__main__':
    pass