import datetime

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Boolean, DateTime
import numpy as np
from Common_Method import log


na = np.NaN
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)
# dialect+driver://username:password@host:port/database
engine = create_engine("postgresql://postgres:Win2008@localhost:5432/cmds")
schema_engine = engine.execution_options(
    schema_translate_map={
        "public": None,  # Table objects with schema="public" will render with no schema
        None: "cmd_owner",  # no schema name -> "user_schema_one"
    }
)

# ext_group_key_table = Table(
#      "ext_all_group_key",
#      MetaData(schema='cmd_owenr'),
#      Column("id", Integer, primary_key=True),
#      Column("cmds_id", Integer),
#      Column("star_id", Integer),
#      Column("org_lvl", Integer),
#      Column("prd_cmds_id", Integer),
#      Column("parent_org_id", Integer),
#      Column("start_cycle", Integer),
#      Column("end_cycle", Integer),
#      Column("stts_ind", Integer),
#      Column("isdeleted", Boolean),
#      Column("is_afflt", Boolean),
#      Column("been_moved", Boolean),
#      Column("algnmtid", String),
#      Column("prd_gk", String),
#      Column("createduser", String),
#      Column("modifieduser", String),
#      Column("createddate", DateTime),
#      Column("modifieddate", DateTime),
#      Column("crt_dt", DateTime),
#  )
# conn = schema_engine.connect()
# gks = ext_group_key_table.select().fetch(count=100)
# conn.commit()
#
#
#
# log(gks)


# connect(host='127.0.0.1', dbname='cmds', user='postgres', password='Win2008')
# df = pd.read_sql_query('SELECT \
#                             hco.hco_id as "hcoId"\
#                             ,hco.hco_name as "hcoName"\
#                             ,hco.stts_ind as "sttsInd"\
#                             ,hco.vn_entity_id as "vnEntityId"\
#                             ,hco.merged_to as "mergedTo"\
#                             ,hcoMergedTo.hco_id as "mergedToHcoId"\
#                             ,hco.clsfctn_name as "hcoType"\
#                             FROM cmd_owner.m_hco hco\
#                             LEFT JOIN cmd_owner.m_hco hcoMergedTo on hco.merged_to = hcoMergedTo.vn_entity_id ', engine, index_col='hcoId')
# cmds_id, star_id, org_lvl, prd_cmds_id, parent_org_id,
# algnmtid, prd_gk, start_cycle, end_cycle, stts_ind
# df_gk = pd.read_sql_query('SELECT * \
#                             FROM cmd_owner.ext_all_group_key WHERE isdeleted=FALSE', engine)
# df_thc = pd.read_csv('./csvfile/THC_202304_all_49323.csv')
# df_thc.columns = ['yearmonth', 'buName', 'groupKey', 'categoryId', 'categoryName', 'hcoStarId', 'hcoId', 'hcoName',
#                   'status', 'status_desc', 'mergedVeevaId', 'mergedHcoName', 'commonHco']

# df_gk.loc[:, 'id'] = np.nan
# df_gk.loc[:, 'prd_gk'] = df_gk['prd_gk'] + '_test'
# df_gk1 = df_gk.drop('id', axis=1)

# log(df_gk1)

# log(df_gk1.to_sql('ext_all_group_key', con=schema_engine, if_exists='append', index=False, chunksize=2000))

# df_merge_gk = df_thc.merge(df_gk, on='groupKey')
# log(df_thc.value_counts(), df_gk.count(), df_merge_gk.count())
# log(df_thc.loc[1])
# print(df_thc)
# 49323
# print(df_thc.merge(df, on='hcoId', how='left').)
# df_merege = df_thc.merge(df, on='hcoId', how='left')
# 49323
# print(df_merege[df_merege.isna()['mergedVeevaId']])
# print(df_thc[df_thc.duplicated(['yearmonth', 'groupKey', 'categoryId', 'hcoId'], keep=False)])

# print(df_merge_gk)
# print(df_merge_gk.query('stts_ind not in [1,0]'))
# df_merge_gk.query('stts_ind==0').loc[:, ['status']] = 'inactive'
# df_invalid = df_merge_gk.query('stts_ind==0').index
# df_invalid.loc[:, ['status']] = 'inactive'
# df_merge_gk.loc[df_invalid, 'status'] = 'inactive'
# df_merge_gk.loc[df_invalid, 'status_desc'] = 'prd_gk_invalid'

# log(datetime.date.strftime('%y%m%d'))

# df_invalid_gk =
# df_merge_gk.loc[df_merge_gk.query('').index]
# df_merge_gk[df_merge_gk['stts_ind'] == 0]['status'] = 'inactive'
# log(df_merge_gk[df_merge_gk['stts_ind'] == 0]['status'])
# log(df_merge_gk.query('stts_ind==0'))
# log(df_merge_gk.loc[:, ['status']])

# df1 = df_gk.iloc[1:5]
# df1.index.name = 'prd_gk'
# df1.index = df1.index + '_test1'
# print(df1, df1.index)
# print(df1.to_sql('cmd_owner.m_sales_prd_test', engine, if_exists='append'))














if __name__ == '__main__':
    pass