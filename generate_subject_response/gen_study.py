import numpy as np
import pandas as pd

from common.common_logger import logger
from generate_subject_response.cadence_handle import cadence_handle
from generate_subject_response.response_sql import sql_query_response, sql_del_response, sql_updt_ver, \
    sql_participant_insert, sql_study_list, sql_participant_list


def generate_study_response(pubdb, trxdb, study_id, usr):
    try:
        df = generate_study_df(pubdb, trxdb, study_id)
        if df is None or len(df) == 0:
            return logger.error("df is None | df len==0")

        df_all = cadence_handle(df)
        if df_all is None:
            return logger.error("df_all is None")
        study_version = df['version'].astype(np.float16).max()
        study_version_str = str(study_version)
        df_all.loc[:, 'r_idx'] = df_all.index
        # insert data
        usr = usr if usr else 'system'
        df_response = pubdb.fetch_dataframe(sql_query_response, (study_id,))
        response_version = df_response['version'].dropna().astype(np.float16).max()
        df_all_ni = df_all.query('immediately==False')
        df_merged_ni = pd.merge(
            df_all_ni[['uniq_name', 'entity_id', 'entity_type', 'subject_id', 'dtf', 'schedule', 'r_idx', 'expired_days', 'overdue_hours']],
            df_response, how='left', on=['uniq_name', 'entity_type', 'subject_id', 'dtf']
        )
        # immediately Special Handling Logic
        df_all_i = df_all.query('immediately==True')
        df_merged_i = pd.merge(
            df_all_i[['uniq_name', 'entity_id', 'entity_type', 'subject_id', 'schedule', 'r_idx', 'dtf', 'expired_days', 'overdue_hours']],
            df_response[['uniq_name', 'entity_id', 'entity_type', 'subject_id', 'schedule_type', 'entity_name',  'submitted', 'r_id', 'version']],
            how='left', on=['uniq_name', 'entity_type', 'subject_id']
        )
        df_merged = pd.concat([df_merged_ni, df_merged_i], ignore_index=True)
        # df_merged = df_merged_ni

        # df_submitted = pd.DataFrame()
        df_submitted = df_merged.query("r_id.notna()")

        # 1. del r_id != submitted
        r_id_list = df_submitted['r_id'].dropna().drop_duplicates().to_list()
        if len(r_id_list) > 0:
            pubdb.update(sql_del_response + 'AND id NOT IN %s', (usr, study_id, tuple(r_id_list)))
            if study_version > response_version:            # draft activate
                df_updt_ver = df_submitted.query('r_id.notna() & submitted==False & version != @study_version_str')
                r_id_ver_list = df_updt_ver['r_id'].dropna().drop_duplicates().to_list()
                if len(r_id_ver_list) > 0:
                    df_updt_ver.loc[:, 'usr'] = usr
                    df_updt_ver.loc[:, 'version'] = study_version_str
                    df_updt_ver = df_updt_ver.replace({ np.nan: None, pd.NA: None })
                    pubdb.chunk_update_df(sql_updt_ver, df_updt_ver[['version', 'usr', 'entity_id_x', 'expired_days', 'overdue_hours', 'r_id']])
        else:
            pubdb.update(sql_del_response, (usr, study_id))
        del df_merged, df_merged_ni, df_response  #, df_merged_i

        # 2. insert submitted!=True
        df_all_insert = df_all.drop(index=df_submitted['r_idx'])
        df_insert = df_all_insert[['study_id', 'study_name', 'cohort_id', 'entity_id', 'uniq_name', 'entity_name', 'entity_type',
                                   'subject_id', 'phase_id', 'days', 'dtf', 'expired_days', 'overdue_hours',
                                   'duration', 'version', 'schedule']]
        if len(df_insert) > 0:
            response_data = df_insert.assign(crt_usr=usr, updt_usr=usr)
            # pd.NA # np.nan psycopg2 not support
            response_data = response_data.replace({pd.NA: None, np.nan: None})
            pubdb.chunk_insert_df(sql_participant_insert, response_data)
            logger.info('insert into len: %s', len(response_data))

        return study_version > response_version
    except Exception as e:
        raise e
    finally:
        logger.info("------generate subject response end------")



def generate_study_df(pubdb, trxdb, study_id):
    try:
        df_study = trxdb.fetch_dataframe(sql_study_list, (study_id, study_id))
        if len(df_study) == 0:
            return logger.error('df len == 0')

        # fetch subject dataframe
        df_subject = pubdb.fetch_dataframe(sql_participant_list, (study_id,))
        if not len(df_subject):
            return logger.error('df_subject is None')

        # df subject explode
        df_subject['dt'] = pd.to_datetime(df_subject['dt'])
        df_subject['duration'] = df_subject['duration'].astype(np.int32)

        df_subject['new_dt'] = df_subject.apply(lambda row: [row['dt'] + pd.DateOffset(days=i) for i in range(row['duration'])], axis=1)
        df_dur = df_subject.explode('new_dt').reset_index(drop=True)
        df_dur['dt'] = df_dur['new_dt']
        df_dur.drop(columns=['new_dt'], inplace=True)
        df_dur['days'] = df_dur.groupby('subject_id').cumcount() + 1
        df_dur['dtf'] = df_dur['dt'].dt.strftime('%Y-%m-%d')
        logger.info('df duration len: %s', len(df_dur))

        df = pd.merge(df_study, df_dur, how='inner', on=['cohort_id'])
        del df_study, df_dur, df_subject
        return df
    except Exception as e:
        logger.error("generate_entity_df - generate error")
        raise e
    finally:
        logger.info("generate_study_df end")

