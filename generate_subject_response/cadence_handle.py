import pandas as pd

from common.common_logger import logger
from common.enum import EntityTypeEnum
from generate_subject_response.enums import ScheduleTypeEnum


def cadence_handle(df):
    df = df.assign(schedule_type=pd.NA)
    # expired_days expired_hours

    # one_time - later
    df_one_time_all = df.query('recurring==False')
    df_one_time_all = df_one_time_all.assign(schedule=ScheduleTypeEnum.one_time)
    df_one_time_all.drop(columns=['start_day', 'start_time', 'pattern', 'pattern_duration', 'end_day_a', 'end_day'],
                     inplace=True)
    # immediately
    df_one_time_i = df_one_time_all.query('immediately==True') #  & time_zone.notna()
    df_one_time_i = df_one_time_i.assign(schedule=ScheduleTypeEnum.immediately)
    # df_one_time_i.loc[:, 'dtf'] = pd.Timestamp.utcnow() + pd.to_timedelta(61, unit='second')
    # df_one_time_i.loc[:, 'dtf'] = df_one_time_i.apply(lambda x: x['dtf'].tz_convert(x['time_zone']).strftime('%Y-%m-%d %H:%M') if x['time_zone'] else '', axis=1)
    df_one_time_i.loc[:, 'dtf'] = df_one_time_i['crt_dt']
    # calculate phase No.
    df_one_time_i_calc = df_one_time_i.copy()
    df_one_time_i_calc.loc[:, 'dtf_str'] = df_one_time_i_calc['dtf'].str[:10]
    df_one_time_i_calc['dt_str'] = df_one_time_i_calc['dt'].dt.strftime('%Y-%m-%d')
    df_one_time_i_calc['period_eq'] = df_one_time_i_calc['dt_str'] == df_one_time_i_calc['dtf_str']
    df_one_time_i_calc.loc[df_one_time_i_calc['period_eq'] == False, 'phase_id'] = None
    def calculate_immediately(grp):
        if (~grp['period_eq']).all():
            df_one_time_i_calc.loc[grp.index[0], 'period_eq'] = True
    df_one_time_i_calc.groupby(['cohort_id', 'entity_id', 'entity_type', 'subject_id']).apply(calculate_immediately)
    df_eq = df_one_time_i_calc.query('period_eq == True')
    df_one_time_i = df_one_time_i.loc[df_eq.index]
    df_one_time_i.loc[:, 'phase_id'] = df_eq['phase_id']
    del df_one_time_i_calc, df_eq

    df_one_time_i.drop(columns=['later_time', 'later_day'], inplace=True)
    df_one_time_i = df_one_time_i.query('dtf.notna()')
    # del active_task -1 baseline data
    TASK = EntityTypeEnum.task
    df_baseline_task = df_one_time_i.query('entity_type==@TASK').query('dtf < dt')
    df_one_time_i.drop(index=df_baseline_task.index, inplace=True)
    # oneTime
    df_one_time = df_one_time_all.query('immediately==False')
    df_one_time.loc[:, 'dtf'] += ' ' + df_one_time['later_time']

    # one_time > 0
    df_one_time_1 = df_one_time.query('later_day > 0 & later_day == days')
    df_one_time_1.drop(columns=['later_time', 'later_day'], inplace=True)

    # one_time <= 0
    df_one_time_0 = df_one_time.query('later_day <= 0 & days==1')
    df_one_time_0.loc[:, 'days'] = df_one_time_0['later_day']
    df_one_time_0.loc[:, 'subtract'] = pd.to_timedelta(df_one_time_0['later_day'], unit='D')
    df_one_time_0.loc[:, 'subtract'] -= pd.to_timedelta(1, unit='D')
    df_one_time_0.loc[:, 'dt'] = df_one_time_0['dt'] + df_one_time_0['subtract']
    df_one_time_0['dtf'] = df_one_time_0['dt'].dt.strftime('%Y-%m-%d')
    df_one_time_0.loc[:, 'dtf'] += ' ' + df_one_time_0['later_time']
    df_one_time_0.drop(columns=['later_time', 'later_day', 'subtract'], inplace=True)
    del df_one_time

    # recurring
    df_recur0 = df.query('recurring==True')
    df_recur1 = df_recur0.query('end_day_a==False').query('start_day <= days')
    df_recur2 = df_recur0.query('end_day_a==True').query('start_day <= days <= end_day')
    df_recur = pd.concat([df_recur1, df_recur2], ignore_index=True)
    df_recur.drop(columns=['later_day', 'later_time', 'start_day', 'end_day_a', 'end_day'], inplace=True)
    df_recur.loc[:, 'dtf'] += ' ' + df_recur['start_time']
    del df_recur0, df_recur1, df_recur2

    # pattern_duration==1
    df_recur_1 = df_recur.query('pattern_duration==1')
    df_recur_1.drop(columns=['start_time', 'pattern', 'pattern_duration'], inplace=True)

    # pattern_duration > 1
    df_recur_2 = df_recur.query('pattern_duration > 1')
    df_recur_2 = df_recur_2.assign(recur_day=pd.NA)
    df_recur_2.loc[:, 'recur_day'] = df_recur_2.groupby(['entity_id', 'entity_type', 'subject_id']).cumcount()
    df_recur_2.loc[:, 'recur_day'] = df_recur_2['recur_day'] % df_recur_2['pattern_duration'] + 1
    ser_contain = df_recur_2.apply(lambda r: r['recur_day'] in r['pattern'], axis=1)
    df_recur_2 = df_recur_2[ser_contain]
    df_recur_2.drop(columns=['start_time', 'pattern', 'pattern_duration', 'recur_day'], inplace=True)
    del df_recur, ser_contain

    df_all = pd.concat([df_one_time_i, df_one_time_1, df_one_time_0, df_recur_1, df_recur_2], ignore_index=True)
    logger.info('df_all len: %s', len(df_all))
    if len(df_all) == 0:
        logger.info('df_all len==0: stop')
        return
    return df_all
