import sys

import pandas as pd
import json

args = sys.argv[1:]
if len(args)==0:
    print('file name is required. and default file name is "dlt.json"')

file_name = args[0] if len(args)>0 else 'dlt.json'
print('input file_name: ', file_name)

with open(file_name, 'r', encoding='utf-8') as file:
    data = json.load(file)

study_dict = {
    'studyId': data['id'],
    'study_name': data['name']
}
df_main = pd.DataFrame([study_dict])

# df_survey = pd.json_normalize(data['surveyDownloadList'])
# df_task = pd.json_normalize(data['activeTaskDownloadList'])
# df_event = pd.json_normalize(data['eventDownloadList'])
df_subject = pd.json_normalize(data['participantDownloadList'])
if len(df_subject) == 0:
    print('participantDownloadList len == 0')
    exit(0)
df_subject = df_subject[['studyId', 'cohort', 'site', 'subject', 'participantResponseDownloadList']]
df_subject.rename({'participantResponseDownloadList': 'res' }, inplace=True, axis=1)

df = pd.merge(df_main, df_subject, how='inner', on='studyId')
df1 = df.explode('res')
df1.dropna(subset=['res'], inplace=True)
df1.loc[:, 'entity_id'] = df1.apply(lambda row: row['res']['entityId'] if pd.notna(row['res']) else None, axis=1)
df1.loc[:, 'entity_type'] = df1.apply(lambda row: row['res']['entityType'] if pd.notna(row['res']) else None, axis=1)
df1.loc[:, 'period'] = df1.apply(lambda row: row['res']['period'] if pd.notna(row['res']) else None, axis=1)
df1.loc[:, 'periodDay'] = df1.apply(lambda row: row['res']['periodDay'] if pd.notna(row['res']) else None, axis=1)
df1.loc[:, 'submittedDt'] = df1.apply(lambda row: row['res']['submittedDt'] if pd.notna(row['res']) else None, axis=1)
# I_QSCAT
df1.loc[:, 'I_QSCAT'] = df1.apply(lambda row: row['res']['name'] if pd.notna(row['res']) else None, axis=1)
df1.loc[:, 'data'] = df1.apply(lambda row: row['res']['data'] if pd.notna(row['res']) else None, axis=1)
df1.drop(columns=['res'], inplace=True)

# RecordDate SaveTS I_ADSSASMDAT_YYYY I_ADSSASMDAT_MM I_ADSSASMDAT_DD
df1['submittedDt'] = pd.to_datetime(df1['submittedDt'], errors='coerce')
df1.loc[:, 'RecordDate'] = df1['submittedDt'].dt.strftime('%Y-%m-%d %H:%M:%s')
df1.loc[:, 'SaveTS'] = df1['submittedDt'].dt.strftime('%Y-%m-%d %H:%M:%s')
df1.loc[:, 'I_ADSSASMDAT_YYYY'] = df1['submittedDt'].dt.strftime('%Y')
df1.loc[:, 'I_ADSSASMDAT_MM'] = df1['submittedDt'].dt.strftime('%m')
df1.loc[:, 'I_ADSSASMDAT_DD'] = df1['submittedDt'].dt.strftime('%d')

# InstanceName FolderName
df1.loc[:, 'InstanceName'] = 'Period ' + df1['period'].astype(str) + ' Day ' + df1['periodDay'].astype(str)
df1.loc[:, 'FolderName'] = 'Period ' + df1['period'].astype(str) + ' Day ' + df1['periodDay'].astype(str)


df1.loc[:, 'max_question'] = df1.apply(lambda row: len(row['data']) if isinstance(row['data'], list) else 0, axis=1)
df_list = []
df1.groupby(['entity_id', 'entity_type']).apply(lambda grp: df_list.append(grp), include_groups=False)

for df3 in df_list:
    max_no = df3['max_question'].max()
    columns_list = []
    if max_no > 0:
        for i in range(1, max_no+1):
            column_name1 = 'I_ADSSRES' + str(i)
            columns_list.append(column_name1)
            df3.loc[:, column_name1] = df3.apply(lambda row: ' ,'.join(row['data'][i-1]['answers']) if len(row['data']) >= i else None, axis=1)
            def handle_std(row):
                if len(row['data']) >= i:
                    data = row['data'][i-1]
                    options = data['options']
                    answers = data['answers']
                    matched_uuids = [option["uuid"] for answer in answers for option in options if option["title"] == answer]
                    return ' ,'.join(matched_uuids)
                return None
            question_type = df3['data'].iat[0][i-1]['type'] if len(df3['data'].iat[0])>i else 0
            is_selection = question_type == 20 or question_type == 30
            column_name2 = column_name1 + ('_STD' if is_selection else '_RAW')
            columns_list.append(column_name2)
            if is_selection:
                df3.loc[:, column_name2] = df3.apply(handle_std, axis=1)
            else:
                df3.loc[:, column_name2] = df3[column_name1]

    df3.drop(columns=['data', 'max_question'], inplace=True)

    df2 = df3.assign(projectid=None, project=None, environmentName=None, subjectId=None, StudySiteId=None, siteid=None,
                     # Site=None,
                     SiteNumber=None, SiteGroup=None, instanceId=None,
                     # InstanceName=None,
                     InstanceRepeatNumber=None, folderid=None, Folder=None,
                     # FolderName=None,
                     FolderSeq=None, TargetDays=None, DataPageId=None,
                     # DataPageName=None,
                     PageRepeatNumber=None,
                     # RecordDate=None,
                     RecordId=None,
                     RecordPosition=None, MinCreated=None, MaxUpdated=None,
                     # SaveTS=None,
                     StudyEnvSiteNumber=None, I_ADSSASMDAT=None, I_ADSSASMDAT_RAW=None, I_ADSSASMDAT_INT=None,
                     # I_ADSSASMDAT_YYYY=None, I_ADSSASMDAT_MM=None, I_ADSSASMDAT_DD=None,
                     # I_ADSSRES1_STD=None, I_ADSSRES2=None, I_ADSSRES2_RAW=None, I_ADSSRES3=None, I_ADSSRES3_STD=None,
                     I_ADSSEVAL=None, I_ADSSEVAL_STD=None,
                     # I_QSCAT=None
                     )
    df2.rename({'study_name': 'DataPageName', 'subject': 'Subject', 'site': 'Site'}, inplace=True, axis=1)
    columns = ['projectid', 'project', 'studyId', 'environmentName', 'subjectId', 'StudySiteId', 'Subject', 'siteid', 'Site', 'SiteNumber',
    'SiteGroup', 'instanceId', 'InstanceName', 'InstanceRepeatNumber', 'folderid', 'Folder', 'FolderName', 'FolderSeq', 'TargetDays', 'DataPageId',
    'DataPageName', 'PageRepeatNumber', 'RecordDate', 'RecordId', 'RecordPosition', 'MinCreated', 'MaxUpdated', 'SaveTS',
    'StudyEnvSiteNumber', 'I_ADSSASMDAT', 'I_ADSSASMDAT_RAW', 'I_ADSSASMDAT_INT', 'I_ADSSASMDAT_YYYY', 'I_ADSSASMDAT_MM', 'I_ADSSASMDAT_DD',]
    columns.extend(columns_list)
    columns.extend(['I_ADSSEVAL', 'I_ADSSEVAL_STD', 'I_QSCAT',])
    df4 = df2[columns]
    file_name = df4['I_QSCAT'].iat[0]
    print(f'generated file: {file_name}')
    df4.to_csv(f'./{file_name}.csv', index=False)


"""
projectid
project
studyid                 studyId
environmentName
subjectId               
StudySiteId
Subject                 subject
siteid
Site                    site
SiteNumber
SiteGroup
instanceId
InstanceName
InstanceRepeatNumber
folderid
Folder
FolderName
FolderSeq
TargetDays
DataPageId
DataPageName
PageRepeatNumber
RecordDate
RecordId
RecordPosition
MinCreated
MaxUpdated
SaveTS
StudyEnvSiteNumber
I_ADSSASMDAT
I_ADSSASMDAT_RAW
I_ADSSASMDAT_INT
I_ADSSASMDAT_YYYY
I_ADSSASMDAT_MM
I_ADSSASMDAT_DD
I_ADSSRES1
I_ADSSRES1_STD
I_ADSSRES2
I_ADSSRES2_RAW
I_ADSSRES3
I_ADSSRES3_STD
I_ADSSEVAL
I_ADSSEVAL_STD
I_QSCAT
"""










if __name__ == '__main__':
    pass