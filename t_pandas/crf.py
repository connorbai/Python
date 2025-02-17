import sys

import pandas as pd
import json

args = sys.argv[1:]
if len(args)==0:
    print('file name is required. and default file name is "crf.json"')

file_name = args[0] if len(args)>0 else 'dlt.json'
print('input file_name: ', file_name)

with open(file_name, 'r', encoding='utf-8') as file:
    data = json.load(file)

study_dict = {
    'studyId': data['StudyId'],
    'study_name': data['StudyName']
}
df_main = pd.DataFrame([study_dict])
df_subject = pd.json_normalize(data['Forms'])
df_subject = df_subject[['FormId', 'FormName', 'Assignments']]
df_subject.loc[:, 'studyId'] = data['StudyId']
df_subject.rename(columns={'FormName': 'I_QSCAT'}, inplace=True)

df = pd.merge(df_main, df_subject, how='inner', on='studyId')
df1 = df.explode('Assignments')
df1.dropna(subset=['Assignments'], inplace=True)
df1.loc[:, 'Subject'] = df1.apply(lambda row: row['Assignments']['Subject'] if pd.notna(row['Assignments']) else None, axis=1)
df1.loc[:, 'Cohort'] = df1.apply(lambda row: row['Assignments']['Cohort'] if pd.notna(row['Assignments']) else None, axis=1)
df1.loc[:, 'site'] = df1.apply(lambda row: row['Assignments']['Site'] if pd.notna(row['Assignments']) else None, axis=1)
df1.loc[:, 'period'] = df1.apply(lambda row: row['Assignments']['Period'] if pd.notna(row['Assignments']) else None, axis=1)
df1.loc[:, 'periodDay'] = df1.apply(lambda row: row['Assignments']['Day'] if pd.notna(row['Assignments']) else None, axis=1)
df1.loc[:, 'FormRecords'] = df1.apply(lambda row: row['Assignments']['FormRecords'] if pd.notna(row['Assignments']) else None, axis=1)
df1.drop(columns=['Assignments'], inplace=True)

df1 = df1.explode('FormRecords')
df1.dropna(subset=['FormRecords'], inplace=True)
df1.loc[:, 'submittedDt'] = df1.apply(lambda row: row['FormRecords']['SubmittedTime'] if pd.notna(row['FormRecords']) else None, axis=1)
df1.loc[:, 'data'] = df1.apply(lambda row: row['FormRecords']['QuestionRecord'] if pd.notna(row['FormRecords']) else '[]', axis=1)
df1.loc[:, 'data'] = df1.apply(lambda row: json.loads(row['data']), axis=1)
df1.drop(columns=['FormRecords'], inplace=True)

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
df1.groupby(['FormId']).apply(lambda grp: df_list.append(grp), include_groups=False)

for df2 in df_list:
    max_no = df2['max_question'].max()
    columns_list = []
    if max_no > 0:
        for i in range(1, max_no+1):
            column_name1 = 'I_ADSSRES' + str(i)
            columns_list.append(column_name1)
            df2.loc[:, column_name1] = df2.apply(lambda row: row['data'][i-1]['answer'] if len(row['data']) >= i else None, axis=1)

            question_type = df2['data'].iat[0][i-1]['type'] if len(df2['data'].iat[0])>i else 0
            is_selection = question_type == 3 or question_type == 4
            column_name2 = column_name1 + ('_STD' if False else '_RAW')
            columns_list.append(column_name2)
            if is_selection:
                df2.loc[:, column_name2] = df2.apply(lambda row: row['data'][i-1]['answerUUid'] if len(row['data']) >= i else None, axis=1)
            else:
                df2.loc[:, column_name2] = df2[column_name1]

    df2.drop(columns=['data', 'max_question'], inplace=True)

    df3 = df2.assign(projectid=None, project=None, environmentName=None, subjectId=None, StudySiteId=None, siteid=None,
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
    df3.rename({'study_name': 'DataPageName', 'subject': 'Subject', 'site': 'Site'}, inplace=True, axis=1)
    columns = ['projectid', 'project', 'studyId', 'environmentName', 'subjectId', 'StudySiteId', 'Subject', 'siteid', 'Site', 'SiteNumber',
    'SiteGroup', 'instanceId', 'InstanceName', 'InstanceRepeatNumber', 'folderid', 'Folder', 'FolderName', 'FolderSeq', 'TargetDays', 'DataPageId',
    'DataPageName', 'PageRepeatNumber', 'RecordDate', 'RecordId', 'RecordPosition', 'MinCreated', 'MaxUpdated', 'SaveTS',
    'StudyEnvSiteNumber', 'I_ADSSASMDAT', 'I_ADSSASMDAT_RAW', 'I_ADSSASMDAT_INT', 'I_ADSSASMDAT_YYYY', 'I_ADSSASMDAT_MM', 'I_ADSSASMDAT_DD',]
    columns.extend(columns_list)
    columns.extend(['I_ADSSEVAL', 'I_ADSSEVAL_STD', 'I_QSCAT',])
    df4 = df3[columns]
    file_name = df4['I_QSCAT'].iat[0]
    print(f'generated file: {file_name}')
    df4.to_csv(f'./{file_name}.csv', index=False)

if __name__ == '__main__':
    pass