from datetime import datetime, timedelta

import boto3
import os
import logging
import io
import paramiko
from botocore.exceptions import ClientError

logger = logging.getLogger("handler")
logger.setLevel(logging.getLevelName(os.environ.get("LOGGER_DEBUG_LEVEL", "INFO")))
"""
    TEST
"""
# secret_name = "sftpmihconnection"
# sftp_host = "filetransfer-t.lilly.com"
# remote_dir = '/lly-edp-departure-us-east-2-qa/rapid_mih_china'
# BUCKET = 'lly-cn-ibu-cmds-ods-qa-private'
# PROCESS_BUCKET = 'lly-cn-ibu-cmds-qa-private'
"""
    PROD
"""
secret_name = "sftpmihconnection-prd"
sftp_host = "filetransfer.lilly.com"
remote_dir = '/lly-edp-departure-us-east-2-prod/rapid_mih_china'
BUCKET = 'lly-cn-ibu-cmds-ods-prd-private'
PROCESS_BUCKET = 'lly-cn-ibu-cmds-prd-private'

user_name = "extaftx0048"  ## TEST Prod
PROCESS_FOLDER = 'cmds-glue/cmds-glue-process-ods-td-to-postgresql'
ECDP_PROCESS_FOLDER = 'cmds-glue/cmds-glue-process-ecdp-to-postgresql'

def get_db_secret_mgr(secret_id):
    try:
        SESSION = boto3.session.Session()
        client = SESSION.client(service_name='secretsmanager', region_name='cn-northwest-1')
        get_secret_value_response = client.get_secret_value(SecretId=secret_id)
        db_conn_dict = get_secret_value_response['SecretString']
        logger.info("secret details read from Secret Manager")
        return db_conn_dict
    except Exception as exp:
        logger.error("Cannot read secret details from Secret Manager")
        raise exp


def sftp_conn(sftp_key):
    try:
        logger.info("key received")
        ssh = paramiko.SSHClient()
        keyfile = io.StringIO(sftp_key)
        logger.info("generated key file")
        rsa_key = paramiko.RSAKey.from_private_key(keyfile)
        logger.info("key RSA KEY")
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print('ssh connecting')
        ssh.connect(hostname=sftp_host, port=22, username=user_name, pkey=rsa_key)
        print('ssh connected')
        aws_sftp = ssh.open_sftp()
        return aws_sftp
    except ClientError as e:
        logger.error("[FAILED]: Process Failed. Please see error details below")
        logger.error(e)
    finally:
        logger.info("Exited sftp_conn method.")


def lambda_handler(event, context):
    credential_dict = get_db_secret_mgr(secret_name)
    aws_sftp = sftp_conn(credential_dict)
    files_check=aws_sftp.listdir(remote_dir)
    print('files_check:', files_check)
    today_str = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    # filename
    algnmnt_file = 'ADS_CUST_ALGNMNT_VW_' + today_str + '.json'
    tier_file = 'ADS_CUST_ALGNMNT_TIER_VW_' + today_str + '.json'
    ecdp_file = 'ecdp_data_' + today_str + '.json'
    algnmnt_key = f'glue/{today_str}/{algnmnt_file}'
    tier_key = f'glue/{today_str}/{tier_file}'
    ecdp_key = f'glue/{today_str}/{ecdp_file}'
    s3 = boto3.client('s3')
    with open('/tmp/done.rdy', 'w') as file:
        file.write('done')

    if ecdp_file in files_check:
        print("File: {} is find done.".format(ecdp_file))
        # download
        aws_sftp.get(f'{remote_dir}/{ecdp_file}', f'/tmp/{ecdp_file}')
        # upload_file
        s3.upload_file(f'/tmp/{ecdp_file}', BUCKET, ecdp_key)
        print("File: {} moving to backup folder done.".format(ecdp_file))
        # copy file to process folder
        s3.copy(CopySource={'Bucket': BUCKET, 'Key': ecdp_key},
                Bucket=PROCESS_BUCKET, Key=f'{ECDP_PROCESS_FOLDER}/ecdp_data.json')
        print("File: {} moving to process folder done.".format(ecdp_file))
        # upload done file
        s3.upload_file('/tmp/done.rdy', PROCESS_BUCKET, f'{ECDP_PROCESS_FOLDER}/done.rdy')
        print("File done: {} uploaded.".format('done.rdy'))
    else:
        print("File: {} is not find done.".format(ecdp_file))


    if algnmnt_file in files_check and tier_file in files_check:
        print("File: {} is find done.".format(ecdp_file))
        # download
        aws_sftp.get(f'{remote_dir}/{algnmnt_file}', f'/tmp/{algnmnt_file}')
        aws_sftp.get(f'{remote_dir}/{tier_file}', f'/tmp/{tier_file}')
        # upload_file
        s3.upload_file(f'/tmp/{algnmnt_file}', BUCKET, algnmnt_key)
        s3.upload_file(f'/tmp/{tier_file}', BUCKET, tier_key)
        print("File: {}, {} moving to backup folder done.".format(algnmnt_file, tier_file))
        # copy file to process folder
        s3.copy(CopySource={'Bucket': BUCKET, 'Key': algnmnt_key},
                Bucket=PROCESS_BUCKET, Key=f'{PROCESS_FOLDER}/ADS_CUST_ALGNMNT.json')
        s3.copy(CopySource={'Bucket': BUCKET, 'Key': tier_key},
                Bucket=PROCESS_BUCKET, Key=f'{PROCESS_FOLDER}/ADS_CUST_ALGNMNT_TIER.json')
        print("File: {}, {} moving to process folder done.".format(algnmnt_file, tier_file))
        # upload done file
        s3.upload_file('/tmp/done.rdy', PROCESS_BUCKET, f'{PROCESS_FOLDER}/done.rdy')
        print("File done: {} uploaded.".format('done.rdy'))
    else:
        print("File: {} is not find done.".format(ecdp_file))