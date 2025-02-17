import logging
import os

# env
env = os.environ.get('ENVIRONMENT', None)
log_lvl = os.environ.get('LOG_LEVEL', logging.INFO)

# aws
buck_name = os.environ.get('BUCKET_NAME', None)
region = os.environ.get('REGION', None)
aws_ak = os.environ.get('AWS_ACCESS_KEY_ID', None)
aws_sk = os.environ.get('AWS_SECRET_ACCESS_KEY', None)

# database
pubdb_name = os.environ.get('DB_NAME_PUB', None)
trxdb_name = os.environ.get('DB_NAME_TRX', None)
db_username = os.environ.get('DB_USERNAME', None)
db_pwd = os.environ.get('DB_PASSWORD', None)
db_host = os.environ.get('DB_HOST', None)
db_port = os.environ.get('DB_PORT', None)

# secret manager
sm_subject = os.environ.get('PARTICIPANT_SM', None)
sm_study = os.environ.get('STUDY_SM', None)

# redis
rds_host = os.environ.get('RDS_HOST', None)
rds_port = os.environ.get('RDS_PORT', None)
rds_db_index = os.environ.get('RDS_INDEX', 0)

# aws lambda name
subject_response = os.environ.get('SUBJECT_RESPONSE', None)
