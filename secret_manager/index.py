import json

import boto3

from common.common_logger import logger
from common.utils import is_local, is_inno
from config import env_conf as env


class SecretManager:
    __client = None

    def __init__(self):
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name='us-east-2')
        self.__client = client

    def get_secret(self, secret_id):
        get_secret_value_response = self.__client.get_secret_value(SecretId=secret_id)
        secret = get_secret_value_response['SecretString']
        json_secret = json.loads(secret)
        host = json_secret['host']
        port = json_secret['port']
        dbname = json_secret['dbname']
        usr = json_secret['username']
        pwd = json_secret['password']
        return host, port, dbname, usr, pwd

    def get_pub_secret(self):
        if is_local() or is_inno():
            host, port, dbpub, dbtrx, usr, pwd = self.get_local_env()
            return host, port, dbpub, usr, pwd
        if env.sm_subject is None:
            logger.error('get trx_secret: None')
        return self.get_secret(env.sm_subject)

    def get_trx_secret(self):
        if is_local() or is_inno():
            host, port, dbpub, dbtrx, usr, pwd = self.get_local_env()
            return host, port, dbtrx, usr, pwd
        if env.sm_study is None:
            logger.error('get trx_secret: None')
        return self.get_secret(env.sm_study)

    @staticmethod
    def get_local_env():
        return env.db_host, env.db_port, env.pubdb_name, env.trxdb_name, env.db_username, env.db_pwd

