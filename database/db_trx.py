from contextlib import contextmanager

from common.decorator import singleton
from database.database import Database
from secret_manager.index import SecretManager


# @singleton
class AdmintrxDB(Database):
    __secret_manager = None

    def __init__(self):
        super().__init__()
        self.__secret_manager = SecretManager()

    __instance = None

    def __new__(cls, *args, **kwd):
        if AdmintrxDB.__instance is None:
            AdmintrxDB.__instance = object.__new__(cls, *args, **kwd)
        return AdmintrxDB.__instance

    def connect(self):
        self.__secret_manager = SecretManager()
        host, port, dbname, usr, pwd = self.__secret_manager.get_trx_secret()
        super().connect(host, port, dbname, usr, pwd)

    @contextmanager
    def connect2(self):
        self.__secret_manager = SecretManager()
        host, port, dbname, usr, pwd = self.__secret_manager.get_pub_secret()
        try:
            yield super().connect(host, port, dbname, usr, pwd)
        finally:
            super().close()

    def get_connection(self):
        conn = super().get_connection()
        if conn:
            return conn
        else:
            self.connect()
            return super().get_connection()
