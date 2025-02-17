from contextlib import contextmanager

from database.database import Database
from secret_manager.index import SecretManager


# @singleton
class AdminPubDB(Database):
    __secret_manager = None

    def __init__(self):
        super(AdminPubDB, self).__init__()
        self.__cursor = None

    __instance = None

    def __new__(cls, *args, **kwd):
        if AdminPubDB.__instance is None:
            AdminPubDB.__instance = object.__new__(cls, *args, **kwd)
        return AdminPubDB.__instance

    def connect(self):
        self.__secret_manager = SecretManager()
        host, port, dbname, usr, pwd = self.__secret_manager.get_pub_secret()
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


