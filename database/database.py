from contextlib import contextmanager

import pandas as pd
import psycopg2

from common.common_logger import logger
from common.utils import is_local, is_inno
from database.db_log import LoggedCursor


class Database:
    __connection = None
    __cursor = None

    def __init__(self):
        pass

    def connect(self, host, port, dbname, usr, pwd):
        if self.__connection and not self.__connection.closed:
            return self.__connection
        try:
            conn = psycopg2.connect(user=usr, password=pwd, host=host, port=port, database=dbname)
            self.__connection = conn
            logger.info("connect database successfully")
            return conn
        except Exception as e:
            logger.error("connect database error: %s", str(e))
            raise e

    def get_connection(self):
        if not self.__connection or self.__connection.closed:
            logger.error('getConnection Error')
        else:
            return self.__connection

    def close(self):
        if self.__cursor:
            self.__cursor.close()
            self.__cursor = None
        if self.__connection:
            self.__connection.close()
            self.__connection = None
            logger.info("disconnect database successfully")
        return True

    def cursor(self):
        if self.__cursor and not self.__cursor.closed:
            return self.__cursor
        else:
            if is_local() or is_inno():
                self.__cursor = self.get_connection().cursor(cursor_factory=LoggedCursor)
            else:
                self.__cursor = self.get_connection().cursor()
        return self.__cursor

    @contextmanager
    def cur(self):
        if is_local() or is_inno():
            __cursor = self.get_connection().cursor(cursor_factory=LoggedCursor)
        else:
            __cursor = self.get_connection().cursor()
        try:
            yield __cursor
        finally:
            if __cursor and not __cursor.closed:
                __cursor.close()
                del __cursor

    def fetchone(self, sql_str, params=None):
        with self.cur() as cur:
            cur.execute(sql_str, params)
            row = cur.fetchone()
            logger.debug("fetchall count: %s", 1 if row is not None else 0)
            return row

    def fetchall(self, sql_str, params=None):
        with self.cur() as cur:
            cur.execute(sql_str, params)
            rows = cur.fetchall()
            logger.debug("fetchall count: %s", len(rows))
            return rows

    def update(self, sql_str, params=None):
        with self.cur() as cur:
            cur.execute(sql_str, params)
            self.get_connection().commit()
            logger.debug("update count: %s", cur.rowcount)
            return cur.rowcount

    def chunk_update_df(self, sql_str, df, chunk_size=100):
        """
        :param df: DataFrame
        :param chunk_size: default 100
        :param sql_str: [str]
        :return:
        """
        with self.cur() as cur:
            try:
                for i in range(0, len(df), chunk_size):
                    data = df.iloc[i:i + chunk_size]
                    len1 = len(data)
                    records = tuple(data.values.flatten())
                    cur.execute(sql_str * len1, records)
                self.get_connection().commit()
                logger.debug("chunk_update_df count: %s", cur.rowcount)
            except Exception as e:
                logger.info("chunk_update transaction.")
                self.get_connection().rollback()
                raise e

    def fetch_dataframe(self, sql_str, params=None, dtype=None):
        with self.cur() as cur:
            cur.execute(sql_str, params)
            rows = cur.fetchall()
            logger.debug("fetch_dataframe count: %s", len(rows))
            return pd.DataFrame(rows, columns=[x.name for x in cur.description], dtype=dtype)

    def insert(self, sql_str, params=None):
        conn = self.get_connection()
        with self.cur() as cur:
            try:
                cur.execute(sql_str, params)
                logger.debug("insert count: %s", cur.rowcount)
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.info("insert transaction.")
                raise e

    def insert_df(self, sql_str, df):
        if len(df) == 0:
            return None
        with self.cur() as cur:
            conn = self.get_connection()
            try:
                df.loc[:, 'id'] = None
                df1 = df.iloc[:, :-1]
                for index in df1.index:
                    data = df1.loc[index]
                    cur.execute(sql_str + ' RETURNING id', tuple(data))
                    insert_id = cur.fetchone()[0]
                    conn.commit()
                    df.loc[index, 'id'] = insert_id
            except Exception as e:
                conn.rollback()
                logger.info("batch_insert transaction.")
                raise e

    def chunk_insert_df(self, sql_str: str, df: pd.DataFrame, batch_size=100):
        conn = self.get_connection()
        with self.cur() as cur:
            try:
                for i in range(0, len(df), batch_size):
                    data = df.iloc[i:i + batch_size]
                    records = [tuple(x) for x in data.values]
                    cur.executemany(sql_str, records)
                    logger.debug("chunk_insert_df count: %s", cur.rowcount)
                conn.commit()
            except Exception as e:
                logger.info("chunk_insert transaction.")
                conn.rollback()
                raise e

    def delete(self, sql_str, params=None):
        conn = self.get_connection()
        with self.cur() as cur:
            try:
                cur.execute(sql_str, params)
                logger.debug("delete count: %s", cur.rowcount)
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.info("delete transaction.")
                raise e
