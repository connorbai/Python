from contextlib import contextmanager

from common.common_logger import logger
from database.db_pub import AdminPubDB
from database.db_trx import AdmintrxDB


@contextmanager
def connect_to_db():
    pubdb = AdminPubDB()
    trxdb = AdmintrxDB()
    try:
        pubdb.connect()
        trxdb.connect()
        yield pubdb, trxdb
    except Exception as e:
        logger.error('def connect_to_db -- error')
        raise e
    finally:
        pubdb.close()
        trxdb.close()


@contextmanager
def connect_to_pub():
    pubdb = AdminPubDB()
    try:
        pubdb.connect()
        yield pubdb
    except Exception as e:
        logger.error('def connect_to_pub -- error')
        raise e
    finally:
        pubdb.close()


@contextmanager
def connect_to_trx():
    trxdb = AdmintrxDB()
    try:
        trxdb.connect()
        yield trxdb
    except Exception as e:
        logger.error('def connect_to_trx -- error')
        raise e
    finally:
        trxdb.close()
