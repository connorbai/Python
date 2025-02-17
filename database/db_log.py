import psycopg2
from common.common_logger import logger


class LoggedCursor(psycopg2.extensions.cursor):
    def execute(self, sql, parameters=None):
        logger.debug(self.mogrify(sql.replace('\n', ''), parameters))
        super().execute(sql, parameters)
