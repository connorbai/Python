import logging
import warnings

from common.utils import is_local, is_inno
from config import env_conf as env

if not is_local():
    warnings.filterwarnings('ignore')

print(f"--------------------LOG_LEVEL: {env.log_lvl}-----------------------")
log_level = logging.getLevelName(env.log_lvl)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)


if is_local() or is_inno():
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)

logger.debug('-------------------init logging level end-------------------')
