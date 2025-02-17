from common import constants
from config import env_conf as env


def is_local():
    return env.env == constants.LOCAL


def is_inno():
    return env.env == constants.INNO