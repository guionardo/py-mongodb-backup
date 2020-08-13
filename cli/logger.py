import logging

LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}

LOG_FORMAT = '%(asctime)s %(name)-8s %(levelname)-8s %(message)s'


def get_level(level: str):
    return LEVELS.get(level, logging.INFO)


def setup_logger(level: str):
    logging.basicConfig(format=LOG_FORMAT, level=get_level(level))
