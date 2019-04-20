import logging
import sys


def setup_logging():
    # All lambda methods use this logging config.
    # Provides a single place where all log config/level/formatting is setup so that one
    # can see source file, line numbers, and any other desired log fields.
    ROOT_LOGGER = logging.getLogger()
    for hndlr in ROOT_LOGGER.handlers:
        ROOT_LOGGER.removeHandler(hndlr)
    HANDLER = logging.StreamHandler(sys.stdout)

    # use whatever format you want here
    FORMAT = '%(asctime)-15s %(process)d-%(thread)d %(name)s [%(filename)s:%(lineno)d] :%(levelname)8s: %(message)s'
    HANDLER.setFormatter(logging.Formatter(FORMAT))
    ROOT_LOGGER.addHandler(HANDLER)
    ROOT_LOGGER.setLevel(logging.DEBUG)

    # Suppress the more verbose modules
    logging.getLogger('__main__').setLevel(logging.DEBUG)
    logging.getLogger('botocore').setLevel(logging.WARN)
    logging.getLogger('paramiko').setLevel(logging.WARN)
    logging.getLogger('raijin_ssh').setLevel(logging.WARN)
    logging.getLogger('urllib3').setLevel(logging.WARN)
    logging.getLogger('elasticsearch').setLevel(logging.WARN)
