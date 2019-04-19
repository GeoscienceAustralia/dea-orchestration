import logging
import sys


# All lambda methods use this logging config.
# Provides a single place where all log config/level/formatting is setup so that one
# can see source file, line numbers, and any other desired log fields.
LOG = logging.getLogger()
for hndlr in LOG.handlers:
    LOG.removeHandler(hndlr)
HANDLER = logging.StreamHandler(sys.stdout)

# use whatever format you want here
FORMAT = '%(asctime)-15s %(process)d-%(thread)d %(name)s [%(filename)s:%(lineno)d] :%(levelname)8s: %(message)s'
HANDLER.setFormatter(logging.Formatter(FORMAT))
LOG.addHandler(HANDLER)
LOG.setLevel(logging.DEBUG)

# Suppress the more verbose modules
logging.getLogger('__main__').setLevel(logging.DEBUG)
logging.getLogger('botocore').setLevel(logging.WARN)
logging.getLogger('paramiko').setLevel(logging.WARN)
logging.getLogger('raijin_ssh').setLevel(logging.WARN)
logging.getLogger('urllib3').setLevel(logging.WARN)
logging.getLogger('elasticsearch').setLevel(logging.WARN)
