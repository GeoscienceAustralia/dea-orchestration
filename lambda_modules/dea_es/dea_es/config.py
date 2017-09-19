import os

from dea_raijin.config import LOGGING_PREFIX, AWS_REGION, DEA_ENVIRONMENT
from dea_raijin.auth import get_ssm_parameter

_DEFAULT_DEA_ES = 'search-digitalearthaustralia-lz7w5p3eakto7wrzkmg677yebm.ap-southeast-2.es.amazonaws.com'

HOST = os.environ.get('DEA_AWS_ES_HOST', _DEFAULT_DEA_ES)

_AWS_ES_ACCESS_KEY = os.environ.get('DEA_AWS_ES_ACCESS_KEY')
_AWS_PRIVATE_KEY = None

if _AWS_ES_ACCESS_KEY:
    # Run if the environment parameter is set
    _AWS_PRIVATE_KEY = get_ssm_parameter(os.environ.get('DEA_AWS_ES_PRIVATE_KEY'))
elif not _AWS_ES_ACCESS_KEY and DEA_ENVIRONMENT == 'dev':
    # Allow users to test with their local aws credentials
    import logging
    LOGGER = logging.getLogger(__name__)
    LOGGER.warning('WARNING: Using local aws access credentials, please set environment variables on lambda function')

    with open(os.path.expanduser('~/.aws/credentials'), 'r') as fd:
        for line in fd.readlines():
            try:
                k, v = line.split(' = ')
                if k == "aws_access_key_id":
                    _AWS_ES_ACCESS_KEY = v.strip()
                if k == "aws_secret_access_key":
                    _AWS_PRIVATE_KEY = v.strip()
            except ValueError:
                pass  # environment config
else:
    pass  # test environment
