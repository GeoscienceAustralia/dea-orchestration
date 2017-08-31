import os

from dea_raijin.config import LOGGING_PREFIX, AWS_REGION
from dea_raijin.auth import get_ssm_parameter


_AWS_ES_ACCESS_KEY = os.environ.get('DEA_AWS_ES_ACCESS_KEY')
_AWS_PRIVATE_KEY = get_ssm_parameter(os.environ.get('DEA_AWS_ES_PRIVATE_KEY'))

HOST = os.environ.get('DEA_AWS_ES_HOST')
