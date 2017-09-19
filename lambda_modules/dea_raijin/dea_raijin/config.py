import os

LOGGING_PREFIX = 'orchestrator'

NCI_PROJECTS = [i.strip() for i in os.environ.get('DEA_NCI_PROJECTS', 'rs0,v10,u46,fk4,r78').split(',')]

AWS_REGION = os.environ.get('DEA_AWS_REGION', 'ap-southeast-2')

DEFAULT_SSM_USER_PATH = os.environ.get('DEA_RAIJIN_USER_PATH', 'orchestrator.raijin.users.default')

# should resolve to one of dev/test/prod
DEA_ENVIRONMENT = os.environ.get('DEA_ENVIRONMENT', 'dev')
