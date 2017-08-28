import os

LOGGING_PREFIX = 'orchestrator'

NCI_PROJECTS = [i.strip() for i in os.environ.get('DEA_NCI_PROJECTS', 'rs0,v10,u46,fk4,r78').split(',')]

AWS_REGION = os.environ.get('DEA_AWS_REGION', 'ap-southeast-2')

RAIJIN_USER = 'av8534'
RAIJIN_HOST = 'raijin.nci.org.au'
DEFAULT_SSM_KEY = 'orchestrator.raijin.login.test'
