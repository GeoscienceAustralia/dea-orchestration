import json
import logging
import boto3
import os
from ssh import exec_command

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


def _update_cubedash_db_config(updatefile, dbname, branch, execute_command=exec_command):
    execute_command(f'execute_update_cubedash_config --file {updatefile} --dbname {dbname} --bitbucket-branch {branch}')


def handler(event, context):
    """
    SNS handler
    """
    sns = boto3.client('sns')
    sns.create_topic(Name=os.environ.get('SNS_TOPIC'))
    LOG.info("Received event: " + json.dumps(event, indent=2))
    message = event['Records'][0]['Sns']['Message']

    LOG.info("Message from Sns: " + message)
    LOG.info("Message ID from Sns: " + event['Records'][0]['Sns']['MessageId'])
    LOG.info("Message timestamp from Sns: " + event['Records'][0]['Sns']['Timestamp'])

    bit_bucket_branch = message.split(':')[0].strip()
    db_name = message.split(':')[1].strip()

    if 'nci_' in db_name:
        update_file = os.environ.get('NCI_EXPLORER_FILE')
    elif 'ows_' in db_name:
        update_file = os.environ.get('OWS_EXPLORER_FILE') \
            if bit_bucket_branch == 'eks-prod' else \
            os.environ.get('OWS_DEV_EXPLORER_FILE')
    elif 'sandbox_' in db_name:
        update_file = os.environ.get('SANDBOX_EXPLORER_FILE')
    elif 'africa_' in db_name:
        update_file = os.environ.get('AFRICA_EXPLORER_FILE')
    else:
        LOG.info("DB Name (" + db_name + ") Error. Config file not updated")
        exit(1)

    _update_cubedash_db_config(update_file,
                               db_name,
                               bit_bucket_branch)
