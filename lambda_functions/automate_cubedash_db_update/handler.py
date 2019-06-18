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

    _update_cubedash_db_config(os.environ.get('CUBEDASH_CONFIG'),
                               message.split('\n')[0],
                               os.environ.get('BITBUCKET_BRANCH'))
