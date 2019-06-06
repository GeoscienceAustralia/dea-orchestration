import json
import logging
import boto3
import os
from raijin_ssh import exec_command

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


def _update_cubedash_db_config(updatefile, dbname, branch):
    exec_command(f'execute_update_cubedash_config --file {updatefile} --dbname {dbname} --bitbucket-branch {branch}')


def handler(event, context):
    """
    test handler
    """
    sns = boto3.client('sns')
    response = sns.create_topic(Name=os.environ.get('SNS_TOPIC'))
    LOG.info("Received event: " + json.dumps(event, indent=2))
    message_id = event['Records'][0]['Sns']['MessageId']
    message = event['Records'][0]['Sns']['Message']
    message_timestamp = event['Records'][0]['Sns']['Timestamp']
    LOG.info("Message from Sns: " + message)
    LOG.info("Message ID from Sns: " + message_id)
    LOG.info("Message timestamp from Sns: " + message_timestamp)
    _update_cubedash_db_config(os.environ.get('CUBEDASH_CONFIG'),
                               message.split('\n')[0],
                               os.environ.get('BITBUCKET_BRANCH'))
