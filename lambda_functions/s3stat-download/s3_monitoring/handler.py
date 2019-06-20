import logging
import os
import re
from datetime import datetime

import boto3

from .log_cfg import setup_logging
import logging 

import datetime
import requests


#today=datetime.date.today()
#yesterday = (today - datetime.timedelta(days =2)).strftime("%Y%m%d"))

bucket_name = "s3stat-monitoring"

file_path  = "/serverless/s3stat-download/dev/"


setup_logging()
LOG = logging.getLogger(__name__)

DEFAULT_SSM_USER_PATH = os.environ.get('SSM_USER_PATH')


def ssh_config_from_ssm_user_path(path=DEFAULT_SSM_USER_PATH):
    return {'userid': get_ssm_parameter(path + '.userid'),
            'password': get_ssm_parameter(path + '.password')}



SSM = None


def get_ssm_parameter(name, with_decryption=True):
    global SSM
    if SSM is None:
        SSM = boto3.client('ssm')

    response = SSM.get_parameter(Name=name, WithDecryption=with_decryption)

    if response:
        try:
            return response['Parameter']['Value']
        except (TypeError, IndexError):
            LOG.error("AWS SSM parameter not found in '%s'. Check regions match, SSM is region specific.", response)
            raise
    raise AttributeError("Key '{}' not found in SSM".format(name))



def handler(event, context):
    """Main Entry Point"""
    

    today=datetime.date.today()
    yesterday =(today - datetime.timedelta(days =2)).strftime("%Y%m%d")

    #ssm_keys = ssh_config_from_ssm_user_path()
    site_url = 'https://www.s3stat.com/Login.aspx?returnPath=%2fCustomer%2fMyAccount.aspx'
    #userid = ssm_keys['userid']
    #password = ssm_keys['password']

    file_url = "https://s3.amazonaws.com/reports.s3stat.com/17448/dea-public-data/stats/day"+ yesterday+".json"
    o_file = yesterday+".json"

# Create session
    s = requests.Session()

# GET request. This will generate cookie
    #s.get(site_url)

# Login to site
    #s.post(site_url, data={'_username': userid, '_password': password})

# Next thing will be to visit URL for file to download
    r = s.get(file_url)
    if r.status_code == requests.codes.ok:
        LOG.info("File downloaded sucessfully")

# Download file
        #tmp = open('/tmp/'+o_file, "rb")
        s3 = boto3.client('s3')

        with open('/tmp/'+o_file, 'wb') as output:
            output.write(r.content)

        s3.upload_file("/tmp/"+ o_file, bucket_name, o_file)



