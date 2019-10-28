import boto3
import logging
import os
from ssh import exec_command

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


def automate_rolling_archive(s3_client, s3_bucket, filelist, execute_command=exec_command):
    execute_command(f'execute_s2nbar_rolling_archive --s3client {s3_client} --s3bucket {s3_bucket} '
                    f'--filelist {filelist}')


def handler(event, context):
    """
    SNS handler

    Level 1 process done list is generated manually till wagl code is updated to push sns notification
    that the Sentinel-2 Definitive ard processing is completed

    # raijin$ module use /g/data/v10/public/modules/modulefiles/
    # raijin$ module use /g/data/v10/private/modules/modulefiles/
    # raijin$ module load wagl/5.4.0
    # raijin$ batch_summary --indir /g/data/v10/work/s2_ard/wagl/batchid-21e0db8763/ --outdir=$PWD
    """
    _level1_done_filepath = os.environ.get('LEVEL1_DONE_LIST')
    _aws_profile = os.environ.get('AWS_PROFILE')
    _s3_bucket = os.environ.get('S3_BUCKET')

    # Create an S3 client
    session = boto3.Session(profile_name=_aws_profile)
    s3_client = session.client('s3')

    LOG.info("Processing: " + str(_level1_done_filepath))
    automate_rolling_archive(s3_client, _s3_bucket, _level1_done_filepath)
