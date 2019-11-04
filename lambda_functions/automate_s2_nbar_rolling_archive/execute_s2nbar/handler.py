import logging
import os
import boto3

from .ssh import exec_command

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


def automate_rolling_archive(profile, s3_bucket, filelist, execute_command=exec_command):
    output, stderr, exit_code = execute_command(f'execute_s2nbar_rolling_archive --profile {profile} '
                                                f'--s3bucket {s3_bucket} --filelist {filelist}')

    if exit_code != 0:
        LOG.error('Could not execute execute_s2nbar_rolling_archive raijin script, exit code: {%s}', exit_code)
        raise Exception(f'SSH Execution Command stdout: {output} stderr: {stderr}')


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
    _s3_bucket = os.environ.get('S3_BUCKET')
    _profile = os.environ.get('AWS_PROFILE_NAME')

    LOG.info("Processing: " + str(_level1_done_filepath))
    s3 = boto3.client('s3')
    automate_rolling_archive(_profile, _s3_bucket, _level1_done_filepath)
