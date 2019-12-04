import logging
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from dateutil.tz import gettz
import time
from raijin_ssh import exec_command
import re

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

JOB_STATUS = {
    'F': 'FINISHED',
    'R': 'RUNNING',
    'H': 'ON_HOLD',
    'Q': 'IN_QUEUE',
    'S': 'SUSPENDED',
    'E': 'EXITING'
}

EXIT_STATUS = {
    '0': 'SUCCESS',
    # Ignore if pbs job returns exit status of 2 to indicate incorrect usage (generally invalid or missing arguments).
    # This scenario normally arises when the state machine in parallel detects no further tasks to process.
    '2': 'SUCCESS',
    '271': 'JOB_TERMINATED',
    'NA': 'IN_QUEUE'
}

SYNC_PREFIX_PATH = {
    'ls8_nbar_scene': '/g/data/rs0/scenes/nbar-scenes-tmp/ls8/',
    'ls7_nbar_scene': '/g/data/rs0/scenes/nbar-scenes-tmp/ls7/',
    'ls8_nbart_scene': '/g/data/rs0/scenes/nbar-scenes-tmp/ls8/',
    'ls7_nbart_scene': '/g/data/rs0/scenes/nbar-scenes-tmp/ls7/',
    'ls8_pq_scene': '/g/data/rs0/scenes/pq-scenes-tmp/ls8/',
    'ls7_pq_scene': '/g/data/rs0/scenes/pq-scenes-tmp/ls7/',
    'ls8_pq_legacy_scene': '/g/data/rs0/scenes/pq-legacy-scenes-tmp/ls8/',
    'ls7_pq_legacy_scene': '/g/data/rs0/scenes/pq-legacy-scenes-tmp/ls7/'
}

SYNC_SUFFIX_PATH = {
    'ls8_nbar_scene': '/??/output/nbar/',
    'ls7_nbar_scene': '/??/output/nbar/',
    'ls8_nbart_scene': '/??/output/nbart/',
    'ls7_nbart_scene': '/??/output/nbart/',
    'ls8_pq_scene': '/??/output/pqa/',
    'ls7_pq_scene': '/??/output/pqa/',
    'ls8_pq_legacy_scene': '/??/output/pqa/',
    'ls7_pq_legacy_scene': '/??/output/pqa/'
}

COG_S3PREFIX_PATH = {
    'wofs_albers': 's3://dea-public-data/WOfS/WOFLs/v2.1.5/combined',
    'ls8_fc_albers': 's3://dea-public-data/fractional-cover/fc/v2.2.1/ls8',
    'ls7_fc_albers': 's3://dea-public-data/fractional-cover/fc/v2.2.1/ls7'
}

TAG_NAMES = {
    'ls7_fc_albers': 'ls7_fc',
    'ls8_fc_albers': 'ls8_fc',
    'wofs_albers': 'wofls'
}

DAM_SCRIPT_PATH = {
    'wofs_dam_script': '/g/data/r78/vmn547/Dams/Dams_scripts/append_water_bodies.sh',
}

EXECUTE_COMMAND = {
    'SYNC': os.environ.get('SYNC_CMD'),
    'INGEST': os.environ.get('INGEST_CMD'),
    'FC': os.environ.get('FC_CMD'),
    'WOFS': os.environ.get('WOFS_CMD'),
    'COG': os.environ.get('COG_CMD'),
    'DAM_SCRIPT': os.environ.get('DAM_SCRIPT_CMD'),
}

PRODUCTS = {
    'SYNC': os.environ.get('SYNC_PRODUCTS'),
    'INGEST': os.environ.get('INGEST_PRODUCTS'),
    'FC': os.environ.get('FC_PRODUCTS'),
    'WOFS': os.environ.get('WOFS_PRODUCTS'),
    'COG': os.environ.get('COG_PRODUCTS'),
    'DAM_SCRIPT': os.environ.get('DAM_SCRIPT_PRODUCTS'),
}

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


def _extract_after_search_string(expression, string):
    match = re.search(expression, string)
    if match is not None:
        # Remove multiple space and new line character in the search result
        return re.sub('\n  +', '', match.group(1))

    # Did not find any matching string
    return "NA"


def _add_records_to_dynamodb(table, event_idict, output):
    # 'output' variable is in "1234567.r-man2,\n" format, hence remove new line character and any empty string
    qsub_job_ids = set(ids for ids in output.strip().split(",") if ids)

    for jobid in qsub_job_ids:
        now = datetime.now(gettz("Australia/Sydney"))  # Local Timestamp
        LOG.info('Update %r job status in dynamodb', jobid)
        table.update_item(Key={'pbs_job_id': jobid},
                          UpdateExpression="SET pbs_job_name = :jname, "
                                           "product = :prod, "
                                           "job_project = :proj, "
                                           "job_queue = :queue, "
                                           "job_status = :jstatus, "
                                           "execution_status = :estatus, "
                                           "queue_timestamp = :tqueue, "
                                           "updated_timestamp = :tstamp, "
                                           "work_dir = :wdir, "
                                           "remarks = :comments",
                          ExpressionAttributeValues={
                              ":jname": event_idict["job_name"],
                              ":prod": event_idict["product"],
                              ":proj": event_idict["project"],
                              ":queue": event_idict["job_queue"],
                              ":jstatus": event_idict["job_status"],
                              ":estatus": event_idict["execution_status"],
                              ":tqueue": now.strftime(DATETIME_FORMAT),
                              ":tstamp": now.strftime(DATETIME_FORMAT),
                              ":wdir": event_idict["work_dir"],
                              ":comments": 'NA'}
                          )
    return qsub_job_ids


def _update_job_status_in_dynamodb(table, job_id, job_state, execution_status, comments='NA'):
    now = datetime.now(gettz("Australia/Sydney"))  # Local Timestamp
    table.update_item(Key={'pbs_job_id': job_id},
                      UpdateExpression="SET job_status = :jstatus, "
                                       "execution_status = :estatus, "
                                       "updated_timestamp = :tstamp, "
                                       "remarks = :comments",
                      ExpressionAttributeValues={
                          ":jstatus": job_state,
                          ":estatus": execution_status,
                          ":tstamp": now.strftime(DATETIME_FORMAT),
                          ":comments": comments},
                      )


def _delete_job(jobid):
    exec_command(f'execute_qdel --job-id {jobid}')


def _execute_qstat_command(job_id, execute_command=exec_command):
    output, stderr, _ = execute_command(f'execute_qstat --job-id {job_id}')

    if not output or output == 'NA':
        LOG.error('"execute_qstat --job-id %r" command failed (stderr: %r)', job_id, stderr)
        raise Exception(f'SSH execution command stdout: {output}')

    comment = _extract_after_search_string('comment = (.*\n.*)', output)
    return f"""_job_name={_extract_after_search_string('Job_Name = (.*)', output)}
_job_state={_extract_after_search_string('job_state = (.*)', output)}
_project={_extract_after_search_string('project = (.*)', output)}
_queue={_extract_after_search_string('queue = (.*)', output)}
_exit_status={_extract_after_search_string('Exit_status = (.*)', output)}
_comment={comment}
_queue_time={_extract_after_search_string('qtime = (.*)', output)}
"""


def _execute_fetch_jobid_command(log_path, execute_command=exec_command):
    output, stderr, _ = execute_command(f'execute_fetch_job_ids --logfile {log_path}')

    if not output:
        LOG.error('"execute_fetch_job_ids --logfile %r" command failed (stderr: %r)', log_path, stderr)
        raise Exception(f'SSH execution command stdout: {output}')

    return output


def _get_job_status(qsub_job_ids, job_id, jobs_failed, output):
    job_status = JOB_STATUS.get(_extract_after_search_string("_job_state=(.*)", output),
                                'UNKNOWN')
    execution_status = EXIT_STATUS.get(_extract_after_search_string("_exit_status=(.*)", output),
                                       'FAILED')
    comments = _extract_after_search_string("_comment=(.*)", output)

    if job_status in 'FINISHED':
        # Update the status as JOB_DELETED when execution status from qstat is 'IN_QUEUE'
        execution_status = 'JOB_DELETED' if execution_status == 'IN_QUEUE' else execution_status

        # Once a job has failed, report entire batch job execution as failed
        jobs_failed = True if execution_status != 'SUCCESS' else jobs_failed

        qsub_job_ids -= {job_id}  # Job completed, remove from the list
    elif job_status in 'SUSPENDED':
        execution_status = 'JOB_SUSPENDED'

        qsub_job_ids -= {job_id}  # Job is deleted/suspended, remove from the list
    else:
        # Job is still pending, hence append the job id to qsub job id's list
        qsub_job_ids |= {job_id}

    return qsub_job_ids, jobs_failed, job_status, execution_status, comments


def _process_job(input_list, table, exe_cmd=exec_command):
    """
    This function does the following:
        a) Loop through all the the job ids and qstat to read pbs job status
        b) Once job has finished, record any job failures in job_failed flag
        c) Update the pbs job status in the aws dynamodb table
    """
    pending_jobs = list()
    job_failed = False

    for qsub_dict in input_list:
        res = _execute_fetch_jobid_command(qsub_dict["log_path"], exe_cmd)
        qsub_job_ids = set(ids for ids in res.strip().split(",") if ids)

        # From each qsub dictionary, fetch qsub job id's
        for job_id in qsub_dict["qsub_job_ids"]:
            # Wait a bit until ssh socket is available.
            # This is to avoid multiple access of ssh socket during parallel state machine execution.
            time.sleep(1)  # Sleep 1s

            # qstat job id and fetch job status
            output = _execute_qstat_command(job_id, exe_cmd)

            # From qstat output fetch qsub job ids, job status, job failed status and execution status
            qsub_job_ids, job_failed, job_status, exe_status, comments = \
                _get_job_status(qsub_job_ids, job_id, job_failed, output)

            # Update the job status in the dynamodb table
            _update_job_status_in_dynamodb(table, job_id, job_status, exe_status, comments)

        pending_jobs.extend(qsub_job_ids)
    return set(pending_jobs), job_failed


def _fetch_and_update(input_list, table, exe_cmd=exec_command):
    """
    This function shall do the following:
       1) Read the job submission log file
       2) Fetch the job id's from the file
       3) Update the job status of all the jobs in aws dynamodb table
       4) Return job status
    """
    olist = list()
    for qsub_dict in input_list:
        # Wait a bit until ssh socket is available.
        # This is to avoid multiple access of ssh socket during parallel state machine execution.
        time.sleep(1)  # Sleep 1s

        output = _execute_fetch_jobid_command(qsub_dict["log_path"], exe_cmd)

        # Add a new record to the dynamodb table if it does not already exist, or update an existing record
        qsub_job_ids = _add_records_to_dynamodb(table, qsub_dict, output)

        olist.append({
            **qsub_dict,
            **{'qsub_job_ids': list(qsub_job_ids)}  # A `set()` cannot be serialized to JSON, hence convert to a list()
        })

    return olist


def create_dynamodb_table(event, context):
    """
    Create a new dynamodb table (if it does not exists) or notify user if:
     1) Table exists
     2) User do not have sufficient permission to create/update table
     3) Unknown error resulted while creating the dynamodb table
    """
    _dynamodb = boto3.resource('dynamodb')
    tablename = os.environ.get('DYNAMODB_TABLENAME')
    LOG.info("Fetch dynamodb table name: %r", tablename)

    try:
        # Create new table
        table = _dynamodb.create_table(
            TableName=tablename,
            KeySchema=[
                {
                    'AttributeName': 'pbs_job_id',
                    'KeyType': 'HASH'  # Partition key
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'pbs_job_id',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

        # Wait until the table is created
        table.meta.client.get_waiter('table_exists').wait(TableName=tablename)
    except ClientError as err:
        if err.response['Error']['Code'] == 'ResourceInUseException':
            LOG.debug("Dynamodb table (%r) exists", tablename)
        elif err.response['Error']['Code'] == 'AccessDeniedException':
            # Some users do not have required IAM role to enable dynamodb table auto scaling
            LOG.warning("User do not have required IAM permissions to create dynamodb table (%r).\nError: %r",
                        tablename,
                        err)
        else:
            LOG.error("Unexpected error: %r", err)

    return ['SYNC', 'INGEST', 'WOFS', 'FC', 'COG', 'DAM_SCRIPT', 'DONE']


def fetch_job_ids(event, context):
    """
    This handler call query job id and update the status in dynamodb table
    """
    _dynamodb = boto3.resource('dynamodb')
    table = _dynamodb.Table(os.environ.get('DYNAMODB_TABLENAME'))

    event_olist = _fetch_and_update(event['event_olist'], table)

    return event_olist


def submit_pbs_job(event, context):
    """
    The Python lambda handler is responsible for:
        a) SSH to raijin login node
        b) Upon successful login, execute the ssh command
        c) If ssh command execution is successful, return submitted job status
        d) If ssh command execution failed, raise an SSH command exception
    """
    event_olist = list()
    products = PRODUCTS.get(event, "UNKNOWN").split(',')
    year = os.environ.get('YEAR')

    for product in products:
        cmd = EXECUTE_COMMAND.get(event, "UNKNOWN") % {
            "product": product,
            "path": SYNC_PREFIX_PATH.get(product, "UNKNOWN"),
            "suffixpath": SYNC_SUFFIX_PATH.get(product, "UNKNOWN"),
            "s3_output": COG_S3PREFIX_PATH.get(product, "UNKNOWN"),
            "tag": TAG_NAMES.get(product, "UNKNOWN"),
            "trasharchived": "no",
            "year": f'{year}',
            "time_range": f'{year}-{year}',
            "dam_script_path": DAM_SCRIPT_PATH.get(product, "UNKNOWN"),
        }
        LOG.info('Executing Command: %r', cmd)

        output, stderr, _ = exec_command(f'{cmd}')

        if not output:
            raise Exception(f'SSH execution command stdout is empty, {output}')

        job_name = _extract_after_search_string("pbs_job_name=(.*)", output)
        log_path = _extract_after_search_string("_log=(.*)", output)
        project = _extract_after_search_string("_project=(.*)", output)
        queue = _extract_after_search_string("_job_queue=(.*)", output)
        work_dir = _extract_after_search_string("_work_dir=(.*)", output)
        product = _extract_after_search_string("_product=(.*)", output)

        event_olist.append({
            "log_path": log_path,
            "job_name": job_name,
            "product": product,
            "project": project,
            "job_queue": queue,
            "job_status": "IN_QUEUE",
            "execution_status": "IN_QUEUE",
            "work_dir": work_dir,
        })

    return event_olist


def check_job_status(event, context):
    """
        Check job status and return job finished status
    """
    _dynamodb = boto3.resource('dynamodb')
    table = _dynamodb.Table(os.environ.get('DYNAMODB_TABLENAME'))

    pending_jobs, job_failed = _process_job(event['event_olist'], table)

    if not pending_jobs and job_failed:
        # Jobs has been deleted or aborted
        LOG.info('Job failed / deleted / aborted')
        LOG.info('Pending Job List: %r', pending_jobs)
        LOG.info('Job Failed Flag: %r', job_failed)
        return -1

    return not pending_jobs


def state_failed(event, context):
    """
    The python lambda handler shall:
        a) Delete the job if it has started execution or waiting in queue
        b) Update the pbs job status in the aws dynamodb table
    """
    _dynamodb = boto3.resource('dynamodb')
    table = _dynamodb.Table(os.environ.get('DYNAMODB_TABLENAME'))
    events_list = event['event_olist']

    for qsub_dict in events_list:
        for job_id in qsub_dict["qsub_job_ids"]:
            # Wait a bit until ssh socket is available.
            # This is to avoid multiple access of ssh socket during parallel state machine execution.
            time.sleep(5)  # Sleep 5s

            # Delete a qsub job
            _delete_job(job_id)

            # Update the job status in the dynamodb table
            _update_job_status_in_dynamodb(table, job_id, 'FINISHED', 'JOB_DELETED')
