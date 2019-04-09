import logging
import os
import boto3
from re import compile as compile_, IGNORECASE
from datetime import datetime
from dateutil.tz import gettz
import time
from raijin_ssh import exec_command

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

_DYNAMODB = boto3.resource('dynamodb')
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

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


def _extract_after_search_string(str_val, out_str):
    p = compile_(str_val, IGNORECASE)
    # Extract string after the substring
    # Split newline character before returning the string
    return p.split(out_str)[1].split('\n')[0]


def fetch_job_ids(event, context):
    """
    This handler function shall do the following:
       1) Read the job submission log file
       2) Fetch the job id's from the file
       3) Update the job status of all the jobs in aws dynamodb table
       4) Return the job id's, product, and work_dir as an event output dictionary along with
          jobs pending execution status
    """
    event_olist = list()
    table = _DYNAMODB.Table(os.environ['DYNAMODB_TABLENAME'])

    for event_ilist in event:
        # Wait a bit until ssh socket is available.
        # This is to avoid multiple access of ssh socket during parallel state machine execution.
        time.sleep(5)  # Sleep 5s, this time should be less than timeout of the lambda function

        output, stderr, _ = exec_command(f'execute_fetch_job_ids --logfile {event_ilist["log_path"]}')

        if not output:
            LOG.error('execute_fetch_job_ids command execution failed (stderr: %r)', stderr)
            raise Exception(f'SSH execution command stdout: {output}')

        # 'output' variable is in "1234567.r-man2,\n" format, hence remove new line character and any empty string
        qsub_job_ids = set(ids for ids in output.strip().split(",") if ids)

        for job_id in qsub_job_ids:
            now = datetime.now(gettz("Australia/Sydney"))  # Local Timestamp
            table.update_item(Key={'pbs_job_id': job_id},
                              UpdateExpression="SET pbs_job_name = :jname, "
                                               "product = :prod, "
                                               "job_project = :proj, "
                                               "job_queue = :queue, "
                                               "job_status = :jstatus, "
                                               "execution_status = :estatus, "
                                               "queue_timestamp = :tqueue,"
                                               "updated_timestamp = :tstamp, "
                                               "work_dir = :wdir, "
                                               "remarks = :comments",
                              ExpressionAttributeValues={
                                  ":jname": event_ilist["job_name"],
                                  ":prod": event_ilist["product"],
                                  ":proj": event_ilist["project"],
                                  ":queue": event_ilist["job_queue"],
                                  ":jstatus": event_ilist["job_status"],
                                  ":estatus": event_ilist["execution_status"],
                                  ":tqueue": now.strftime(DATETIME_FORMAT),
                                  ":tstamp": now.strftime(DATETIME_FORMAT),
                                  ":wdir": event_ilist["work_dir"],
                                  ":comments": 'NA'}
                              )

        event_olist.append({
            'qsub_job_ids': list(qsub_job_ids),  # A `set()` cannot be serialized to JSON, hence convert to a list()
            'product': event_ilist["product"],
            'work_dir': event_ilist["work_dir"],
        })

    return {
        'event_olist': event_olist,
        'jobs_finished': False,
    }


def submit_pbs_job(event, context):
    """
    The Python lambda handler is responsible for:
        a) SSH to raijin login node
        b) Upon successful login, execute the ssh command
        c) If ssh command execution is successful, return submitted job status read from the ssh output
        d) If ssh command execution failed, raise an SSH command exception
    """
    cmd = event["execute_cmd"] % event
    LOG.info('Executing Command: %r', cmd)

    output, stderr, _ = exec_command(f'{cmd}')

    if not output:
        raise Exception(f'SSH execution command stdout is empty, {output}')

    job_name = _extract_after_search_string(r"pbs_job_name=*", output)
    log_path = _extract_after_search_string(r"_log=*", output)
    project = _extract_after_search_string(r"_project=*", output)
    queue = _extract_after_search_string(r"_job_queue=*", output)
    work_dir = _extract_after_search_string(r"_work_dir=*", output)
    product = _extract_after_search_string(r"_product=*", output)

    return {
        "log_path": log_path,
        "job_name": job_name,
        "product": product,
        "project": project,
        "job_queue": queue,
        "job_status": "IN_QUEUE",
        "execution_status": "IN_QUEUE",
        "work_dir": work_dir,
    }


def check_job_status(event, context):
    """
    The Python lambda handler shall:
        a) Loop through all the event list inputted to this handler from the parallel state machines
        b) From the 'event_olist' dictionary element, loop through all the job ids and qstat to read
           pbs job status
        c) Once job has finished, record any job failures in job_failed flag
        d) Update the pbs job status in the aws dynamodb table
        e) Return the job id's, product, and work_dir as an event output dictionary along with
           jobs pending execution status
    """
    table = _DYNAMODB.Table(os.environ['DYNAMODB_TABLENAME'])
    event_olist = list()
    pending_jobs = list()
    jobs_failed = False

    # Loop through all the event list inputted to this handler from the parallel state machines
    for event_ilist in event['event_olist']:
        qsub_job_ids = list()

        # From each event list, fetch qsub job id's
        for job_id in event_ilist["qsub_job_ids"]:
            # Wait a bit until ssh socket is available.
            # This is to avoid multiple access of ssh socket during parallel state machine execution.
            time.sleep(5)  # Sleep 5s, this time should be less than timeout of the lambda function

            output, stderr, _ = exec_command(f'execute_qstat --job-id {job_id}')

            if not output:
                raise Exception(f'SSH execution command: execute_qstat command execution failed {stderr}')

            queue_time = datetime.strptime(_extract_after_search_string(r"_queue_time=*", output),
                                           '%a %b %d %H:%M:%S %Y')

            job_status = JOB_STATUS.get(_extract_after_search_string(r"_job_state=*", output),
                                        'UNKNOWN')
            execution_status = EXIT_STATUS.get(_extract_after_search_string(r"_exit_status=*", output),
                                               'FAILED')

            if job_status not in ('FINISHED', 'SUSPENDED') and execution_status == 'IN_QUEUE':
                # Job is still pending
                qsub_job_ids.append(job_id)
            else:
                # Job has finished or the job is deleted/suspended
                if job_status == 'SUSPENDED':
                    execution_status = 'JOB_SUSPENDED'
                else:
                    # When a job is deleted, job status is reported as completed. In this scenario update the execution
                    # status as JOB_DELETED
                    execution_status = 'JOB_DELETED' if execution_status == 'IN_QUEUE' else execution_status

                # Once a job has failed, report entire batch job execution as failed
                jobs_failed = True if execution_status != 'SUCCESS' else jobs_failed

            # Write to the dynamoDB database
            table.update_item(Key={'pbs_job_id': job_id},
                              UpdateExpression="SET pbs_job_name = :jname, "
                                               "product = :prod, "
                                               "job_project = :proj, "
                                               "job_queue = :queue, "
                                               "job_status = :jstatus, "
                                               "execution_status = :estatus, "
                                               "queue_timestamp = :tqueue,"
                                               "updated_timestamp = :tstamp, "
                                               "work_dir = :wdir, "
                                               "remarks = :comments",
                              ExpressionAttributeValues={
                                  ":jname": _extract_after_search_string(r"_job_name=*", output),
                                  ":prod": event_ilist["product"],
                                  ":proj": _extract_after_search_string(r"_project=*", output),
                                  ":queue": _extract_after_search_string(r"_queue=*", output),
                                  ":jstatus": job_status,
                                  ":estatus": execution_status,
                                  ":tqueue": queue_time.strftime(DATETIME_FORMAT),
                                  ":tstamp": datetime.now(gettz("Australia/Sydney")).strftime(DATETIME_FORMAT),
                                  ":wdir": event_ilist["work_dir"],
                                  ":comments": _extract_after_search_string(r"_comment= *", output)}
                              )

        event_olist.append({
            'qsub_job_ids': qsub_job_ids,
            'product': event_ilist["product"],
            'work_dir': event_ilist["work_dir"],
        })
        pending_jobs.extend(qsub_job_ids)

    if not pending_jobs and jobs_failed:
        # Jobs has been deleted or aborted
        return {
            'event_olist': event['event_olist'],  # Pass event so that we can qstat job id's and update dynamodb
            'jobs_finished': -1,  # Report failure as something happened during batch execution
        }

    return {
        'event_olist': event_olist,
        'jobs_finished': not pending_jobs,
    }


def state_failed(event, context):
    """
    The State Machine Failed Python lambda handler shall:
        a) Delete the job if it has started execution or waiting in queue
        b) Update the pbs job status in the aws dynamodb table
    """
    table = _DYNAMODB.Table(os.environ['DYNAMODB_TABLENAME'])
    jobs_failed = False

    for event_ilist in event['event_olist']:
        for job_id in event_ilist["qsub_job_ids"]:
            # Wait a bit until ssh socket is available.
            # This is to avoid multiple access of ssh socket during parallel state machine execution.
            time.sleep(5)  # Sleep 5s, this time should be less than timeout of the lambda function

            output, stderr, _ = exec_command(f'execute_qstat --job-id {job_id}')

            if not output:
                LOG.error('execute_qstat command execution failed (stderr: %r)', stderr)
                raise Exception(f'SSH execution command stdout: {output}')

            job_state = _extract_after_search_string(r"_job_state=*", output)
            queue_time = datetime.strptime(_extract_after_search_string(r"_queue_time=*", output),
                                           '%a %b %d %H:%M:%S %Y')

            job_status = JOB_STATUS.get(job_state, 'UNKNOWN')
            execution_status = EXIT_STATUS.get(_extract_after_search_string(r"_exit_status=*", output),
                                               'FAILED')

            if job_status not in ('FINISHED', 'SUSPENDED'):
                # Job is still pending, hence delete them
                exec_command(f'execute_qdel --job-id {job_id}')
                execution_status = 'JOB_DELETED'
            else:
                # Job has finished or the job is deleted/suspended
                if job_status == 'SUSPENDED':
                    execution_status = 'JOB_SUSPENDED'
                else:
                    # When a job is deleted, job status is reported as completed. In this scenario update the execution
                    # status as JOB_DELETED
                    execution_status = 'JOB_DELETED' if execution_status == 'IN_QUEUE' else execution_status

            # Report entire batch job execution as failed
            jobs_failed = True if execution_status != 'SUCCESS' else jobs_failed

            # Read from the dynamoDB database
            LOG.info('Read dynamodb key {"pbs_job_id": str(%r)} value', job_id)
            result = table.get_item(Key={"pbs_job_id": job_id})

            # Write to the dynamoDB database
            table.update_item(Key={'pbs_job_id': job_id},
                              UpdateExpression="SET pbs_job_name = :jname, "
                                               "product = :prod, "
                                               "job_project = :proj, "
                                               "job_queue = :queue, "
                                               "job_status = :jstatus, "
                                               "execution_status = :estatus, "
                                               "queue_timestamp = :tqueue,"
                                               "updated_timestamp = :tstamp, "
                                               "work_dir = :wdir, "
                                               "remarks = :comments",
                              ExpressionAttributeValues={
                                  ":jname": _extract_after_search_string(r"_job_name=*", output),
                                  ":prod": result["product"],
                                  ":proj": _extract_after_search_string(r"_project=*", output),
                                  ":queue": _extract_after_search_string(r"_queue=*", output),
                                  ":jstatus": job_status,
                                  ":estatus": execution_status,
                                  ":tqueue": queue_time.strftime(DATETIME_FORMAT),
                                  ":tstamp": datetime.now(gettz("Australia/Sydney")).strftime(DATETIME_FORMAT),
                                  ":wdir": result["work_dir"],
                                  ":comments": _extract_after_search_string(r"_comment= *", output)}
                              )
