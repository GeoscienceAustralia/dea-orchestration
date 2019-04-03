import logging
import os
import boto3
from re import compile as compile_, IGNORECASE
from datetime import datetime
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
    for event_ilist in event:
        # Wait a bit until ssh socket is available.
        # This is to avoid multiple access of ssh socket during parallel state machine execution.
        time.sleep(10)  # Sleep 10s, this time should be less than timeout of the lambda function

        output, stderr, _ = exec_command(f'execute_fetch_job_ids --logfile {event_ilist["log_path"]}')

        if not output:
            LOG.error('execute_fetch_job_ids command execution failed (stderr: %r)', stderr)
            raise Exception(f'SSH execution command stdout: {output}')

        # 'output' variable is in "1234567.r-man2,\n" format, hence remove new line character and any empty string
        qsub_job_ids = set(ids for ids in output.strip().split(",") if ids)

        table = _DYNAMODB.Table(os.environ['DYNAMODB_TABLENAME'])

        for job_id in qsub_job_ids:
            now = datetime.now()  # Local Timestamp
            item = {
                'pbs_job_id': job_id,
                'pbs_job_name': event_ilist["job_name"],
                'product': event_ilist["product"],
                'project': event_ilist["project"],
                'job_queue': event_ilist["job_queue"],
                'job_status': event_ilist["job_status"],
                'execution_status': event_ilist["execution_status"],
                'queue_timestamp': now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                'timestamp': now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                'work_dir': event_ilist["work_dir"],
                'remarks': 'NA',
            }

            # Write to the dynamoDB database
            table.put_item(Item=item)

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

    jobs_failed = False

    # Loop through all the event list inputted to this handler from the parallel state machines
    for event_ilist in event['event_olist']:
        qsub_job_ids = list()

        # From each event list, fetch qsub job id's
        for job_id in event_ilist["qsub_job_ids"]:
            # Wait a bit until ssh socket is available.
            # This is to avoid multiple access of ssh socket during parallel state machine execution.
            time.sleep(10)  # Sleep 10s, this time should be less than timeout of the lambda function

            output, stderr, _ = exec_command(f'execute_qstat --job-id {job_id}')

            if not output:
                LOG.error('execute_qstat command execution failed (stderr: %r)', stderr)
                raise Exception(f'SSH execution command stdout: {output}')

            job_state = _extract_after_search_string(r"_job_state=*", output)
            queue_time = _extract_after_search_string(r"_exit_status=*", output)

            job_status = JOB_STATUS.get(job_state, 'UNKNOWN')
            execution_status = EXIT_STATUS.get(_extract_after_search_string(r"_exit_status=*", output),
                                               'FAILED')

            if job_status not in ('FINISHED', 'SUSPENDED'):
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

            item = {
                'pbs_job_id': job_id,
                'pbs_job_name': _extract_after_search_string(r"_job_name=*", output),
                'product': event_ilist["product"],
                'project': _extract_after_search_string(r"_project=*", output),
                'job_queue': _extract_after_search_string(r"_queue=*", output),
                'job_status': job_status,
                'execution_status': execution_status,
                'queue_timestamp': queue_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                'timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                'work_dir': event_ilist["work_dir"],
                'remarks': _extract_after_search_string(r"_comment= *", output),
            }

            # Write to the dynamoDB database
            table.put_item(Item=item)

        event_olist.append({
            'qsub_job_ids': qsub_job_ids,
            'product': event_ilist["product"],
            'work_dir': event_ilist["work_dir"],
        })

    return {
        'event_olist': event_olist,
        'jobs_finished': jobs_failed,
    }
