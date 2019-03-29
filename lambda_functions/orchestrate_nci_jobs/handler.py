import logging
import os
import boto3
from re import compile as compile_, IGNORECASE
from datetime import datetime
from raijin_ssh import exec_command

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
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
    Handler function responsible to detect any new emails sent to 'dea-ncimonitoring/NCIEmail' S3 bucket
     and extract PBS job related information (including dataset processing info) from the email body.

    The Architecture used in this setup is as follows:
        <New S3 object created> ---Delayed Event----> <SQS Queue> ---Event----> <AWS Lambda> --
                 ---Processed Data--|----Log--------> AWS CloudWatch
                                    |----Indexing---> AWS ElasticSearch Domain
    Ref: https://aws.amazon.com/blogs/
           1) compute/fanout-S3-event-notifications-to-multiple-endpoints/
           2) database/indexing-metadata-in-amazon-elasticsearch-service-using-aws-lambda-and-python/

    The Python lambda handler is responsible for:
        a) Extracting PBS related information from the email body
        b) Fetch dataset processing information from the PBS logs
        c) Construct AWS ES metadata document
        d) Connect to the Amazon ES domain endpoint
        e) Creates an index if index does not exists in the AWS ES
        f) Write the updated metadata document into Amazon ES
    """
    event_olist = list()
    for event_ilist in event:
        output, stderr, exit_code = exec_command(f'execute_fetch_job_ids --logfile {event_ilist["log_path"]}')

        # 'output' variable is in "1234567.r-man2,\n" format, hence remove new line character and any empty string
        qsub_job_ids = [ids for ids in output.strip().split(",") if ids]

        table = dynamodb.Table(os.environ['DYNAMODB_TABLENAME'])

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
                'work_dir': event_ilist["work_dir"],
                'queue_timestamp': now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                'timestamp': now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                'remarks': 'NA'
            }

            # write to the dynamoDB database
            table.put_item(Item=item)

        event_olist.append({
            'qsub_job_ids': qsub_job_ids,
            'product': event_ilist["product"],
            'queue_timestamp': now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'work_dir': event_ilist["work_dir"],
        })

    return {
        'event_olist': event_olist,
        'jobs_finished': 'PENDING',
    }


def submit_pbs_job(event, context):
    """
    Handler function responsible to detect any new emails sent to 'dea-ncimonitoring/NCIEmail' S3 bucket
     and extract PBS job related information (including dataset processing info) from the email body.

    The Architecture used in this setup is as follows:
        <New S3 object created> ---Delayed Event----> <SQS Queue> ---Event----> <AWS Lambda> --
                 ---Processed Data--|----Log--------> AWS CloudWatch
                                    |----Indexing---> AWS ElasticSearch Domain
    Ref: https://aws.amazon.com/blogs/
           1) compute/fanout-S3-event-notifications-to-multiple-endpoints/
           2) database/indexing-metadata-in-amazon-elasticsearch-service-using-aws-lambda-and-python/

    The Python lambda handler is responsible for:
        a) Extracting PBS related information from the email body
        b) Fetch dataset processing information from the PBS logs
        c) Construct AWS ES metadata document
        d) Connect to the Amazon ES domain endpoint
        e) Creates an index if index does not exists in the AWS ES
        f) Write the updated metadata document into Amazon ES
    """
    cmd = os.environ["SYNC_CMD"] % event
    LOG.info(f'Executing Command: {cmd}')

    output, stderr, exit_code = exec_command(f'{cmd}')

    if exit_code != 0:
        LOG.error(f'Exit code: {exit_code}')
        raise Exception(f'SSH Execution Command stdout: {output} stderr: {stderr}')

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
    Handler function responsible to detect any new emails sent to 'dea-ncimonitoring/NCIEmail' S3 bucket
     and extract PBS job related information (including dataset processing info) from the email body.

    The Architecture used in this setup is as follows:
        <New S3 object created> ---Delayed Event----> <SQS Queue> ---Event----> <AWS Lambda> --
                 ---Processed Data--|----Log--------> AWS CloudWatch
                                    |----Indexing---> AWS ElasticSearch Domain
    Ref: https://aws.amazon.com/blogs/
           1) compute/fanout-S3-event-notifications-to-multiple-endpoints/
           2) database/indexing-metadata-in-amazon-elasticsearch-service-using-aws-lambda-and-python/

    The Python lambda handler is responsible for:
        a) Extracting PBS related information from the email body
        b) Fetch dataset processing information from the PBS logs
        c) Construct AWS ES metadata document
        d) Connect to the Amazon ES domain endpoint
        e) Creates an index if index does not exists in the AWS ES
        f) Write the updated metadata document into Amazon ES
    """
    table = dynamodb.Table(os.environ['DYNAMODB_TABLENAME'])
    event_olist = list()

    jobs_finished = "FINISHED"

    for event_ilist in event['event_olist']:
        qsub_job_ids = list()

        for job_id in event_ilist["qsub_job_ids"]:
            output, stderr, exit_code = exec_command(f'execute_qstat --job-id {job_id}')
            now = datetime.now()  # Local Timestamp

            job_name = _extract_after_search_string(r"_job_name=*", output)
            job_state = _extract_after_search_string(r"_job_state=*", output)
            project = _extract_after_search_string(r"_project=*", output)
            queue = _extract_after_search_string(r"_queue=*", output)
            exit_status = _extract_after_search_string(r"_exit_status=*", output)
            qstat_comment = _extract_after_search_string(r"_comment= *", output)

            job_status = JOB_STATUS.get(job_state, 'UNKNOWN')
            execution_status = EXIT_STATUS.get(exit_status, 'FAILED')

            if not (job_status == 'FINISHED' or job_status == 'SUSPENDED'):
                qsub_job_ids.append(job_id)
            else:
                # Job has finished or deleted or suspended, if execution status still reports as IN_QUEUE then
                # update the correct status
                if job_status == 'SUSPENDED':
                    execution_status = 'JOB_SUSPENDED'
                else:
                    # Job has finished
                    execution_status = 'JOB_DELETED' if execution_status == 'IN_QUEUE' else execution_status

            item = {
                'pbs_job_id': job_id,
                'pbs_job_name': job_name,
                'project': project,
                'job_queue': queue,
                'job_status': job_status,
                'execution_status': execution_status,
                'timestamp': now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                'remarks': qstat_comment,
                'product': event_ilist["product"],
                'queue_timestamp': event_ilist["queue_timestamp"],
                'work_dir': event_ilist["work_dir"],
            }

            # write to the dynamoDB database
            table.put_item(Item=item)

        if qsub_job_ids:
            jobs_finished = "PENDING"  # Wait till all the jobs to complete execution

        event_olist.append({
            'qsub_job_ids': qsub_job_ids,
            'product': event_ilist["product"],
            'queue_timestamp': event_ilist["queue_timestamp"],
            'work_dir': event_ilist["work_dir"],
        })

    return {
        'event_olist': event_olist,
        'jobs_finished': jobs_finished,
    }
