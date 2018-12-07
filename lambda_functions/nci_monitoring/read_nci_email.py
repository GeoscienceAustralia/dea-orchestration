import copy
import logging
import urllib
from datetime import datetime
import boto3
import json
from es_connection import get_es_connection
from re import compile, IGNORECASE
from raijin_ssh import exec_command

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

ES_INDEX = 'nci-monitor-'
SIZE = {'KB': 1024, 'MB': 1024 ** 2, 'GB': 1024 ** 3, 'TB': 1024 ** 4}

S3 = boto3.resource('s3')

_ndatasets_found = 0
_nfiles_created = 0
_nfiles_create_fail = 0
_nds_index_pass = 0
_nds_index_fail = 0
_service_units = 0.0


def _pattern(str_val):
    return compile(str_val, IGNORECASE)


def _fetch_ds_count(re_exp, output):
    dsline = _pattern(re_exp).findall(output)
    if dsline:
        return _pattern(r'[0-9.+-]{1,}').findall(dsline[0])[0]
    return 0


def _push_metadata_to_es(doc):
    es = get_es_connection()

    now = datetime.utcnow()
    doc = copy.deepcopy(doc)
    doc.update({
        '@timestamp': now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
    })

    LOG.info(doc)
    index = ES_INDEX + now.strftime('%Y-%m-%d')

    # Create an index (if one has not already been created) and
    # write the metadata document into AWS Elasticsearch
    retval = es.index(index=index, doc_type='doc', body=doc)
    LOG.info(retval)
    es.indices.refresh(index=index)


def _fetch_job_info(email_body):
    global _ndatasets_found, _nfiles_created, _nfiles_create_fail
    global _nds_index_pass, _nds_index_fail, _service_units
    # CPU calculation flag: If we have walltime, cputime, and ncpus in the email body then set this flag
    cflag = False

    cpu_time = walltime = ncpus = exit_status = 0, 0, 0, 0
    job_id = job_name = exe_status = "NA", "NA", "NA"
    mem_used = vmem_used = "0kb", "0kb"
    job_efficiency = 0.0

    for line in email_body.split('\r\n'):
        if "resources_used.walltime" in line:
            val = line.split("=")[1]
            walltime = sum(int(i) * 60 ** index for index, i in enumerate(val.split(":")[::-1]))
        elif "resources_used.cput" in line:
            val = line.split("=")[1]
            cpu_time = sum(int(i) * 60 ** index for index, i in enumerate(val.split(":")[::-1]))
            cflag = True
        elif "resources_used.ncpus" in line:
            ncpus = line.split("=")[1]
        elif "Date: " in line:
            job_time = line.split(": ")[1]
        elif "PBS Job Id" in line:
            job_id = line.split(": ")[1]
        elif "Job Name" in line:
            job_name = line.split(": ")[1]
        elif "Execution" in line or "execution" in line:
            exe_status = line
            LOG.info(str(exe_status))
        elif "Exit_status" in line:
            exit_status = line.split("=")[1]
        elif "resources_used.mem" in line:
            mem_used = line.split("=")[1]
        elif "resources_used.vmem" in line:
            vmem_used = line.split("=")[1]

    # Do not perform job efficiency calculation for the jobs currently executing or just started
    if cflag:
        job_efficiency = (float(cpu_time) / float(walltime) / float(ncpus)) * 100

        output, stderr, exit_code = exec_command(f'execute_fetch_dataset_info --job-id {job_id.strip()}')

        if exit_code != 0:
            logging.error(f'Could not get qstat report for {job_id.strip()}, exit code: {exit_code}')
            raise Exception(f'SSH Execution Command stdout: {output} stderr: {stderr}')

        _ndatasets_found = int(_fetch_ds_count(r'_ndatasets_found=[0-9.+-]{1,}', output))
        _nfiles_created = int(_fetch_ds_count(r'_nfiles_created=[0-9.+-]{1,}', output))
        _nfiles_create_fail = int(_fetch_ds_count(r'_nfiles_create_fail=[0-9.+-]{1,}', output))
        _nds_index_pass = int(_fetch_ds_count(r'_nds_index_pass=[0-9.+-]{1,}', output))
        _nds_index_fail = int(_fetch_ds_count(r'_nds_index_fail=[0-9.+-]{1,}', output))
        _service_units = float(_fetch_ds_count(r'_service_units=[0-9.+-]{1,}', output))

    return (cflag, job_efficiency, ncpus, job_time, job_id, job_name, exe_status,
            exit_status, mem_used, vmem_used)


def handler(event, context):
    """
    Handler function responsible to detect any new emails sent to 'dea-ncimonitoring/NCIEmail' S3 queue
     and extract PBS job related information from the email body.

    The Architecture used in this setup is as follows:
        <New S3 object created> ---Delayed Event----> <SQS Queue> ---Event----> <AWS Lambda> --
                 ---Processed Data--|----Log--------> AWS CloudWatch
                                    |----Indexing---> AWS ElasticSearch Domain
    Ref: https://aws.amazon.com/blogs/
           1) compute/fanout-S3-event-notifications-to-multiple-endpoints/
           2) database/indexing-metadata-in-amazon-elasticsearch-service-using-aws-lambda-and-python/

           The Python handler code does the following:
                a) Fetch PBS related information from the email body upon S3 event
                b) Connects to the Amazon ES domain endpoint
                c) Creates an index if index does not exists in the AWS ES
                d) Write the updated metadata document into Amazon ES
    """
    message = [record['body'] for record in event.get('Records', [])]
    email_record = json.loads(message[0])["Records"][0]

    new_email = [(email_record['s3']['bucket']['name'],
                  urllib.parse.unquote(email_record['s3']['object']['key']))]

    if new_email:
        LOG.info("Changed/new object notification received from S3 bucket to the sqs queue")
        for bucket, s3_key in new_email:
            LOG.info(f"Processing S3 message://{bucket}/{s3_key}")
            email_body = S3.Object(bucket, s3_key).get()['Body'].read().decode('utf-8')

            (cflag, job_efficiency, ncpus,
             job_time, job_id, job_name, exe_status,
             exit_status, mem_used, vmem_used) = _fetch_job_info(email_body)

            if cflag:
                nds_failed = max(_nfiles_create_fail, _nds_index_fail)
                ds_efficiency = 0.0

                # Dataset Efficiency (%) = (Total number of datasets - number of datasets failed) / total number of ds
                if _ndatasets_found > 0:
                    ds_efficiency = float((_ndatasets_found - nds_failed) / _ndatasets_found) * 100

                # Push the metadata coming from every trigger generated by
                # object creation events in S3 bucket
                _push_metadata_to_es({
                    'pbsjob_raijin': {
                        "info": {
                            'job_id': job_id.strip(),
                            'job_name': job_name.strip(),
                            'job_time': job_time,
                            'execution_status': exe_status,
                            'exit_status': int(exit_status),
                        },
                        'details': {
                            'job_efficiency (%)': job_efficiency,
                            'memory (MB)': float(mem_used.split("kb")[0]) / SIZE['MB'],
                            'virtual_memory (MB)': float(vmem_used.split("kb")[0]) / SIZE['MB'],
                            'ncpus': int(ncpus),
                            'ndatasets_to_process': _ndatasets_found,
                            'nfile_creation_pass': _nfiles_created,
                            'nfile_creation_fail': _nfiles_create_fail,
                            'ndatasets_index_pass': _nds_index_pass,
                            'ndatasets_index_fail': _nds_index_fail,
                            'service_units_used': _service_units,
                            'datasets_processing_efficiency (%)': ds_efficiency,
                        },
                    },
                })
    else:
        LOG.info("No new/updated email record found in the S3 bucket")
