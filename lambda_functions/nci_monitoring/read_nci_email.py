import logging
import boto3
import urllib
from datetime import datetime
import copy
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

ES_INDEX = 'nci-monitor-'
ES_HOST = 'search-digitalearthaustralia-lz7w5p3eakto7wrzkmg677yebm.ap-southeast-2.es.amazonaws.com'
AWS_REGION = 'ap-southeast-2'
ES_PORT = 443
SIZE = {'KB': 1024, 'MB': 1024**2, 'GB': 1024**3, 'TB': 1024**4}

S3 = boto3.resource('s3')


def _push_metadata_to_es(doc):
    LOG.info('Connecting to the ES Endpoint, {%s}:{%s}', ES_HOST, ES_PORT)
    credentials = boto3.Session().get_credentials()
    auth = AWS4Auth(credentials.access_key, credentials.secret_key,
                    AWS_REGION, 'es', session_token=credentials.token)

    # Connect to the Amazon ES domain endpoint
    es = Elasticsearch(
        hosts=[{'host': ES_HOST, 'port': ES_PORT}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection)

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
    # Enable CPU calculation flag, if we have walltime, cputime, and ncpus in the email body
    cflag = False

    cpu_time = walltime = ncpus = exit_status = 0, 0, 0, 0
    job_id = job_name = exe_status = "NA", "NA", "NA"
    mem_used = vmem_used = "0kb", "0kb"
    job_efficiency = 0.0

    # Report when the job started
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

    return (cflag, job_efficiency, ncpus, job_time, job_id, job_name, exe_status,
            exit_status, mem_used, vmem_used)


def handler(event, context):
    """
    Handler function responsible to detect any new emails sent to 'dea-ncimonitoring/NCIEmail' S3 bucket
     and extract PBS job related information from the email body.

    The Architecture used in this setup is as follows:
        <New S3 object created> ---Event----> <AWS Lambda>---Processed Data--|----Log--------> AWS CloudWatch
                                                                             |----Indexing---> AWS ElasticSearch Domain
    Ref: https://aws.amazon.com/blogs/
           1) compute/fanout-s3-event-notifications-to-multiple-endpoints/
           2) database/indexing-metadata-in-amazon-elasticsearch-service-using-aws-lambda-and-python/

           The Python handler code does the following:
                a) Fetch PBS related information from the email body upon S3 event
                b) Connects to the Amazon ES domain endpoint
                c) Creates an index if index does not exists in the AWS ES
                d) Write the updated metadata document into Amazon ES
    """
    new_email = [(record['s3']['bucket']['name'], urllib.parse.unquote(record['s3']['object']['key'])) for record in
                 event.get('Records', [])]

    LOG.info("Changed/new object detected in the s3 bucket: %s", str(new_email))

    if new_email:
        # TODO
        # Fetch number of dataset processed by the PBS job and
        # calculate datasets processing efficiency. And write the updated metadata
        # document into AWS ES.
        # ndatasets = datasets_eff = "NA", "NA"

        for bucket, s3_key in new_email:
            LOG.info("Processing s3://{%s}/{%s}", str(bucket), str(s3_key))
            email_body = S3.Object(bucket, s3_key).get()['Body'].read().decode('utf-8')

            (cflag, job_efficiency, ncpus,
             job_time, job_id, job_name, exe_status,
             exit_status, mem_used, vmem_used) = _fetch_job_info(email_body)

            if cflag:
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
                            'memory (MB)': float(mem_used.split("kb")[0])/SIZE['MB'],
                            'virtual_memory (MB)': float(vmem_used.split("kb")[0])/SIZE['MB'],
                            'ncpus': int(ncpus),
                            # TODO
                            # 'ndatasets_processed': ndatasets,
                            # 'datasets_processing_efficiency': datasets_eff,
                        },
                    },
                })
    else:
        LOG.info("No new/updated email record found in the S3 bucket")
