import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import boto3

from es_connection import upload_to_elasticsearch
from ssh import exec_command
from utils import human2bytes, human2decimal

LOG = logging.getLogger(__name__)

NCI_PROJECTS = os.environ['NCI_PROJECTS'].split(',')
NCI_STORAGE = os.environ['NCI_STORAGE'].split(',')

CPU_FIELDS = ['cpu_grant', 'cpu_usage', 'cpu_avail', 'cpu_bonus_used']
STORAGE_FIELDS = ['grant', 'usage', 'avail', 'igrant', 'iusage', 'iavail']

CLOUDWATCH_NAMESPACE = 'nci_metrics'
CLOUDWATCH_MAX_SEND = 20

CLOUDWATCH = boto3.client('cloudwatch')
ES_INDEX = 'nci-quota-usage-'
ES_DOC_TYPE = 'nci_quota_usage'


def handler(event, context):
    """Main Entry Point"""
    with ThreadPoolExecutor() as executor:
        usages = executor.map(record_project_usage, NCI_PROJECTS)

        for usage in usages:
            LOG.info("Usage: %s", usage)


def record_project_usage(project):
    output, stderr, exit_code = exec_command('monitor {}'.format(project))

    if exit_code != 0:
        msg = 'Could not get quota report for %s, with exit code %s' % (project, exit_code)
        LOG.error(msg)
        raise RuntimeError(msg)

    usage = project_usage(output)
    assert project == usage['project']

    upload_to_cloudwatch_metrics(usage)
    upload_to_elasticsearch(usage, ES_INDEX, ES_DOC_TYPE)

    return usage


def upload_to_cloudwatch_metrics(usage):
    now = datetime.utcnow()
    cloud_metrics = [make_cw_metric(resource_name, resource_value, usage['project'], now)
                     for resource_name, resource_value in usage.items()
                     if resource_name not in ['project', 'period']]
    # Upload to cloud watch
    chunked_metrics = [cloud_metrics[x:x + CLOUDWATCH_MAX_SEND]
                       for x in range(0, len(cloud_metrics), CLOUDWATCH_MAX_SEND)]
    for some_cloud_metrics in chunked_metrics:
        CLOUDWATCH.put_metric_data(
            Namespace=CLOUDWATCH_NAMESPACE,
            MetricData=some_cloud_metrics
        )


def make_cw_metric(resource_name, resource_value, project, time):
    system, resource = resource_name.split('_')[:2]
    return {
        'MetricName': 'quota',
        'Dimensions': [
            {'Name': 'project', 'Value': project},
            {'Name': 'resource', 'Value': resource},
            {'Name': 'system', 'Value': system}
        ],
        'Timestamp': time,
        'Value': float(resource_value)
    }


def project_usage(output):
    usage = {
        'project': re.findall(r'.*Project=([\w\d]+)', output)[0],
        'period': re.findall(r'.*Compute Period=(.*?) .*', output, re.MULTILINE)[0],
    }
    try:
        cpu = {
            'cpu_grant': 1000 * float(re.findall(r'.*Total Grant:\s+([\d.]+) KSU.*', output, re.MULTILINE)[0]),
            'cpu_usage': 1000 * float(re.findall(r'.*Total Used:\s+([\d.]+) KSU.*', output, re.MULTILINE)[0]),
            'cpu_avail': 1000 * float(re.findall(r'.*Total Avail:\s+([\d.]+) KSU.*', output, re.MULTILINE)[0]),
            'cpu_bonus_used': 1000 * float(re.findall(r'.*Bonus Used:\s+([\d.]+) KSU.*', output, re.MULTILINE)[0])
        }
        usage.update(cpu)
    except (TypeError, IndexError):
        pass
    for filesystem in NCI_STORAGE:
        storage = storage_usage(filesystem, output)
        usage.update(storage)
    return usage


def storage_usage(storage_pt, text):
    vals = re.findall(r'''%s\s+([\d\w.]+)
                            \s+([\d\w.]+)
                            \s+([\d\w.]+)
                            \s+([\d.]+[KM])
                            \s+([\d.]+[KM])
                            \s+([\d.]+[KM])''' % storage_pt, text, re.MULTILINE | re.X)
    if vals:
        vals = vals[0]
        out = {
            '%s_grant' % storage_pt: human2bytes(vals[0]),
            '%s_usage' % storage_pt: human2bytes(vals[1]),
            '%s_avail' % storage_pt: human2bytes(vals[2]),
            '%s_igrant' % storage_pt: human2decimal(vals[3]),
            '%s_iusage' % storage_pt: human2decimal(vals[4]),
            '%s_iavail' % storage_pt: human2decimal(vals[5]),
        }
        return out
    return {}


def update_es_template(es_connection, index_prefix):
    template = {
        'template': index_prefix + '*',
        'mappings': {
            'nci_quota_usage': {
                'properties': {
                    'cpu_avail': {'type': 'float'},
                    'cpu_bonus_used': {'type': 'float'},
                    'cpu_grant': {'type': 'float'},
                    'cpu_usage': {'type': 'float'}
                }
            }
        }
    }

    es_connection.indices.put_template('nci-usage', body=template)
