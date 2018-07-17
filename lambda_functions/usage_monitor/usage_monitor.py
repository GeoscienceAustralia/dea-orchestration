import re
import boto3
from datetime import datetime

from elasticsearch import helpers

from dea_raijin import RaijinCommand
from dea_raijin.config import NCI_PROJECTS
from dea_raijin.utils import human2bytes

from dea_es import ES_CONNECTION as ES

CODE_BUCKET = 'datacube-core-deployment'
NCI_STORAGE = ['gdata1', 'gdata1a', 'gdata1b', 'gdata2', 'gdata3', 'short', 'massdata']
CLOUDWATCH_NAMESPACE = 'nci_metrics'
CLOUDWATCH_MAX_SEND = 20

CPU_FIELDS = ['cpu_grant', 'cpu_usage', 'cpu_avail', 'cpu_bonus_used']
STORAGE_FIELDS = ['grant', 'usage', 'avail', 'igrant', 'iusage', 'iavail']

CW_CLIENT = boto3.client('cloudwatch')

ES_INDEX = 'nci-quotausage-'


class UsageMonitorCommand(RaijinCommand):

    COMMAND_NAME = 'UsageMonitorCommand'

    def __init__(self):
        super().__init__(self)

    def command(self, *args, **kwargs):
        for project in NCI_PROJECTS:
            self.monitor_project(project)

    def monitor_project(self, project):
        now = datetime.utcnow()

        output, stderr, exit_code = self.raijin.exec_command('monitor {}'.format(project))

        if exit_code != 0:
            self.logger.error('Could not get quota report for %s, with exit code %s', project, exit_code)
            return

        usage = project_usage(output)
        assert project == usage['project']
        cloud_metrics = [make_metric(resource_name, resource_value, project, now)
                         for resource_name, resource_value in usage.items()
                         if resource_name not in ['project', 'period']]

        # Upload to cloud watch
        for some_cloud_metrics in [
                cloud_metrics[x:x + CLOUDWATCH_MAX_SEND] for x in
                range(0, len(cloud_metrics), CLOUDWATCH_MAX_SEND)]:

            CW_CLIENT.put_metric_data(
                Namespace=CLOUDWATCH_NAMESPACE,
                MetricData=some_cloud_metrics
            )

        # Upload to elastic Search
        update_template(ES)
        es_data = [dict(**usage)]

        es_data[0].update({
            '_index': ES_INDEX + now.strftime('%Y'),
            '_type': 'nci_quota_usage',
            '@timestamp': now.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        })

        summary = helpers.bulk(client=ES, actions=es_data)
        self.logger.info(summary)


def handler(event, context):
    return UsageMonitorCommand().run()


def make_metric(resource_name, resource_value, project, time):
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
            'cpu_grant': re.findall(r'.*Total Grant:\s+([\d\.]+) KSU.*', output, re.MULTILINE)[0],
            'cpu_usage': re.findall(r'.*Total Used:\s+([\d\.]+) KSU.*', output, re.MULTILINE)[0],
            'cpu_avail': re.findall(r'.*Total Avail:\s+([\d\.]+) KSU.*', output, re.MULTILINE)[0],
            'cpu_bonus_used': re.findall(r'.*Bonus Used:\s+([\d\.]+) KSU.*', output, re.MULTILINE)[0]
        }
        usage.update(cpu)
    except (TypeError, IndexError):
        pass
    for filesystem in NCI_STORAGE:
        storage = storage_usage(filesystem, output)
        usage.update(storage)
    return usage


def storage_usage(storage_pt, text):
    vals = re.findall(r'''%s\s+([\d\w\.]+)
                            \s+([\d\w\.]+)
                            \s+([\d\w\.]+)
                            \s+([\d\.]+[KM])
                            \s+([\d\.]+[KM])
                            \s+([\d\.]+[KM])''' % storage_pt, text, re.MULTILINE | re.X)
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


def human2decimal(s):
    unit = s[-1]
    val = float(s[:-1])
    if unit == 'K':
        return int(val * 1000)
    elif unit == 'M':
        return int(val * 1000000)
    else:
        raise ValueError('Error parsing "%s" into integer.' % s)
        return


def update_template(es):
    usage_template = {
        'template': ES_INDEX + '*',
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

    es.indices.put_template('nci-usage', body=usage_template)
