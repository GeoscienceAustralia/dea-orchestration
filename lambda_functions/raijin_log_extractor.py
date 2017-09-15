import boto3
import io
import yaml
import json
import datetime

import elasticsearch
from elasticsearch import helpers
from dateutil.parser import parse as date_parser

from dea_raijin import BaseCommand

from dea_es import ES_CONNECTION as ES

S3_CLIENT = boto3.client('s3')
SUBMISSION_INFO_FN = 'submission-info.yaml'

FILE_TO_LOGTYPE = {
    'out.log': 'info',
    'err.log': 'error'
}

ES_TEMPLATE_NAME = "dea-ncijob-log"
ES_TEMPLATE = {
    'template': 'dea-ncijob-log-*',
    'mappings': {
        'raijin_structured_log': {
            'properties': {
                'timestamp': {'type': 'date'},
                'event': {'type': 'text'},
                'user': {'type': 'text'},
                'node': {
                    'properties': {
                        'hostname': {'type': 'text'},
                        'runtime_id': {'type': 'text'},
                    },
                },
                'message': {'type': 'text'},
                'task_id': {'type': 'keyword'},
            },
        },
        'raijin_unstructured_log': {
            'properties': {
                'timestamp': {'type': 'date'},
                'event': {'type': 'text'},
                'message': {'type': 'text'},
                'logger': {'type': 'text'},
                'task_id': {'type': 'keyword'},
            },
        },
    }
}


def _extract_event_info(event):
    """_extract_event_info

    :param event: (str) event information from lambda trigger
    """
    return {
        'Key': event['s3']['object']['key'],
        'Bucket': event['s3']['bucket']['name']
    }


def _get_submission_info(key, bucket):
    """_get_submission_info returns the submission info of a raijin job
    from an s3 bucket

    :param key: (str) key of the associated logfile
    :param bucket: (str) bucket that the logfile is kept
    """
    info_key = key[:key.rfind('/') + 1] + SUBMISSION_INFO_FN

    try:
        submission_info = yaml.load(
            S3_CLIENT.get_object(Key=info_key, Bucket=bucket)['Body'].read()
        )
    except S3_CLIENT.exceptions.NoSuchKey:
        submission_info = {
            'pbs_job_id': 'UNKOWN'
        }

    return submission_info


class RaijinLogExtractor(BaseCommand):

    COMMAND_NAME = 'RaijinLogExtractor'

    def __init__(self, event):
        """Constructor_

        :param event: (dict) information passed in by s3
        """
        super().__init__(self)
        # self.s3_objects = map(_extract_event_info, event['Records'])
        self.s3_objects = [{
            'Bucket': 'dea-raijin-logs',
            'Key': 'test/one/two/008/err.log'
        }]

    def command(self):
        update_template(ES)

        for s3_object in self.s3_objects:
            self.process_task_output(s3_object)

    @staticmethod
    def process_structured_log(templ, line_no, json_log, last_timestamp):
        log = templ.copy()
        log.update(json_log)
        log['_id'] = log['_id'] + str(line_no)  # Add the line number to the id

        if 'timestamp' in log and isinstance(log['timestamp'], str):
            try:
                log['timestamp'] = date_parser(log['timestamp'])
            except ValueError:
                log['timestamp'] = None

        return log

    @staticmethod
    def process_unstructured_log(templ, line_range, msgs, timestamp):
        log = templ.copy()
        log['_id'] = log['_id'] + '{}_{}'.format(*line_range)
        log['message'] = '\n'.join(msgs)
        log['timestamp'] = timestamp

        return log

    @staticmethod
    def create_log_templates(s3_object, s3_metadata):
        submission_info = _get_submission_info(s3_object['Key'], s3_object['Bucket'])
        log_type = FILE_TO_LOGTYPE[s3_object['Key'].split('/')[-1]]
        index_name = ES_TEMPLATE_NAME + '-' + datetime.datetime.now().strftime('%Y-%m-%d')

        unstructured_log_tmpl = {
            '_id': s3_object['Key'].replace('/', '_') + '_',
            '_type': 'raijin_unstructured_log',
            '_index': index_name,
            'user': 'raijin_logs',
            'task_id': submission_info['pbs_job_id'],
            'event': s3_object['Key'].replace('/', '_') + '.' + log_type,
            'logger': 'raijin.unstructured'
        }

        structured_log_tmpl = {
            '_id': s3_object['Key'].replace('/', '_') + '_',
            '_type': 'raijin_structured_log',
            '_index': index_name,
            'user': 'raijin_logs',
            'task_id': submission_info['pbs_job_id']
        }

        return (structured_log_tmpl, unstructured_log_tmpl)

    def process_task_output(self, s3_object):
        """process_task_output

        :param s3_object: (dict) Key, Bucket definition to parse
        """
        s3_metadata = S3_CLIENT.get_object(**s3_object)
        last_known_timestamp = s3_metadata['LastModified']

        structured_log_tmpl, unstructured_log_tmpl = self.create_log_templates(s3_object, s3_metadata)

        structured_logs = []
        unstructured_logs = []
        unstructured_msg = []
        unstructured_range = [0, 0]

        buff = io.StringIO(s3_metadata['Body'].read().decode('utf-8'))

        for line_no, line in enumerate(buff.readlines()):
            if not line:
                continue  # skip empty lines
            try:
                json_load = json.loads(line)
                if unstructured_msg:
                    ulog = self.process_unstructured_log(
                        unstructured_log_tmpl, unstructured_range,
                        unstructured_msg, last_known_timestamp
                    )
                    unstructured_logs.append(ulog)
                    unstructured_msg = []
                    unstructured_range = [0, 0]

                log = self.process_structured_log(structured_log_tmpl, line_no, json_load, last_known_timestamp)
                if log['timestamp']:
                    last_known_timestamp = log['timestamp']
                structured_logs.append(log)
            except (TypeError, json.decoder.JSONDecodeError):

                if unstructured_msg:
                    unstructured_range[1] = line_no
                else:
                    unstructured_range[0] = line_no

                unstructured_msg.append(line)

        # Submit last unstructured msg
        if unstructured_msg:
            ulog = self.process_unstructured_log(
                unstructured_log_tmpl, unstructured_range,
                unstructured_msg, last_known_timestamp
            )
            unstructured_logs.append(ulog)

        # Upload logs
        if structured_logs:
            helpers.bulk(client=ES, actions=structured_logs)

        # Upload logs
        if unstructured_logs:
            helpers.bulk(client=ES, actions=unstructured_logs)


def update_template(es):
    try:
        es.indices.put_template(name=ES_TEMPLATE_NAME, body=ES_TEMPLATE, create=True)
    except elasticsearch.exceptions.RequestError:
        pass  # The template already exists


def handler(event, context):
    return RaijinLogExtractor(event).run()
