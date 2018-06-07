import yaml
import io
import datetime
import json

from dateutil.parser import parse as parse_date

import boto3
import elasticsearch
from elasticsearch import helpers

from dea_raijin import BaseCommand
from dea_es import ES_CONNECTION as ES

S3_CLIENT = boto3.client('s3')


class ESHandler(object):

    TEMPLATE_NAME = 'dea-ncijob-log'
    TEMPLATE = {
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

    def __init__(self, es_connection):
        self.es = es_connection
        self.index_name = self.TEMPLATE_NAME + '-' + datetime.datetime.now().strftime('%Y-%m-%d')
        self.log_templates = {}
        self.log_data = {}

        self.update_es_templates()

    def update_es_templates(self, skip_if_present=True):
        try:
            self.es.indices.put_template(name=self.TEMPLATE_NAME, body=self.TEMPLATE, create=skip_if_present)
        except elasticsearch.exceptions.RequestError:
            if not skip_if_present:
                raise

    def init_log_templates(self, metadata):
        self.log_templates = {
            'structured': {
                '_id': metadata['key_prefix'] + '_{}',
                '_type': 'raijin_structured_log',
                '_index': self.index_name,
                'user': 'raijin_logs',
                'task_id': metadata['pbs_job_id']
            },
            'unstructured': {
                '_id': metadata['key_prefix'] + '_{}',
                '_type': 'raijin_unstructured_log',
                '_index': self.index_name,
                'user': 'raijin_logs',
                'task_id': metadata['pbs_job_id'],
                'event': metadata['key_prefix'] + '_' + metadata['log_type'],
                'logger': 'raijin.unstructured'
            }
        }

        for key in self.log_templates:
            self.log_data[key] = []

    def process_log(self, log_template, processed_msg):
        log_msg = self.log_templates[log_template].copy()
        log_msg.update(processed_msg)
        log_msg['_id'] = log_msg['_id'].format(processed_msg['line_no'])

        self.log_data[log_template].append(log_msg)

    def submit_logs(self):
        for key in self.log_templates:
            if self.log_data[key]:
                helpers.bulk(client=self.es, actions=self.log_data[key])


class S3Handler(object):

    SUBMISSION_INFO_FN = 'subbmission-info.yaml'

    def __init__(self, event_info):
        self.s3_objects = list(map(self._extract_event_info, event_info['Records']))
        self.curr = 0

    @staticmethod
    def _extract_event_info(event):
        """_extract_event_info

        :param event: (str) event information from lambda trigger

        >>> _extract_event_info({'s3': {'object': {'key': 'hello'}}, {'bucket': {'name': 'world'}}})
        {'Key': 'hello', 'Bucket': 'world'}
        """

        return {
            'Key': event['s3']['object']['key'],
            'Bucket': event['s3']['bucket']['name']
        }

    @staticmethod
    def _get_metadata(key, bucket):
        """_get_metadata returns the submission info of a raijin job
        from an s3 bucket

        :param key: (str) key of the associated logfile
        :param bucket: (str) bucket that the logfile is kept
        """
        info_key = key[:key.rfind('/') + 1] + S3Handler.SUBMISSION_INFO_FN

        try:
            metadata = yaml.load(
                S3_CLIENT.get_object(Key=info_key, Bucket=bucket)['Body'].read()
            )
        except S3_CLIENT.exceptions.NoSuchKey:
            metadata = {
                'pbs_job_id': 'UNKNOWN'
            }

        metadata['key_prefix'] = key.replace('/', '_')

        # Get log type based on file name
        metadata['log_type'] = {
            'out.log': 'info',
            'err.log': 'error'
        }.get(key.split('/')[-1], 'unknown')

        return metadata

    @staticmethod
    def _get_datastream(key, bucket):
        return io.StringIO(S3_CLIENT.get_object(Key=key, Bucket=bucket)['Body'].read().decode('utf-8'))

    def __iter__(self):
        self.curr = 0
        return self

    def __next__(self):
        # pylint: disable=unsubscriptable-object
        if self.curr >= len(self.s3_objects):
            raise StopIteration

        result = {
            'metadata': self._get_metadata(**self.s3_objects[self.curr]),
            'data': self._get_datastream(**self.s3_objects[self.curr])
        }

        self.curr += 1
        return result


class RaijinLogExtractor(BaseCommand):

    COMMAND_NAME = 'RaijinLogExtractor'

    def __init__(self):
        super().__init__(self)

    def command(self, *args, **kwargs):
        event = kwargs.get('event')
        data_feeder = S3Handler(event)

        for data_feed in data_feeder:
            data_consumer = ESHandler(ES)
            data_consumer.init_log_templates(data_feed['metadata'])
            self.process_feed(data_feed, data_consumer)

    @staticmethod
    def _create_unstructured_msg(msg_list, line_range, timestamp):
        return {
            'message': '\n'.join(msg_list),
            'line_no': line_range[0],
            'line_end': line_range[1],
            'timestamp': timestamp
        }

    def process_feed(self, data_feed, data_consumer):

        last_known_timestamp = None
        unstructured_range = [0, 0]
        unstructured_msg = []

        for line_no, line in enumerate(data_feed['data'].readlines()):
            try:
                log_load = json.loads(line)
                if unstructured_msg:
                    data_consumer.process_log(
                        'unstructured',
                        self._create_unstructured_msg(
                            unstructured_msg, unstructured_range, last_known_timestamp
                        )
                    )
                    unstructured_range = [0, 0]
                    unstructured_msg = []

                log_load['line_no'] = line_no
                log_load['timestamp'] = parse_date(log_load['timestamp'])
                data_consumer.process_log('structured', log_load)

                if log_load['timestamp']:
                    last_known_timestamp = log_load['timestamp']
            except (TypeError, json.decoder.JSONDecodeError):
                if unstructured_msg:
                    unstructured_range[1] = line_no
                else:
                    unstructured_range[0] = line_no
                    unstructured_range[1] = line_no
                unstructured_msg.append(line.replace('\n', ''))

        if unstructured_msg:
            data_consumer.process_log(
                'unstructured',
                self._create_unstructured_msg(
                    unstructured_msg, unstructured_range, last_known_timestamp
                )
            )

        data_consumer.submit_logs()


def handler(event, context):
    return RaijinLogExtractor().run(event=event)
