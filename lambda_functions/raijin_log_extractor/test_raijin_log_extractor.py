import unittest
from unittest.mock import MagicMock, patch, call
import datetime
import io

from raijin_log_extractor import ESHandler, handler


class TestESHandler(unittest.TestCase):

    metadata = {
        'pbs_job_id': 'test-job-id',
        'key_prefix': 'test_prefix_id',
        'log_type': 'test.log'
    }

    def test_init_log_templates(self):
        es = ESHandler(MagicMock())

        es.init_log_templates(self.metadata)

        structured_tmpl = es.log_templates['structured']

        # Sanity check template defaults
        assert structured_tmpl['_id'] == self.metadata['key_prefix'] + '_{}'
        assert structured_tmpl['task_id'] == self.metadata['pbs_job_id']

        unstructured_tmpl = es.log_templates['unstructured']

        assert unstructured_tmpl['_id'] == self.metadata['key_prefix'] + '_{}'
        assert unstructured_tmpl['task_id'] == self.metadata['pbs_job_id']
        assert unstructured_tmpl['event'] == self.metadata['key_prefix'] + '_' + self.metadata['log_type']

    def test_process_log(self):
        es = ESHandler(MagicMock())
        es.init_log_templates(self.metadata)

        assert len(es.log_data['structured']) == 0
        assert len(es.log_data['unstructured']) == 0
        log_message = {'line_no': 1}

        es.process_log('structured', log_message)
        assert len(es.log_data['structured']) == 1
        assert len(es.log_data['unstructured']) == 0

        es.process_log('unstructured', log_message)
        assert len(es.log_data['structured']) == 1
        assert len(es.log_data['unstructured']) == 1


class TestHandler(unittest.TestCase):

    METADATA = {
        'pbs_job_id': 'test-job-id',
        'key_prefix': 'test_prefix_id',
        'log_type': 'test.log'
    }

    @patch('raijin_log_extractor.ESHandler')
    @patch('raijin_log_extractor.S3Handler')
    def test_simple_message(self, mock_s3, mock_es):

        test_data = io.StringIO('''\
{"timestamp": "2017-06-12T12:00:00", "message": "hello-world"}
TIMESTAMP: 2017-06-12T12:00:00 Hello, world!\
''')

        mock_s3_obj = mock_s3()
        mock_s3_obj.__iter__.return_value = [
            {'metadata': self.METADATA, 'data': test_data}
        ]
        handler({'Records': []}, None)

        mock_es_obj = mock_es()
        mock_es_obj.process_log.assert_has_calls([
            call(
                'structured',
                {
                    'timestamp': datetime.datetime(2017, 6, 12, 12, 0),
                    'message': 'hello-world',
                    'line_no': 0
                }
            ),
            call(
                'unstructured',
                {
                    'message': 'TIMESTAMP: 2017-06-12T12:00:00 Hello, world!',
                    'line_no': 1,
                    'line_end': 1,
                    'timestamp': datetime.datetime(2017, 6, 12, 12, 0)
                }
            )
        ])

        mock_es_obj.submit_logs.assert_called_once()

    @patch('raijin_log_extractor.ESHandler')
    @patch('raijin_log_extractor.S3Handler')
    def test_consolidated_message(self, mock_s3, mock_es):
        test_data = io.StringIO('''\
TIMESTAMP: 2017-10-24T23:00:00 Something went wrong
TIMESTAMP: 2017-06-12T12:00:00 Hello, world!
{"timestamp": "2017-06-12T12:00:00", "message": "hello-world"}
TIMESTAMP: 2017-06-12T12:00:00 Hello, world!\
''')
        mock_s3_obj = mock_s3()
        mock_s3_obj.__iter__.return_value = [
            {'metadata': self.METADATA, 'data': test_data}
        ]
        handler({'Records': []}, None)

        mock_es_obj = mock_es()
        mock_es_obj.process_log.assert_has_calls([
            call(
                'unstructured',
                {
                    'message': 'TIMESTAMP: 2017-10-24T23:00:00 Something went wrong\n' +
                               'TIMESTAMP: 2017-06-12T12:00:00 Hello, world!',
                    'line_no': 0,
                    'line_end': 1,
                    'timestamp': None
                }
            ),
            call(
                'structured',
                {
                    'timestamp': datetime.datetime(2017, 6, 12, 12, 0),
                    'message': 'hello-world',
                    'line_no': 2
                }
            ),
            call(
                'unstructured',
                {
                    'message': 'TIMESTAMP: 2017-06-12T12:00:00 Hello, world!',
                    'line_no': 3,
                    'line_end': 3,
                    'timestamp': datetime.datetime(2017, 6, 12, 12, 0)
                }
            )
        ])

        mock_es_obj.submit_logs.assert_called_once()
