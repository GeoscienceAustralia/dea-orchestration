import unittest
from unittest.mock import call
from mock import patch
from handler import automate_rolling_archive, handler
from ssh import _connect


class TestLambdaFunction(unittest.TestCase):

    @patch('handler.boto3.client')
    @patch('handler.automate_rolling_archive')
    def test_handler(self, mock_client, mock_automate_rolling_archive):
        event = {
            "Records": [
                {
                    "Sns": {
                        "Message": "Level Sentinel2 Definitive process completed\n",
                        "Timestamp": "Timestamp",
                        "MessageId": "Message_1234"
                    },
                },
            ]
        }

        # Run the function under test
        handler(event, None)

        mock_automate_rolling_archive.assert_called

    @patch('handler.boto3.client')
    def test_execute_command(self, mock_s3_client):
        execs = []

        s3_bucket_name = "testBucket"
        level1_done_list = [
            '/foo/level1/done/batch1',
            '/foo/level1/done/batch2',
        ]

        def _mock_execute_command(*args, **kwargs):
            execs.append('foo execute command')

            return "", "", 0

        # Run the function under test
        automate_rolling_archive(mock_s3_client, s3_bucket_name, level1_done_list,
                                 execute_command=_mock_execute_command)

        assert len(execs) == 1
        assert execs[0].startswith('foo execute')

    @patch('handler.boto3.client')
    @patch('handler.exec_command')
    def test_automate_rolling_archive(self, mock_s3_client, exec_command_mock):
        exec_command_mock.return_value = "", "", 0
        s3_bucket_name = "testBucket"
        level1_done_list = [
            '/foo/level1/done/batch1',
            '/foo/level1/done/batch2',
        ]

        # Run the function under test
        automate_rolling_archive(mock_s3_client, s3_bucket_name, level1_done_list, execute_command=exec_command_mock)

        # Check that the correct arguments are passed to handler.execute_command
        assert exec_command_mock.call_args == call(
            f'execute_s2nbar_rolling_archive --s3client {mock_s3_client} --s3bucket {s3_bucket_name} '
            f'--filelist {level1_done_list}')
