import unittest
from mock import patch
from handler import _update_cubedash_db_config, handler


def test_execute_command():
    execs = []

    def _mock_execute_update_cubedash_config_command(*args, **kwargs):
        execs.append('foo execute command')

        return "", "", 0

    output = _update_cubedash_db_config('test/file',
                                        'test_dbname',
                                        'test_branch',
                                        _mock_execute_update_cubedash_config_command)

    assert len(execs) == 1
    assert execs[0].startswith('foo execute')
    assert not output


class TestLambdaFunction(unittest.TestCase):

    @patch('handler.boto3.client')
    @patch('handler._update_cubedash_db_config')
    def test_nci_db_update(self, mock_client, mock_update_cubedash_call):
        event = {
            "Records": [
                {
                    "Sns": {
                        "Message": "nci_20190101\n",
                        "Timestamp": "Timestamp",
                        "MessageId": "Message_1234"
                    },
                },
            ]
        }
        handler(event, None)
        mock_update_cubedash_call.assert_called

    @patch('handler.boto3.client')
    @patch('handler._update_cubedash_db_config')
    def test_ows_db_update(self, mock_client, mock_update_cubedash_call):
        event = {
            "Records": [
                {
                    "Sns": {
                        "Message": "ows_20190101\n",
                        "Timestamp": "Timestamp",
                        "MessageId": "Message_1234"
                    },
                },
            ]
        }
        handler(event, None)
        mock_update_cubedash_call.assert_called

    @patch('handler.boto3.client')
    @patch('handler._update_cubedash_db_config')
    def test_db_name_error(self, mock_client, mock_update_cubedash_call):
        event = {
            "Records": [
                {
                    "Sns": {
                        "Message": "20190101\n",
                        "Timestamp": "Timestamp",
                        "MessageId": "Message_1234"
                    },
                },
            ]
        }
        try:
            handler(event, None)
        except SystemExit:
            mock_update_cubedash_call.assert_called
