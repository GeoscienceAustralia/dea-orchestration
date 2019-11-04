import unittest
from mock import patch
from .handler import automate_rolling_archive, handler


class TestLambdaFunction(unittest.TestCase):

    def test_automate_rolling_archive(self):
        execs = []

        s3_bucket_name = "testBucket"
        level1_done_list = "/foo/level-1-done.txt,"

        def _mock_execute_command(*args, **kwargs):
            execs.append('foo execute command')

            return "", "", 0
        _profile = "dev"

        # Run the function under test
        automate_rolling_archive(_profile, s3_bucket_name, level1_done_list,
                                 execute_command=_mock_execute_command)

        assert len(execs) == 1
        assert execs[0].startswith('foo execute')

    @patch('execute_s2nbar.handler.automate_rolling_archive')
    def test_handler(self, mock_automate_rolling_archive):
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

        mock_automate_rolling_archive.return_value = "", "", 0

        # Run the function under test
        handler(event, None)

        # Check that automate_rolling_archive is called
        mock_automate_rolling_archive.assert_called
