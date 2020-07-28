import boto3
import json
from pathlib import Path

from unittest.mock import patch, call
from moto import mock_s3
from s3_monthly_update.handler import handler


@mock_s3
@patch("s3_monthly_update.handler.get_monthly_jsons")
@patch("s3_monthly_update.handler.stats")
@patch("s3_monthly_update.handler.S3_OUTPUT_BUCKET", "s3_stat")
def test_handler(get_monthly_jsons_mock, stats_mock):
    bucket_name = "s3_stat"
    key = "stats/month"
    s3 = boto3.resource("s3")
    # Create the bucket since this is all in Moto's 'virtual' AWS account
    bucket = s3.create_bucket(Bucket=bucket_name)

    bucket.upload_file(str(Path(__file__).parent / "sample.json"), key)

    event_body = {
        "Records": [{"s3": {"bucket": {"name": bucket_name}, "object": {"key": key}}}]
    }
    event = {"Records": [{"body": json.dumps(event_body)}]}

    sample_json = (Path(__file__).parent / "sample.json").read_bytes().decode("utf-8")

    get_monthly_jsons_mock.return_value = [sample_json]

    # Run the Function
    handler(event, None)

    assert stats_mock.called
    assert stats_mock.call_count == 1
