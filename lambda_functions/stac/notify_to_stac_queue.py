from collections import OrderedDict
from dateutil.parser import parse
from pyproj import Proj, transform
from pathlib import Path
from parse import parse as pparse
import datetime
import yaml
import json
import boto3
from dea.aws import make_s3_client
from dea.aws.inventory import list_inventory


def s3_key_to_stac_queue(sqs_client, queue_url, bucket, s3_key):
    s3_event_message = {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": s3_key}}}]}
    # send a message to SQS
    return sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(s3_event_message)
    )


def process_list(s3_keys):
    sqs = boto3.client('sqs')

    for item in s3_keys:
        if Path(item.Key).suffix == '.yaml':
            # send a message to SQS
            s3_key_to_stac_queue(sqs, "https://sqs.ap-southeast-2.amazonaws.com/451924316694/static-stac-queue",
                                 item.Bucket, item.Key)


if __name__ == '__main__':
    manifest = 's3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/'
    # manifest += '2018-10-13T08-00Z/manifest.json' ## force for now, because of dev account permissions

    s3 = make_s3_client()

    process_list(list_inventory(manifest, s3=s3))



