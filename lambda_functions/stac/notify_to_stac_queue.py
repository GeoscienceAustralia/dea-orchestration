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


def create_s3_event_message(bucket, s3_key):
    return {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": s3_key}}}]}


if __name__ == '__main__':
    manifest = 's3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/'
    # manifest += '2018-10-13T08-00Z/manifest.json' ## force for now, because of dev account permissions

    s3 = make_s3_client()

    full_inventory = list_inventory(manifest, s3=s3)

    sqs = boto3.client('sqs')

    for item in full_inventory:
        if Path(item.Key).suffix == '.yaml':
            # send a message to SQS
            response = sqs.send_message(
                QueueUrl="https://sqs.ap-southeast-2.amazonaws.com/451924316694/static-stac-queue",
                MessageBody=json.dumps(create_s3_event_message(item.Bucket, item.Key))
            )
