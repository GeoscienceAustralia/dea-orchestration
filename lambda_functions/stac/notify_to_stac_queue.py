from pathlib import Path
from operator import xor
from types import SimpleNamespace
import json
import boto3
from dea.aws import make_s3_client
from dea.aws.inventory import list_inventory

import click


def s3_key_to_stac_queue(sqs_client, queue_url, bucket, s3_key):
    s3_event_message = {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": s3_key}}}]}
    # send a message to SQS
    return sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(s3_event_message)
    )


def messages_to_sqs(s3_keys, queue_url):
    sqs = boto3.client('sqs')

    for item in s3_keys:
        if Path(item.Key).suffix == '.yaml':
            # send a message to SQS
            s3_key_to_stac_queue(sqs, queue_url,
                                 item.Bucket, item.Key)


@click.group(help=__doc__)
def cli():
    pass


@cli.command()
@click.option('--inventory-manifest', '-i', help="The manifest of AWS inventory list")
@click.option('--queue-url', '-q', help="AWS sqs url")
@click.option('--s3-keys', '-s', nargs=-1, type=click.Path(), help="List of s3 keys")
@click.option('--bucket', '-b', help="AWS bucket")
def message_stac_queue(inventory_manifest, queue_url, s3_keys, bucket):
    """
    Send messages (yaml s3 keys) to stac_queue
    """
    assert xor(bool(inventory_manifest), bool(s3_keys)), "Use one of inventory-manifest or s3-keys"
    assert s3_keys and bucket, "The s3-keys must be specified with the associated bucket"
    if s3_keys:
        key_list = [SimpleNamespace(Bucket=bucket, Key=key) for key in s3_keys]
    else:
        if not inventory_manifest:
            inventory_manifest = 's3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/'
        s3 = make_s3_client()
        key_list = list_inventory(inventory_manifest, s3=s3)
    if not queue_url:
        queue_url = "https://sqs.ap-southeast-2.amazonaws.com/451924316694/static-stac-queue"
    messages_to_sqs(key_list, queue_url)


if __name__ == '__main__':
    cli()
