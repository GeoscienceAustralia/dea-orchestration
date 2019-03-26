"""
Send notification messages to 'static_stac_queue' for each yaml file uploaded to s3
corresponding to datasets that belong to products specified in GLOBAL_CONFIG

Incremental updates can be done by using the 'from-date' option to limit the
selected dataset yaml files modified by a date later than the specified date.

The s3 yaml file list is obtained from the specified s3 inventory list unless a file
list is provided in the command line.
"""

import json
import logging

import boto3
import click
import dateutil.parser
import yaml
from pathlib import PurePosixPath

from odc.aws import make_s3_client
from odc.aws.inventory import list_inventory
from stac_utils import yamls_in_inventory_list, parse_date

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


@click.command(help=__doc__)
@click.option('--config', type=click.Path(exists=True), default='stac_config.yaml', help='The config file')
@click.option('--inventory-manifest', '-i',
              default='s3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/',
              help="The manifest of AWS inventory list")
@click.option('--queue-url', '-q', default='https://sqs.ap-southeast-2.amazonaws.com/451924316694/static-stac-queue',
              help="AWS sqs url")
@click.option('--bucket', '-b', required=True, help="AWS bucket")
@click.option('--from-date', callback=parse_date, help="The date from which to update the catalog")
@click.argument('s3-keys', nargs=-1, type=str)
def cli(config, inventory_manifest, queue_url, bucket, from_date, s3_keys=None):
    """
    Send messages (yaml s3 keys) to stac_queue
    """

    with open(config, 'r') as cfg_file:
        cfg = yaml.load(cfg_file)

    if not s3_keys:
        s3_client = make_s3_client()
        inventory_items = list_inventory(inventory_manifest, s3=s3_client)

        if from_date:
            inventory_items = (
                item
                for item in inventory_items
                if dateutil.parser.parse(item.LastModifiedDate) > from_date
            )

        s3_keys = yamls_in_inventory_list(inventory_items, cfg)

    LOG.info('Sending %s update messages', len(s3_keys))

    messages_to_sqs(s3_keys, bucket, queue_url)

    LOG.info('Done')


def messages_to_sqs(s3_keys, bucket, queue_url):
    """
    Send messages to stac queue for all the s3 keys in the given list
    """

    sqs = boto3.client('sqs')

    for item in s3_keys:
        if PurePosixPath(item).suffix == '.yaml':
            # send a message to SQS
            s3_key_to_stac_queue(sqs, queue_url, bucket, item)


def s3_key_to_stac_queue(sqs_client, queue_url, bucket, s3_key):
    """
    Send a message typical to s3 object put event to stac queue corresponding to the given s3 key
    """

    s3_event_message = {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": s3_key}}}]}
    # send a message to SQS
    return sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(s3_event_message)
    )


if __name__ == '__main__':
    cli()
