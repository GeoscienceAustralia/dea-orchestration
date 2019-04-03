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
from ruamel.yaml import YAML
from itertools import islice

from odc.aws import make_s3_client
from odc.aws.inventory import list_inventory
from stac_utils import yamls_in_inventory_list, parse_date

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

yaml = YAML(typ='safe')


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
    else:
        # Filter out non yaml keys
        s3_keys = [item for item in s3_keys if item.endswith('.yaml')]

    LOG.info('Sending %s update messages', len(s3_keys))

    messages_to_sqs(s3_keys, bucket, queue_url)

    LOG.info('Done')


def messages_to_sqs(s3_keys, bucket, queue_url):
    """
    Send messages to stac queue for all the s3 keys in the given list
    """

    sqs = boto3.client('sqs')

    for batch in chunks(s3_keys, 10):

        batch_request = [dict(Id=str(n), MessageBody=s3_key_event(bucket, key)) for n, key in enumerate(batch)]
        response = sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=batch_request
        )

        if 'Failed' in response:
            LOG.error('Failed messages: %s', response['Failed'])


def s3_key_event(bucket, s3_key):
    """
    Send a message typical to s3 object put event to stac queue corresponding to the given s3 key
    """

    return json.dumps({"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": s3_key}}}]})


def chunks(iterable, chunk_size):
    """Split the items in an iterable into chunks of a given size.

    If the number of items isn't a multiple of chunk_size, the last chunk will
    be smaller than chunk_size.

    This is like the grouper() recipe in the itertools documentation, except
    that no filler value is used, the code is more straightforward, and it
    is more efficient on sequences via special casing.
    """
    try:
        if int(chunk_size) != chunk_size or chunk_size < 1:
            raise ValueError('chunk_size must be an integer greater than zero!')
    except TypeError:
        raise ValueError('chunk_size must be an integer greater than zero!')

    try:
        # try efficient version for sequences
        n = len(iterable)
        if n == 0:
            pass
        elif chunk_size >= n:
            # just yield the given sequence
            # this avoids needlessly copying the entire sequence
            yield iterable
        else:
            for start in range(0, n, chunk_size):
                yield iterable[start:start + chunk_size]
    except (TypeError, AttributeError):  # may be thrown by len() or the slicing
        # use generic version which works on all iterables
        iterator = iter(iterable)
        while True:
            chunk = list(islice(iterator, chunk_size))
            if not chunk:
                break
            yield chunk


if __name__ == '__main__':
    cli()
