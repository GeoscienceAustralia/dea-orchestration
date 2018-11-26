"""
Send notification messages to 'static_stac_queue' for each yaml file uploaded to s3
corresponding to datasets that belong to products specified in GLOBAL_CONFIG

Incremental updates can be done by using the 'from-date' option to limit the
selected dataset yaml files modified by a date later than the specified date.

The s3 yaml file list is obtained from the specified s3 inventory list unless a file
list is provided in the command line.
"""

import json
from pathlib import Path
from parse import parse as pparse
import boto3
import click
from dea.aws import make_s3_client
from dea.aws.inventory import list_inventory
from pandas import Timestamp

GLOBAL_CONFIG = {
    "homepage": "http://www.ga.gov.au/",
    "licence": {
        "name": "CC BY Attribution 4.0 International License",
        "link": "https://creativecommons.org/licenses/by/4.0/",
        "short_name": "CCA 4.0",
        "copyright": "DEA, Geoscience Australia"
    },
    "contact": {
        "name": "Geoscience Australia",
        "organization": "Commonwealth of Australia",
        "email": "sales@ga.gov.au",
        "phone": "+61 2 6249 9966",
        "url": "http://www.ga.gov.au"
    },
    "provider": {
        "scheme": "s3",
        "region": "ap-southeast-2",
        "requesterPays": "False"
    },
    "aws-domain": "https://data.dea.ga.gov.au",
    "root-catalog": "https://data.dea.ga.gov.au/catalog.json",
    "aws-products": ['fractional-cover/fc/v2.2.0/ls8']
}


def check_date(context, param, value):
    """
    Click callback to validate a date string
    """
    try:
        return Timestamp(value)
    except ValueError as error:
        raise ValueError('Date must be valid string for pandas Timestamp') from error


@click.command()
@click.option('--inventory-manifest', '-i',
              default='s3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/',
              help="The manifest of AWS inventory list")
@click.option('--queue-url', '-q', default='https://sqs.ap-southeast-2.amazonaws.com/451924316694/static-stac-queue',
              help="AWS sqs url")
@click.option('--bucket', '-b', required=True, help="AWS bucket")
@click.option('--from-date', callback=check_date, help="The date from which to update the catalog")
@click.argument('s3-keys', nargs=-1, type=click.Path())
def cli(inventory_manifest, queue_url, bucket, from_date, s3_keys):
    """
    Send messages (yaml s3 keys) to stac_queue
    """

    if not s3_keys:
        s3_client = make_s3_client()
        s3_keys = list_inventory(inventory_manifest, s3=s3_client)
        if from_date:
            s3_keys = incremental_list(s3_keys, from_date)
        s3_keys = shed_bucket_and_validate(s3_keys)

    messages_to_sqs(s3_keys, bucket, queue_url)


def shed_bucket_and_validate(keys):
    """
    Return generator of yaml files in s3 of products that belong to 'aws-products' in GLOBAL_CONFIG
    """

    for item in keys:
        template = '{}x_{x}/y_{y}/{}.yaml'
        if bool(sum([bool(pparse(p + template, item.Key)) for p in GLOBAL_CONFIG['aws-products']])):
            yield item.Key


def incremental_list(inventory_s3_keys, from_date):
    """
    Filter the given generator list with items having LastModifiedDate attribute to a generator with the
    last modified date later than the given date
    """
    for item in inventory_s3_keys:
        time_modified = Timestamp(item.LastModifiedDate)
        if from_date < time_modified:
            yield item


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


def messages_to_sqs(s3_keys, bucket, queue_url):
    """
    Send messages to stac queue for all the s3 keys in the given list
    """

    sqs = boto3.client('sqs')

    for item in s3_keys:
        if Path(item).suffix == '.yaml':
            # send a message to SQS
            s3_key_to_stac_queue(sqs, queue_url, bucket, item)


if __name__ == '__main__':
    cli()
