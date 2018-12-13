"""
Delete parent catalog files in s3 bucket that correspond to given product prefix.
The list of files with names 'catalog.json' is obtained from s3 inventory lists.
"""

from pathlib import Path

import boto3
import click
from dea.aws import make_s3_client
from dea.aws.inventory import list_inventory


@click.command(help=__doc__)
@click.option('--aws-product-prefix', required=True, help="The prefix of a product in the AWS bucket")
@click.option('--bucket', '-b', default='dea-public-data', help="AWS bucket")
def delete_stac_catalog_parents(aws_product_prefix, bucket):
    s3_client = boto3.client('s3')
    delete_files = dict(Objects=[])
    for item in list_inventory('s3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/',
                               s3=make_s3_client()):
        s3_key_file = Path(item.Key)

        # add to delete list
        if s3_key_file.name == 'catalog.json' and aws_product_prefix in item.Key:
            print(item.Key)
            delete_files['Objects'].append(dict(Key=item.Key))

        # flush out the delete list if aws limit (1000) reached
        if len(delete_files['Objects']) >= 1000:
            s3_client.delete_objects(Bucket=bucket, Delete=delete_files)
            delete_files = dict(Objects=[])

    # flush out the remaining
    if len(delete_files['Objects']) >= 1000:
        s3_client.delete_objects(Bucket=bucket, Delete=delete_files)


if __name__ == '__main__':
    delete_stac_catalog_parents()
