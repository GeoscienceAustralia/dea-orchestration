"""
Delete parent catalog files in s3 bucket that correspond to given product prefix.
The list of files with names 'catalog.json' is obtained from s3 inventory lists.
"""

import boto3
import click
from odc.aws import make_s3_client
from odc.aws.inventory import list_inventory
from pathlib import Path

AWS_DELETE_LIMIT = 1000


@click.command(help=__doc__)
@click.option('--aws-product-prefix', required=True, help="The prefix of a product in the AWS bucket")
@click.option('--bucket', '-b', default='dea-public-data', help="AWS bucket")
@click.option('--inventory-bucket', default='dea-public-data-inventory')
def delete_stac_catalog_parents(aws_product_prefix, bucket, inventory_bucket):
    s3_client = boto3.client('s3')
    delete_files = dict(Objects=[])
    for item in list_inventory(f's3://{inventory_bucket}/{bucket}/{bucket}-csv-inventory/',
                               s3=make_s3_client()):
        s3_key_file = Path(item.Key)

        # add to delete list
        if s3_key_file.name == 'catalog.json' and aws_product_prefix in item.Key:
            print(item.Key)
            delete_files['Objects'].append(dict(Key=item.Key))

        # flush out the delete list if aws limit reached
        if len(delete_files['Objects']) >= AWS_DELETE_LIMIT:
            s3_client.delete_objects(Bucket=bucket, Delete=delete_files)
            delete_files = dict(Objects=[])

    # flush out the remaining
    if len(delete_files['Objects']) >= AWS_DELETE_LIMIT:
        s3_client.delete_objects(Bucket=bucket, Delete=delete_files)


if __name__ == '__main__':
    delete_stac_catalog_parents()
