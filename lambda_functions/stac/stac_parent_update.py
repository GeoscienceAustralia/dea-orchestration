from collections import OrderedDict
from pathlib import Path
from parse import parse as pparse
import json
import boto3
import click
from dea.aws import make_s3_client
from dea.aws.inventory import list_inventory

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
    "root-catalog": "https://data.dea.ga.gov.au/catalog.json"
}


def update_parent_catalogs(s3_key, s3_resource, bucket):
    """
    Assumed structure:
        root catalog
            -> per product/catalog.json
                -> x/catalog.json
                    -> y/catalog.json
    """

    template = '{prefix}/x_{x}/y_{y}/{}'
    params = pparse(template, s3_key).__dict__['named']
    y_catalog_name = f'{params["prefix"]}/x_{params["x"]}/y_{params["y"]}/catalog.json'
    y_obj = s3_resource.Object(bucket, y_catalog_name)

    try:
        # load y catalog dict
        y_catalog = json.loads(y_obj.get()['Body'].read().decode('utf-8'))

    except s3_resource.meta.client.exceptions.NoSuchKey as e:

        # The object does not exist.
        y_catalog = create_y_catalog(params["prefix"], params["x"], params["y"])

        # Potentially x catalog may not exist
        update_x_catalog(s3_key, s3_resource, bucket)

    # Create item link
    item = {'href': f'{GLOBAL_CONFIG["aws-domain"]}/{s3_key}',
            'rel': 'item'}

    # Add item to catalog
    # ToDo: Check whether the item exists first
    y_catalog["links"].append(item)

    # Put y_catalog dict to s3
    obj = s3_resource.Object(bucket, y_catalog_name)
    obj.put(Body=json.dumps(y_catalog))


def create_y_catalog(prefix, x, y):
    y_catalog_name = f'{prefix}/x_{x}/y_{y}/catalog.json'
    x_catalog_name = f'{prefix}/x_{x}/catalog.json'
    return OrderedDict([
        ('name', f'{prefix}/x_{x}/y_{y}'),
        ('description', 'List of items'),
        ('links', [
            {'href': f'{GLOBAL_CONFIG["aws-domain"]}/{y_catalog_name}',
             'ref': 'self'},
            {'href': f'{GLOBAL_CONFIG["aws-domain"]}/{x_catalog_name}',
             'rel': 'parent'},
            {'href': GLOBAL_CONFIG["root-catalog"],
             'rel': 'root'}
        ])
        ])


def update_x_catalog(s3_key, s3_resource, bucket):
    template = '{prefix}/x_{x}/y_{y}/{}'
    params = pparse(template, s3_key).__dict__['named']
    y_catalog_name_abs = f'{params["prefix"]}/x_{params["x"]}/y_{params["y"]}/catalog.json'
    y_catalog_name_rel_to_x = f'y_{params["y"]}/catalog.json'
    x_catalog_name = f'{params["prefix"]}/x_{params["x"]}/catalog.json'
    x_obj = s3_resource.Object(bucket, x_catalog_name)

    try:

        # load x catalog dict
        x_catalog = json.load(x_obj.get()['Body'].read().decode('utf-8'))

    except s3_resource.meta.client.exceptions.NoSuchKey as e:

        # The object does not exist.
        x_catalog = create_x_catalog(params["prefix"], params["x"])

    # search y catalog link
    for link in x_catalog["links"]:
        if link["href"] in (y_catalog_name_abs, y_catalog_name_rel_to_x):
            return

    # y catalog link not found so update it
    x_catalog["links"].append({"href": y_catalog_name_abs, "rel": "child"})

    # Write back x catalog
    x_obj.put(Body=json.dumps(x_catalog))


def create_x_catalog(prefix, x):
    # ToDo: check product catalog name/parent to x

    x_catalog_name = f'{prefix}/x_{x}/catalog.json'
    return OrderedDict([
        ('name', f'{prefix}/x_{x}'),
        ('description', 'List of Sub Directories'),
        ('links', [
            {'href': f'{GLOBAL_CONFIG["aws-domain"]}/{x_catalog_name}',
             'ref': 'self'},
            {'href': f'{GLOBAL_CONFIG["aws-domain"]}/{prefix}/catalog.json',
             'rel': 'parent'},
            {'href': GLOBAL_CONFIG["root-catalog"],
             'rel': 'root'}
        ])
        ])


def update_parents_all(s3_keys, bucket):

    s3_res = boto3.resource('s3')

    for item in s3_keys:
        if Path(item).suffix == '.yaml':
            # Update parent catalogs
            s3_key = Path(item)
            stac_s3_key = f'{s3_key.parent}/{s3_key.stem}_STAC.json'
            update_parent_catalogs(stac_s3_key, s3_res, bucket)


@click.command()
@click.option('--inventory-manifest', '-i', help="The manifest of AWS inventory list")
@click.option('--bucket', '-b', required=True, help="AWS bucket")
@click.argument('s3-keys', nargs=-1, type=click.Path())
def cli(inventory_manifest, bucket, s3_keys):
    """
    Update parent catalogs of datasets based on s3 keys having suffux .yaml
    """
    assert not (inventory_manifest and s3_keys), "Use one of inventory-manifest or s3-keys"

    def _shed_bucket(keys):
        for item in keys:
            yield item.Key

    if not s3_keys:
        if not inventory_manifest:
            inventory_manifest = 's3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/'
        s3 = make_s3_client()
        s3_keys = _shed_bucket(list_inventory(inventory_manifest, s3=s3))

    update_parents_all(s3_keys, bucket)


if __name__ == '__main__':
    cli()
