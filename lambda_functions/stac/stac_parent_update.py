"""
Update parent catalogs based on .yaml files corresponding to datasets uploaded in s3. File lists are
obtained from s3 inventory lists. Incremental updates can be done by using the 'from-date' option to limit
the selected dataset yaml files modified by a date later than the specified date. Updated catalog files
are uploaded to the specified bucket.

The s3 yaml file list is obtained from s3 inventory list unless a file list is provided in the command line.
"""

from collections import OrderedDict
from pathlib import Path
import json
import yaml
from parse import parse as pparse
import boto3
import click
from dea.aws import make_s3_client
from dea.aws.inventory import list_inventory
from pandas import Timestamp


def check_date(context, param, value):
    """
    Click callback to validate a date string
    """
    try:
        return Timestamp(value)
    except ValueError as error:
        raise ValueError('Date must be valid string for pandas Timestamp') from error


@click.command()
@click.option('--config', type=click.Path(exists=True), default='stac_config.yaml', help='The config file')
@click.option('--inventory-manifest', '-i',
              default='s3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/',
              help="The manifest of AWS inventory list")
@click.option('--bucket', '-b', required=True, help="AWS bucket to upload to")
@click.option('--from-date', callback=check_date, help="The date from which to update the catalog")
@click.argument('s3-keys', nargs=-1, type=click.Path())
def cli(config, inventory_manifest, bucket, from_date, s3_keys):
    """
    Update parent catalogs of datasets based on s3 keys having suffix .yaml
    """

    with open(config, 'r') as cfg_file:
        cfg = yaml.load(cfg_file)

    if not s3_keys:
        s3_client = make_s3_client()
        s3_keys = list_inventory(inventory_manifest, s3=s3_client)
        if from_date:
            s3_keys = incremental_list(s3_keys, from_date)
        s3_keys = remove_bucket_and_validate(s3_keys, cfg)

    CatalogUpdater(cfg).update_parents_all(s3_keys, bucket)


def remove_bucket_and_validate(keys, cfg):
    """
    Return generator of yaml files in s3 of products that belong to 'aws-products' in GLOBAL_CONFIG
    """
    for item in keys:
        template = '{}x_{x}/y_{y}/{}.yaml'
        if bool(sum([bool(pparse(p + template, item.Key)) for p in cfg['aws-products']])):
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


class CatalogUpdater:
    """
    Collate all the new links to be added and then update S3
    Assumed structure:
            root catalog
                -> per product/catalog.json
                    -> x/catalog.json
                        -> y/catalog.json
    """

    def __init__(self, config):
        self.config = config
        self.y_catalogs = {}
        self.x_catalogs = {}
        self.top_level_catalogs = {}

    def update_parents_all(self, s3_keys, bucket):
        """
        Update corresponding parent catalogs of the given list of yaml files
        """

        for item in s3_keys:
            s3_key = Path(item)
            # Collate parent catalog links
            self.add_to_y_catalog_links(f'{s3_key.parent}/{s3_key.stem}_STAC.json')

        # Update catalog files in s3 bucket now
        self.update_all_y_s3(bucket)
        self.update_all_x_s3(bucket)
        self.update_all_top_level_s3(bucket)

    def add_to_y_catalog_links(self, s3_key):
        """
        Add the given s3 key to corresponding y catalog links
        """

        template = '{prefix}/x_{x}/y_{y}/{}'
        params = pparse(template, s3_key)
        if params:
            params = params.named

            y_catalog_name = f'{params["prefix"]}/x_{params["x"]}/y_{params["y"]}/catalog.json'

            if self.y_catalogs.get(y_catalog_name):
                self.y_catalogs[y_catalog_name].add(s3_key)
            else:
                self.y_catalogs[y_catalog_name] = {s3_key}

            # Add the y catalog name to the corresponding x catalog
            self.add_to_x_catalog_links(y_catalog_name)

    def update_all_y_s3(self, bucket):
        """
        Update all the catalogs in S3 that has updated links
        """

        s3_res = boto3.resource('s3')
        for y_catalog_name in self.y_catalogs:
            obj = s3_res.Object(bucket, y_catalog_name)

            y_catalog = self.create_y_catalog(y_catalog_name)

            # Add the links
            for link in self.y_catalogs[y_catalog_name]:
                y_catalog['links'].append({'href': f'{self.config["aws-domain"]}/{link}', 'rel': 'item'})

            # Put y_catalog dict to s3
            obj = s3_res.Object(bucket, y_catalog_name)
            obj.put(Body=json.dumps(y_catalog), ContentType='application/json')

    def create_y_catalog(self, y_catalog_name):
        """
        Create a y catalog dict
        """

        template = '{prefix}/x_{x}/y_{y}/{}'
        params = pparse(template, y_catalog_name).named
        prefix, x, y = params['prefix'], params['x'], params['y']
        x_catalog_name = f'{prefix}/x_{x}/catalog.json'
        return OrderedDict([
            ('stac_version', '0.6.0'),
            ('id', f'{prefix}/x_{x}/y_{y}'),
            ('description', 'List of items'),
            ('links', [
                {'href': f'{self.config["aws-domain"]}/{y_catalog_name}',
                 'ref': 'self'},
                {'href': f'{self.config["aws-domain"]}/{x_catalog_name}',
                 'rel': 'parent'},
                {'href': self.config["root-catalog"],
                 'rel': 'root'}
            ])
            ])

    def add_to_x_catalog_links(self, y_catalog_name_abs):
        """
        Add the given y catalog s3 key link to the corresponding x catalog links
        """

        template = '{prefix}/x_{x}/{}'
        params = pparse(template, y_catalog_name_abs).named
        x_catalog_name = f'{params["prefix"]}/x_{params["x"]}/catalog.json'

        if self.x_catalogs.get(x_catalog_name):
            self.x_catalogs[x_catalog_name].add(y_catalog_name_abs)
        else:
            self.x_catalogs[x_catalog_name] = {y_catalog_name_abs}

        # Add x catalog link to product catalog
        if self.top_level_catalogs.get(params['prefix']):
            self.top_level_catalogs[params['prefix']].add(x_catalog_name)
        else:
            self.top_level_catalogs[params['prefix']] = {x_catalog_name}

    def update_all_x_s3(self, bucket):
        """
        Update all the x catalogs in S3 that has updated links
        """

        s3_res = boto3.resource('s3')
        for x_catalog_name in self.x_catalogs:

            x_catalog = self.create_x_catalog(x_catalog_name)

            # update the links
            for link in self.x_catalogs[x_catalog_name]:
                x_catalog['links'].append({'href': f'{self.config["aws-domain"]}/{link}', 'rel': 'child'})

            # Put x_catalog dict to s3
            obj = s3_res.Object(bucket, x_catalog_name)
            obj.put(Body=json.dumps(x_catalog), ContentType='application/json')

    def create_x_catalog(self, x_catalog_name):
        """
        Create a x catalog dict
        """

        template = '{prefix}/x_{x}/{}'
        params = pparse(template, x_catalog_name).named
        prefix, x = params['prefix'], params['x']
        return OrderedDict([
            ('stac_version', '0.6.0'),
            ('id', f'{prefix}/x_{x}'),
            ('description', 'List of Sub Directories'),
            ('links', [
                {'href': f'{self.config["aws-domain"]}/{x_catalog_name}',
                 'ref': 'self'},
                {'href': f'{self.config["aws-domain"]}/{prefix}/catalog.json',
                 'rel': 'parent'},
                {'href': self.config["root-catalog"],
                 'rel': 'root'}
            ])
        ])

    def update_all_top_level_s3(self, bucket):
        """
        Update all the parent catalogs one level above x dir in s3
        """

        s3_res = boto3.resource('s3')

        for top_level in self.top_level_catalogs:
            product_name = Path(top_level).parts[0]
            top_level_catalog_name = f'{top_level}/catalog.json'

            # create the top level catalog
            top_level_catalog = OrderedDict([
                ('stac_version', '0.6.0'),
                ('id', top_level),
                ('description', 'List of Sub Directories'),
                ('links', [
                    {'href': f'{self.config["aws-domain"]}/{top_level_catalog_name}',
                     'ref': 'self'},
                    {'href': f'{self.config["aws-domain"]}/{product_name}/catalog.json',
                     'rel': 'parent'},
                    {'href': self.config["root-catalog"],
                     'rel': 'root'}
                ])
            ])

            # Update the links
            for link in self.top_level_catalogs[top_level]:
                top_level_catalog['links'].append({'href': f'{self.config["aws-domain"]}/{link}', 'rel': 'child'})

            # Put top level catalog to s3
            obj = s3_res.Object(bucket, top_level_catalog_name)
            obj.put(Body=json.dumps(top_level_catalog), ContentType='application/json')


if __name__ == '__main__':
    cli()
