"""
Update parent catalogs based on YAML files corresponding to datasets uploaded in S3.

 - File lists are obtained from S3 inventory lists.
 - Incremental updates can be done by using the '--from-date' option to limit the selected dataset yaml files
   modified by a date later than the specified date.
 - Updated catalog files are uploaded to the specified bucket.

The S3 YAML file list is obtained from S3 inventory list unless a file list is provided in the command line.
"""

import json
from collections import OrderedDict

import boto3
import click
import dateutil.parser
import yaml
from odc.aws import make_s3_client
from odc.aws.inventory import list_inventory
from parse import parse as pparse
from pathlib import PurePosixPath

from stac_utils import yamls_in_inventory_list, parse_date


@click.command(help=__doc__)
@click.option('--config', type=click.Path(exists=True), default='stac_config.yaml', help='The config file')
@click.option('--inventory-manifest', '-i',
              default='s3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/',
              help="The manifest of AWS inventory list")
@click.option('--bucket', '-b', required=True, help="AWS bucket to upload to")
@click.option('--from-date', callback=parse_date, help="The date from which to update the catalog")
@click.argument('s3-keys', nargs=-1, type=str)
def cli(config, inventory_manifest, bucket, from_date, s3_keys=None):
    """
    Update parent catalogs of datasets based on S3 keys ending in .yaml
    """

    # Call a non-click function for testability
    update_parent_catalogs(bucket, config, from_date, inventory_manifest, s3_keys)


def update_parent_catalogs(bucket, config, from_date, inventory_manifest, s3_keys=None):
    with open(config, 'r') as cfg_file:
        cfg = yaml.load(cfg_file)

    if s3_keys:
        s3_client = make_s3_client()
        inventory_items = list_inventory(inventory_manifest, s3=s3_client)
        if from_date:
            inventory_items = (item for item in inventory_items
                               if dateutil.parser.parse(item.LastModifiedDate) > from_date)
        s3_keys = yamls_in_inventory_list(inventory_items, cfg)

    cu = StacCollections(cfg)
    cu.add_items(s3_keys)
    cu.persist_all_catalogs(bucket)


class StacCollections:
    """
    Collate all the new links to be added and then update S3
    """

    def __init__(self, config):
        self.config = config
        self.items_catalogs = {}
        self.mid_level_catalogs = {}
        self.collection_catalogs = {}


        self.s3_res = boto3.resource('s3')

    def add_items(self, items):
        """
        Update parent catalogs of the given list of yaml files

        Collection Catalogs
           |
           ------Mid Level Catalog (top mid level)
                 |
                 --------Mid Level Catalog (in between mid level)
                         |
                         -----------Item Catalog (bottom catalog level)
                                    |
                                    ----------Items
        """

        for item in items:

            prod_dict = self.search_product_in_config(item)

            prefixes = self.get_prefixes(prod_dict['catalog_structure'], item)
            collection_prefix = str(PurePosixPath(prefixes[0]).parent)

            s3_key = PurePosixPath(item)

            # Add to the top mid level catalogs which have parent pointing to collection catalog
            if len(prefixes) > 1:
                child_catalog_name = f'{prefixes[1]}/catalog.json'
                self.add_to_catalog(self.mid_level_catalogs, prefixes[0],
                                    f'{collection_prefix}/catalog.json', child_catalog_name)

            # Add to bottom catalog level that hold links to items
            if len(prefixes) == 1:
                parent_catalog_name = f'{collection_prefix}/catalog.json'
            else:
                parent_catalog_name = f'{prefixes[-2]}/catalog.json'
            self.add_to_catalog(self.items_catalogs, prefixes[-1],
                                parent_catalog_name, f'{s3_key.parent}/{s3_key.stem}_STAC.json')

            # Add to in between top level and item level catalogs
            for count, catalog_prefix in enumerate(prefixes[1:-1]):
                self.add_to_catalog(self.mid_level_catalogs, catalog_prefix,
                                    f'{prefixes[count]}/catalog.json',
                                    f'{prefixes[count + 2]}/catalog.json')

            # Add to collection catalog
            if self.collection_catalogs.get(collection_prefix):
                self.collection_catalogs[collection_prefix].add(f'{prefixes[0]}/catalog.json')
            else:
                self.collection_catalogs[collection_prefix] = {f'{prefixes[0]}/catalog.json'}

    def persist_all_catalogs(self, bucket):

        # Update catalog files in s3 bucket now
        self.persist_collection_catalogs(bucket)
        self.persist_mid_level_catalogs(bucket)
        self.persist_item_catalogs(bucket)

    @staticmethod
    def get_prefixes(templates, item):
        """
        Get S3 prefixes corresponding to each catalog template
        """

        prefixes = []
        for template in templates:
            template_ = '{prefix}/' + template + '/{}'
            params = pparse(template_, item)
            if not params:
                template_ = template + '/{}'
                params = pparse(template_, item)
                if not params:
                    raise NameError('Catalog template parsing error: ' + item)
                prefixes.append(template.format(**params.named))
            else:
                prefixes.append(('{prefix}/' + template).format(**params.named))
        return prefixes

    @staticmethod
    def add_to_catalog(catalog_dict, catalog_prefix, parent_catalog_name, item):
        """
        Add a catalog file name to a catalog dict
        """

        if catalog_prefix in catalog_dict:
            if catalog_dict[catalog_prefix]['parent'] != parent_catalog_name:
                raise NameError('Incorrect parent catalog name for : ' + item)
            catalog_dict[catalog_prefix]['links'].add(item)
        else:
            catalog_dict[catalog_prefix] = {'parent': parent_catalog_name,
                                            'links': {item}}

    def persist_mid_level_catalogs(self, bucket):
        """
        Update all the catalogs in S3 that has updated links
        """

        for catalog_prefix, catalog_def in self.mid_level_catalogs.items():
            # Create the catalog
            catalog = self.create_catalog(catalog_prefix,
                                          catalog_def['parent'],
                                          'List of Directories')

            # Add the links
            for link in catalog_def['links']:
                catalog['links'].append({'href': f'{self.config["aws-domain"]}/{link}', 'rel': 'child'})

            # Put dict to s3
            obj = self.s3_res.Object(bucket, f'{catalog_prefix}/catalog.json')
            obj.put(Body=json.dumps(catalog), ContentType='application/json')

    def persist_item_catalogs(self, bucket):
        """
        Update all the x catalogs in S3 that has updated links
        """

        for catalog_prefix in self.items_catalogs:

            # Create catalog
            catalog = self.create_catalog(catalog_prefix,
                                          self.items_catalogs[catalog_prefix]['parent'],
                                          'List of items')

            # update the links
            for link in self.items_catalogs[catalog_prefix]['links']:
                catalog['links'].append(
                    {'href': f'{self.config["aws-domain"]}/{link}',
                     'rel': 'item'})

            # Put catalog dict to s3
            obj = self.s3_res.Object(bucket, f'{catalog_prefix}/catalog.json')
            obj.put(Body=json.dumps(catalog), ContentType='application/json')

    def create_catalog(self, prefix, parent_catalog_name, description):
        """
        Create a STAC catalog
        """

        return OrderedDict([
            ('stac_version', '0.6.0'),
            ('id', prefix),
            ('description', description),
            ('links', [
                {'href': f'{self.config["aws-domain"]}/{prefix}/catalog.json',
                 'rel': 'self'},
                {'href': f'{self.config["aws-domain"]}/{parent_catalog_name}',
                 'rel': 'parent'},
                {'href': self.config["root-catalog"],
                 'rel': 'root'}
            ])
        ])

    def search_product_in_config(self, prefix):
        """
        Search the product list in the config and return the product dict that matches the given prefix.
        """

        for product_dict in self.config['products']:
            if product_dict.get('prefix'):
                if product_dict['prefix'] in prefix or prefix in product_dict['prefix']:
                    return product_dict
        return None

    def persist_collection_catalogs(self, bucket):
        """
        Update all the parent catalogs one level above x dir in s3. These are STAC Collections.

        See https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md
        for more information on Collections.
        """


        for collection_prefix in self.collection_catalogs:
            collection_catalog_name = f'{collection_prefix}/catalog.json'
            info = self.search_product_in_config(collection_prefix)

            # get extents from config
            extent = info.get('extent')
            spatial_extent = self.config['aus-extent']['spatial']
            temporal_extent = self.config['aus-extent']['temporal']
            spatial_extent = extent.get('spatial', spatial_extent) if extent else spatial_extent
            temporal_extent = extent.get('temporal', temporal_extent) if extent else temporal_extent

            # create the collection catalog
            collection_catalog = OrderedDict([
                ('stac_version', '0.6.0'),
                ('id', info['name']),
                ('description', info['description'])])
            if info.get('keywords'):
                collection_catalog['keywords'] = info['keywords']
            if info.get('version'):
                collection_catalog['version'] = info['version']
            collection_catalog['license'] = self.config['license']['short_name']
            if info.get('providers'):
                collection_catalog['providers'] = info.get('providers')
            collection_catalog['extent'] = {'spatial': spatial_extent, 'temporal': temporal_extent}

            product_type = PurePosixPath(collection_prefix).parts[0]
            if collection_catalog_name == f'{product_type}/catalog.json' or not info.get('product_suite'):
                # Parent and root catalogs are same
                collection_catalog['links'] = [
                    {'href': f'{self.config["aws-domain"]}/{collection_catalog_name}', 'rel': 'self'},
                    {'href': self.config["root-catalog"], 'rel': 'parent'},
                    {'href': self.config["root-catalog"], 'rel': 'root'}
                ]
            else:
                # We have a distinct product type directory which can hold the parent catalog
                collection_catalog['links'] = [
                    {'href': f'{self.config["aws-domain"]}/{collection_catalog_name}', 'rel': 'self'},
                    {'href': f'{self.config["aws-domain"]}/{product_type}/catalog.json', 'rel': 'parent'},
                    {'href': self.config["root-catalog"], 'rel': 'root'}
                ]

            # Update the links
            for link in self.collection_catalogs[collection_prefix]:
                collection_catalog['links'].append({'href': f'{self.config["aws-domain"]}/{link}', 'rel': 'child'})

            # Put collection catalog to s3
            obj = self.s3_res.Object(bucket, collection_catalog_name)
            obj.put(Body=json.dumps(collection_catalog), ContentType='application/json')


if __name__ == '__main__':
    cli()
