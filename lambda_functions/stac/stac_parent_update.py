"""
Update parent catalogs based on YAML files corresponding to datasets uploaded in S3.

 - File lists are obtained from S3 inventory lists.
 - Incremental updates can be done by using the '--from-date' option to limit the selected dataset yaml files
   modified by a date later than the specified date.
 - Updated catalog files are uploaded to the specified bucket.

The S3 YAML file list is obtained from S3 inventory list unless a file list is provided in the command line.
"""

import json
import logging
from collections import OrderedDict
from pathlib import PurePosixPath

import boto3
import click
import dateutil.parser
import ruamel.yaml
from parse import parse as pparse

from odc.aws import make_s3_client
from odc.aws.inventory import list_inventory
from stac_utils import yamls_in_inventory_list, parse_date

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
LOG = logging.getLogger()
LOG.setLevel(logging.INFO)
YAML = ruamel.yaml.YAML(typ='safe')


@click.command(help=__doc__)
@click.option('--config', type=click.Path(exists=True), default='stac_config.yaml', help='The config file')
@click.option('--inventory-manifest', '-i',
              default='s3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/',
              help="The manifest of AWS inventory list")
@click.option('--contents-file', help='file to read the list of new STAC Items from.')
@click.option('--bucket', '-b', required=True, help="AWS bucket to upload to")
@click.option('--from-date', callback=parse_date, help="The date from which to update the catalog")
@click.option('--dry-run', is_flag=True, flag_value=True, help="Don't persist anything to S3")
@click.argument('s3-keys', nargs=-1, type=str)
def cli(config, inventory_manifest, bucket, from_date, contents_file, s3_keys=None, dry_run=False):
    """
    Update parent catalogs of datasets based on S3 keys ending in .yaml
    """

    with open(config, 'r') as cfg_file:
        cfg = YAML.load(cfg_file)

    # Call a non-click function for testability
    update_parent_catalogs(bucket, cfg, from_date, inventory_manifest, contents_file, s3_keys, dry_run)


def update_parent_catalogs(bucket, cfg, from_date, inventory_manifest, contents_file, s3_keys=None, dry_run=False):
    if contents_file is not None:
        with open(contents_file) as fin:
            s3_keys = (line.strip()
                       for line in fin.readlines())

    elif not s3_keys:
        s3_client = make_s3_client()
        inventory_items = list_inventory(inventory_manifest, s3=s3_client)
        if from_date:
            inventory_items = (item
                               for item in inventory_items
                               if dateutil.parser.parse(item.LastModifiedDate) > from_date)
        s3_keys = yamls_in_inventory_list(inventory_items, cfg)

    cu = StacCollections(cfg, dry_run)
    cu.add_items(s3_keys)
    cu.persist_all_catalogs(bucket, dry_run=dry_run)


class StacCollections:
    """
    Collate all the new links to be added and then update S3
    """

    def __init__(self, config, dry_run=False):
        self.config = config
        self.items_catalogs = {}
        self.mid_level_catalogs = {}
        self.collection_catalogs = {}

        if dry_run:
            self.s3_res = None
        else:
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

            assert not item.startswith('s3:'), 'Input should be S3 Keys, not full URLs'

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

    def persist_all_catalogs(self, bucket, dry_run=False):

        # Update catalog files in s3 bucket now
        self.persist_collection_catalogs(bucket, dry_run)
        self.persist_mid_level_catalogs(bucket, dry_run)
        self.persist_item_catalogs(bucket, dry_run)

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

    def persist_mid_level_catalogs(self, bucket, dry_run):
        """
        Update all the catalogs in S3 that has updated links
        """

        for catalog_prefix, catalog_def in self.mid_level_catalogs.items():
            description = self.search_product_in_config(catalog_prefix)['description']
            # Create the catalog
            catalog = self.create_catalog(catalog_prefix,
                                          catalog_def['parent'],
                                          description)

            # Add the links
            for link in catalog_def['links']:
                catalog['links'].append({'href': f'{self.config["aws-domain"]}/{link}', 'rel': 'child'})

            # Put dict to s3
            catalog_key = f'{catalog_prefix}/catalog.json'
            if not dry_run:
                obj = self.s3_res.Object(bucket, catalog_key)
                obj.put(Body=json.dumps(catalog), ContentType='application/json')
            LOG.info('Wrote mid-level s3://%s', catalog_key)

    def persist_item_catalogs(self, bucket, dry_run):
        """
        Update all the x catalogs in S3 that has updated links
        """

        for catalog_prefix in self.items_catalogs:

            description = self.search_product_in_config(catalog_prefix)['description']
            # Create catalog
            catalog = self.create_catalog(catalog_prefix,
                                          self.items_catalogs[catalog_prefix]['parent'],
                                          description)

            # update the links
            for link in self.items_catalogs[catalog_prefix]['links']:
                catalog['links'].append(
                    {'href': f'{self.config["aws-domain"]}/{link}',
                     'rel': 'item'})

            # Put catalog dict to s3
            item_catalog_key = f'{catalog_prefix}/catalog.json'
            if not dry_run:
                obj = self.s3_res.Object(bucket, item_catalog_key)
                obj.put(Body=json.dumps(catalog), ContentType='application/json')
            LOG.info('Wrote item-catalog s3://%s', item_catalog_key)

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

    def persist_collection_catalogs(self, bucket, dry_run):
        """
        Update all the parent catalogs one level above x dir in s3. These are STAC Collections.

        See https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md
        for more information on Collections.
        """

        for collection_prefix in self.collection_catalogs:
            collection_catalog_key = f'{collection_prefix}/catalog.json'
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
            if collection_catalog_key == f'{product_type}/catalog.json' or not info.get('product_suite'):
                # Parent and root catalogs are same
                collection_catalog['links'] = [
                    {'href': f'{self.config["aws-domain"]}/{collection_catalog_key}', 'rel': 'self'},
                    {'href': self.config["root-catalog"], 'rel': 'parent'},
                    {'href': self.config["root-catalog"], 'rel': 'root'}
                ]
            else:
                # We have a distinct product type directory which can hold the parent catalog
                collection_catalog['links'] = [
                    {'href': f'{self.config["aws-domain"]}/{collection_catalog_key}', 'rel': 'self'},
                    {'href': f'{self.config["aws-domain"]}/{product_type}/catalog.json', 'rel': 'parent'},
                    {'href': self.config["root-catalog"], 'rel': 'root'}
                ]

            # Update the links
            for link in self.collection_catalogs[collection_prefix]:
                collection_catalog['links'].append({'href': f'{self.config["aws-domain"]}/{link}', 'rel': 'child'})

            # Put collection catalog to s3
            if not dry_run:
                obj = self.s3_res.Object(bucket, collection_catalog_key)
                obj.put(Body=json.dumps(collection_catalog), ContentType='application/json')
            LOG.info('Wrote collection catalog s3://%s', collection_catalog_key)


if __name__ == '__main__':
    cli()
