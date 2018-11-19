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

    CatalogUpdater().update_parents_all(s3_keys, bucket)


class CatalogUpdater:
    """
    Collate all the new links to be added and then update S3
    Assumed structure:
            root catalog
                -> per product/catalog.json
                    -> x/catalog.json
                        -> y/catalog.json
    """

    def __init__(self):
        self.y_catalogs = {}
        self.x_catalogs = {}

    def update_parents_all(self, s3_keys, bucket):
        """
        Update corresponding parent catalogs of the given list of yaml files
        """

        for item in s3_keys:
            s3_key = Path(item)
            if s3_key.suffix == '.yaml':
                # Collate parent catalog links
                self.add_to_y_catalog_links(f'{s3_key.parent}/{s3_key.stem}_STAC.json')

        # Update catalog files in s3 bucket now
        self.update_all_s3(bucket)

    def add_to_y_catalog_links(self, s3_key):
        """
        Add the given s3 key to corresponding y catalog links
        """

        template = '{prefix}/x_{x}/y_{y}/{}'
        params = pparse(template, s3_key).__dict__['named']
        y_catalog_name = f'{params["prefix"]}/x_{params["x"]}/y_{params["y"]}/catalog.json'

        # No addition if exists
        if self.y_catalogs.get(y_catalog_name):
            self.y_catalogs[y_catalog_name].add(s3_key)
        else:
            self.y_catalogs[y_catalog_name] = {s3_key}

    @staticmethod
    def _shed_domain_from_link(link):
        """
        Remove AWS domain part from the s3 link
        """

        template = GLOBAL_CONFIG['aws-domain'] + '/{key}'
        params = pparse(template, link).__dict__['named']
        return params['key']

    def update_all_s3(self, bucket):
        """
        Update all the catalogs in S3 that has updated links
        """

        s3_res = boto3.resource('s3')
        for y_catalog_name in self.y_catalogs.keys():
            obj = s3_res.Object(bucket, y_catalog_name)

            try:
                # load catalog dict
                y_catalog = json.loads(obj.get()['Body'].read().decode('utf-8'))

            except s3_res.meta.client.exceptions.NoSuchKey as e:

                # The object does not exist.
                y_catalog = self.create_y_catalog(y_catalog_name)

                # Potentially x catalog link may not exist
                self.add_to_x_catalog_links(y_catalog_name)

            # existing links in the catalog file
            existing_links = {self._shed_domain_from_link(link['href']) for link in y_catalog['links']}

            # New links to be added
            new_links = self.y_catalogs[y_catalog_name] - existing_links

            # update the links
            for link in new_links:
                y_catalog['links'].append({'href': f'{GLOBAL_CONFIG["aws-domain"]}/{link}', 'rel': 'item'})

            # Put y_catalog dict to s3
            obj = s3_res.Object(bucket, y_catalog_name)
            obj.put(Body=json.dumps(y_catalog))

        # Update all x catalogs in s3
        self.update_all_x_s3(bucket)

    @staticmethod
    def create_y_catalog(y_catalog_name):
        """
        Create a y catalog dict
        """

        template = '{prefix}/x_{x}/y_{y}/{}'
        params = pparse(template, y_catalog_name).__dict__['named']
        prefix, x, y = params['prefix'], params['x'], params['y']
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

    def add_to_x_catalog_links(self, y_catalog_name_abs):
        """
        Add the given y catalog s3 key link to the corresponding x catalog links
        """

        template = '{prefix}/x_{x}/{}'
        params = pparse(template, y_catalog_name_abs).__dict__['named']
        x_catalog_name = f'{params["prefix"]}/x_{params["x"]}/catalog.json'

        # No addition if exists
        if self.x_catalogs.get(x_catalog_name):
            self.x_catalogs[x_catalog_name].add(y_catalog_name_abs)
        else:
            self.x_catalogs[x_catalog_name] = {y_catalog_name_abs}

    def update_all_x_s3(self, bucket):
        """
        Update all the x catalogs in S3 that has updated links
        """

        s3_res = boto3.resource('s3')
        for x_catalog_name in self.x_catalogs.keys():
            obj = s3_res.Object(bucket, x_catalog_name)

            try:
                # load catalog dict
                x_catalog = json.loads(obj.get()['Body'].read().decode('utf-8'))

            except s3_res.meta.client.exceptions.NoSuchKey as e:

                # The object does not exist.
                x_catalog = self.create_x_catalog(x_catalog_name)

            # existing links in the catalog file
            existing_links = {self._shed_domain_from_link(link['href']) for link in x_catalog["links"]}

            # New links to be added
            new_links = self.x_catalogs[x_catalog_name] - existing_links

            # update the links
            for link in new_links:
                x_catalog['links'].append({'href': f'{GLOBAL_CONFIG["aws-domain"]}/{link}', 'rel': 'child'})

            # Put x_catalog dict to s3
            obj = s3_res.Object(bucket, x_catalog_name)
            obj.put(Body=json.dumps(x_catalog))

    @staticmethod
    def create_x_catalog(x_catalog_name):
        """
        Create a x catalog dict
        """

        template = '{prefix}/x_{x}/{}'
        params = pparse(template, x_catalog_name).__dict__['named']
        prefix, x = params['prefix'], params['x']
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


if __name__ == '__main__':
    cli()
