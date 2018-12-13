"""
Update the product suite catalogs, i.e. STAC catalogs corresponding to product suites
such as WOfS, fractional-cover, and Geomedian-Australia.
"""

import json
from collections import OrderedDict

import boto3
import click
import yaml

S3 = boto3.resource('s3')

# Read the config file
with open('stac_config.yaml', 'r') as cfg_file:
    CFG = yaml.load(cfg_file)


@click.command(help=__doc__)
@click.option('--bucket', '-b', default='dea-public-data-dev', help="AWS bucket")
def update_stac_product_suite_catalogs(bucket):
    do_wofs(bucket)
    do_fractional_cover(bucket)
    do_geomedian_australia(bucket)


def do_wofs(bucket):
    """
    Update WOfS (wofs_albers, wofs_filtered_summary, wofs_statistical_summary, wofs_annual_summary)
    """

    collection_catalog = OrderedDict([
        ('stac_version', '0.6.0'),
        ('id', 'WOfS'),
        ('description', 'Water observation from space suite of products include wofs_albers, '
                        'wofs_filtered_summary, wofs_statistical_summary, and wofs_annual_summary')])
    collection_catalog['license'] = CFG['license']['short_name']
    collection_catalog['extent'] = {'spatial': CFG['aus-extent']['spatial'],
                                    'temporal': CFG['aus-extent']['temporal']}
    collection_catalog['links'] = [
        {'href': f'{CFG["aws-domain"]}/WOfS/catalog.json', 'ref': 'self'},
        {'href': CFG["root-catalog"], 'rel': 'parent'},
        {'href': CFG["root-catalog"], 'rel': 'root'},
        {'href': f'{CFG["aws-domain"]}/WOFLs/v2.1.0/combined/catalog.json', 'rel': 'child'},
        {'href': f'{CFG["aws-domain"]}/WOFLs/v2.1.5/combined/catalog.json', 'rel': 'child'},
        {'href': f'{CFG["aws-domain"]}/annual_summary/v2.1.5/combined/catalog.json', 'rel': 'child'},
        {'href': f'{CFG["aws-domain"]}/filtered_summary/v2.1.0/combined/catalog.json', 'rel': 'child'},
        {'href': f'{CFG["aws-domain"]}/summary/v2.1.0/combined/catalog.json', 'rel': 'child'}
    ]

    # Put collection catalog to s3
    obj = S3.Object(bucket, f'WOfS/catalog.json')
    obj.put(Body=json.dumps(collection_catalog), ContentType='application/json')


def do_fractional_cover(bucket):
    """
    Update fractional-cover ({ls5, ls7, ls8}_fc_albers, fc_percentile_annual, fc_percentile_seasonal)
    """

    collection_catalog = OrderedDict([
        ('stac_version', '0.6.0'),
        ('id', 'factional-cover'),
        ('description', 'Fractional cover suite of products include fractional cover products '
                        'corresponding to each landsat albers (ls5, ls7, ls8) and two fractional cover percentile '
                        'products fc-percentile-annual and fc-percentile-seasonal')])
    collection_catalog['license'] = CFG['license']['short_name']
    collection_catalog['extent'] = {'spatial': CFG['aus-extent']['spatial'],
                                    'temporal': CFG['aus-extent']['temporal']}
    collection_catalog['links'] = [
        {'href': f'{CFG["aws-domain"]}/fractional-cover/catalog.json', 'ref': 'self'},
        {'href': CFG["root-catalog"], 'rel': 'parent'},
        {'href': CFG["root-catalog"], 'rel': 'root'},
        {'href': f'{CFG["aws-domain"]}/fractional-cover/fc/v2.2.0/ls5/catalog.json', 'rel': 'child'},
        {'href': f'{CFG["aws-domain"]}/fractional-cover/fc/v2.2.0/ls7/catalog.json', 'rel': 'child'},
        {'href': f'{CFG["aws-domain"]}/fractional-cover/fc/v2.2.0/ls8/catalog.json', 'rel': 'child'},
        {'href': f'{CFG["aws-domain"]}/fractional-cover/fc-percentile/annual/v2.1.0/combined/catalog.json',
         'rel': 'child'},
        {'href': f'{CFG["aws-domain"]}/fractional-cover/fc-percentile/seasonal/v2.1.0/combined/catalog.json',
         'rel': 'child'}
    ]

    # Put collection catalog to s3
    obj = S3.Object(bucket, f'fractional-cover/catalog.json')
    obj.put(Body=json.dumps(collection_catalog), ContentType='application/json')


def do_geomedian_australia(bucket):
    """
    Geomedian-australia ({ls5, ls7, ls8}_nbart_geomedian_annual)
    """

    collection_catalog = OrderedDict([
        ('stac_version', '0.6.0'),
        ('id', 'geomedian-australia'),
        ('description', 'Geomedian Australia suite of products include geomedian products '
                        'corresponding to each landsat albers (ls5, ls7, ls8)')])
    collection_catalog['license'] = CFG['license']['short_name']
    collection_catalog['extent'] = {'spatial': CFG['aus-extent']['spatial'],
                                    'temporal': CFG['aus-extent']['temporal']}
    collection_catalog['links'] = [
        {'href': f'{CFG["aws-domain"]}/geomedian-australia/catalog.json', 'ref': 'self'},
        {'href': CFG["root-catalog"], 'rel': 'parent'},
        {'href': CFG["root-catalog"], 'rel': 'root'},
        {'href': f'{CFG["aws-domain"]}/geomedian-australia/v2.1.0/L5/catalog.json', 'rel': 'child'},
        {'href': f'{CFG["aws-domain"]}/geomedian-australia/v2.1.0/L7/catalog.json', 'rel': 'child'},
        {'href': f'{CFG["aws-domain"]}/geomedian-australia/v2.1.0/L8/catalog.json', 'rel': 'child'},
    ]

    # Put collection catalog to s3
    obj = S3.Object(bucket, f'geomedian-australia/catalog.json')
    obj.put(Body=json.dumps(collection_catalog), ContentType='application/json')


if __name__ == '__main__':
    update_stac_product_suite_catalogs()
