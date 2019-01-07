"""
AWS serverless lambda function that generate stac catalog file corresponding to yaml file
upload event.
"""

import datetime
import json
from collections import OrderedDict
from pathlib import Path

import boto3
import yaml
from dateutil.parser import parse
from parse import parse as pparse
from pyproj import Proj, transform

S3_RES = boto3.resource('s3')

# Read the config file
with open('stac_config.yaml', 'r') as cfg_file:
    CFG = yaml.load(cfg_file)


def stac_handler(event, context):
    """
    Receive Events about updated files in S3

    The assumed path structure looks like:

        dea-public-data-dev/fractional-cover/fc/v2.2.0/ls5/x_-1/y_-11/2008/11/08/
                LS5_TM_FC_3577_-1_-11_20081108005928000000_v1508892769.yaml
    """

    # Extract message, i.e. yaml file href's
    file_items = event.get('Records', [])

    for file_item in file_items:
        # Load YAML file from s3
        bucket, s3_key = get_bucket_and_key(file_item)

        if not is_valid_yaml(s3_key):
            continue

        obj = S3_RES.Object(bucket, s3_key)
        metadata_doc = yaml.load(obj.get()['Body'].read().decode('utf-8'))

        # Generate STAC dict
        s3_key_ = Path(s3_key)
        stac_s3_key = f'{s3_key_.parent}/{s3_key_.stem}_STAC.json'
        item_abs_path = f'{CFG["aws-domain"]}/{stac_s3_key}'
        parent_abs_path = f'{CFG["aws-domain"]}/{get_stac_item_parent(s3_key)}'
        stac_item = stac_dataset(metadata_doc, item_abs_path, parent_abs_path)

        # Put STAC dict to S3
        obj = S3_RES.Object(bucket, stac_s3_key)
        obj.put(Body=json.dumps(stac_item), ContentType='application/json')


def is_valid_yaml(s3_key):
    """
    Return whether the given key is valid
    """

    s3_key_ = Path(s3_key)
    for product_prefix in [p['prefix'] for p in CFG['products']]:
        if product_prefix in str(s3_key_.parent) and product_prefix != str(s3_key_.parent):
            return True
    return False


def get_bucket_and_key(message):
    """
    Parse the bucket and S3 key from the SQS message
    """

    s3_event = json.loads(message["body"])["Records"][0]
    return s3_event["s3"]["bucket"]["name"], s3_event["s3"]["object"]["key"]


def stac_dataset(metadata_doc, item_abs_path, parent_abs_path):
    """
    Returns a dict corresponding to a stac item catalog
    """

    if metadata_doc['grid_spatial']['projection'].get('valid_data', None):
        geodata = valid_coord_to_geojson(metadata_doc['grid_spatial']['projection']['valid_data']['coordinates'],
                                         metadata_doc['grid_spatial']['projection']['spatial_reference'])
    else:
        # Compute geometry from geo_ref_points
        points = [[list(point.values()) for point in
                   metadata_doc['grid_spatial']['projection']['geo_ref_points'].values()]]

        # last point and first point should be same
        points[0].append(points[0][0])

        geodata = valid_coord_to_geojson(points, metadata_doc['grid_spatial']['projection']['spatial_reference'])

    # Convert the date to add time zone.
    center_dt = parse(metadata_doc['extent']['center_dt'])
    center_dt = center_dt.replace(microsecond=0)
    time_zone = center_dt.tzinfo
    if not time_zone:
        center_dt = center_dt.replace(tzinfo=datetime.timezone.utc).isoformat()
    else:
        center_dt = center_dt.isoformat()

    stac_item = OrderedDict([
        ('id', metadata_doc['id']),
        ('type', 'Feature'),
        ('bbox', [metadata_doc['extent']['coord']['ll']['lon'],
                  metadata_doc['extent']['coord']['ll']['lat'],
                  metadata_doc['extent']['coord']['ur']['lon'],
                  metadata_doc['extent']['coord']['ur']['lat']]),
        ('geometry', geodata['geometry']),
        ('properties', {
            'datetime': center_dt,
            'provider': CFG['contact']['name'],
            'license': CFG['license']['name'],
            'copyright': CFG['license']['copyright'],
            'product_type': metadata_doc['product_type'],
            'homepage': CFG['homepage']
        }),
        ('links', [
            {'href': item_abs_path, 'rel': 'self'},
            {'href': parent_abs_path, 'rel': 'parent'}
        ]),
        ('assets', {
            band_name: {
                # "type"? "GeoTIFF" or image/vnd.stac.geotiff; cloud-optimized=true
                'href': band_data['path'],
                "required": 'true',
                "type": "GeoTIFF"
            }
            for band_name, band_data in metadata_doc['image']['bands'].items()
        })
    ])

    return stac_item


def valid_coord_to_geojson(valid_coord, spatial_reference):
    """
        The polygon coordinates come in Albers' format, which must be converted to
        lat/lon as in universal format in EPSG:4326
    """

    albers = Proj(init=spatial_reference)
    geo = Proj(init='epsg:4326')
    for i in range(len(valid_coord[0])):
        j = transform(albers, geo, valid_coord[0][i][0], valid_coord[0][i][1])
        valid_coord[0][i] = list(j)

    return {
        'geometry': {
            "type": "Polygon",
            "coordinates": valid_coord
        }
    }


def get_stac_item_parent(s3_key):
    """
    Parse the parent stac catalog from the given s3 key
    """

    for product_dict in [p_ for p_ in CFG['products']]:
        if product_dict['prefix'] in s3_key:
            template = product_dict['catalog_structure'][-1]
            template_ = '{prefix}/' + template + '/{}'
            params = pparse(template_, s3_key)
            if not params:
                template_ = template + '/{}'
                params = pparse(template_, s3_key)
                if not params:
                    raise NameError('Catalog template parsing error: ' + s3_key)
                return template.format(**params.named) + '/catalog.json'
            return ('{prefix}/' + template).format(**params.named) + '/catalog.json'
    raise NameError('Catalog template parsing error: No parent catalog for ' + s3_key)


def main():
    import sys
    infile, outfile = sys.argv[1], sys.argv[2]
    with open(infile) as fin, open(outfile, 'w') as fout:
        metadata_doc = yaml.safe_load(fin)
        stac_doc = stac_dataset(metadata_doc, '/example_abspath', '/')
        json.dump(stac_doc, fout, indent=4)


if __name__ == '__main__':
    main()
