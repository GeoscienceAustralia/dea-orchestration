"""
AWS serverless lambda function that generate stac catalog file corresponding to yaml file
upload event.
"""
import json
import logging
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

import boto3
import datetime
import pycrs
from dateutil.parser import parse
from parse import parse as pparse
from pathlib import Path, PurePosixPath
from pyproj import Proj, transform
import ruamel.yaml


LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

S3_RES = boto3.resource('s3')
YAML = ruamel.yaml.YAML(typ='safe')

# Read the config file
with open(Path(__file__).parent / 'stac_config.yaml', 'r') as cfg_file:
    CFG = YAML.load(cfg_file)


def stac_handler(event, context):
    """
    Receive Events about updated files in S3

    The assumed path structure looks like:

        dea-public-data-dev/fractional-cover/fc/v2.2.0/ls5/x_-1/y_-11/2008/11/08/
                LS5_TM_FC_3577_-1_-11_20081108005928000000_v1508892769.yaml
    """

    # LOG.debug('Received event: %s', json.dumps(event))
    # Extract message, i.e. yaml file href's
    file_items = event.get('Records', [])

    LOG.info('Event contains %s records', len(file_items))

    processed_files = convert_yamls(file_items)

    LOG.info('Converted %s ODC Datasets to STAC', processed_files)


def convert_yamls(file_items):
    with ThreadPoolExecutor(max_workers=1) as executor:
        results = executor.map(convert_yaml, file_items)
    return len(list(results))


def convert_yaml(file_message):
    """
    Convert an ODC Dataset YAML on S3 into a STAC JSON

    As specified in an S3 Notification message

    :return: True if successful, otherwise False
    """
    file_message_ = json.loads(file_message['body'])
    if 'Records' in file_message_:
        s3_event = file_message_["Records"][0]
        bucket, s3_key = s3_event["s3"]["bucket"]["name"], s3_event["s3"]["object"]["key"]
    else:
        LOG.info('No Records found in file event!')
        return False
    if not is_valid_yaml(s3_key):
        return False
    # Load YAML file from s3
    obj = S3_RES.Object(bucket, s3_key)
    metadata_doc = YAML.load(obj.get()['Body'].read().decode('utf-8'))
    # Generate STAC dict
    s3_key_ = PurePosixPath(s3_key)
    stac_s3_key = f'{s3_key_.parent}/{s3_key_.stem}_STAC.json'
    item_abs_path = f'{CFG["aws-domain"]}/{stac_s3_key}'
    parent_abs_path = f'{CFG["aws-domain"]}/{get_stac_item_parent(s3_key)}'
    stac_item = stac_dataset(metadata_doc, item_abs_path, parent_abs_path)
    # Put STAC dict to S3
    obj = S3_RES.Object(bucket, stac_s3_key)
    obj.put(Body=json.dumps(stac_item), ContentType='application/json')
    LOG.info('Successfully wrote s3://%s/%s STAC metadata.', bucket, stac_s3_key)
    return True


def is_valid_yaml(s3_key):
    """
    Return whether the given key is valid
    """

    # S3 Keys always have forward slashes
    s3_key_ = PurePosixPath(s3_key)

    if s3_key_.suffix != '.yaml':
        LOG.info('%s does not end in .yaml. Skipping.', s3_key)
        return False

    for product_prefix in [p['prefix'] for p in CFG['products']]:
        # We don't want the yaml file to be located in the top level directory of the product
        # since that could be a product definition file
        if product_prefix in str(s3_key_.parent) and product_prefix != str(s3_key_.parent):
            return True

    LOG.info('%s does not start with a configured prefix. Skipping.', s3_key)
    return False


def stac_dataset(metadata_doc, item_abs_path, parent_abs_path):
    """
    Returns a dict corresponding to a stac item catalog
    """

    if metadata_doc['grid_spatial']['projection'].get('valid_data', None):
        geodata = valid_coord_to_geojson(metadata_doc['grid_spatial']['projection']['valid_data'],
                                         metadata_doc['grid_spatial']['projection']['spatial_reference'])
    else:
        # Compute geometry from geo_ref_points
        points = [[list(point.values()) for point in
                   metadata_doc['grid_spatial']['projection']['geo_ref_points'].values()]]

        # last point and first point should be same
        points[0].append(points[0][0])

        geodata = valid_coord_to_geojson({'type': 'Polygon', 'coordinates': points},
                                         metadata_doc['grid_spatial']['projection']['spatial_reference'])

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
        ('geometry', geodata),
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
                'href': band_data['path'],
                "type": "image/vnd.stac.geotiff; cloud-optimized=true"
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

    coords = valid_coord['coordinates']
    try:
        albers = Proj(init=spatial_reference)
    except RuntimeError:
        albers = Proj(pycrs.parse.from_unknown_text(spatial_reference).to_proj4())

    geo = Proj(init='epsg:4326')
    for i in range(len(coords[0])):
        j = transform(albers, geo, coords[0][i][0], coords[0][i][1])
        coords[0][i] = list(j)

    return {"type": "Polygon", "coordinates": coords}


def get_stac_item_parent(s3_key):
    """
    Parse the parent stac catalog from the given s3 key
    """

    for product_dict in CFG['products']:
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
        metadata_doc = YAML.safe_load(fin)
        stac_doc = stac_dataset(metadata_doc, '/example_abspath', '/')
        json.dump(stac_doc, fout, indent=4)


if __name__ == '__main__':
    main()
