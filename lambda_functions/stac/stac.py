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

PRODUCT_CONFIG = {
    "wofs_filtered_summary": {
        "description": "Wofs Filtered Summary algorithm ... ",
        "bands": {
            "confidence": "confidence",
            "wofs_filtered_summary": "wofs filtered summary"
        }
    },
    "wofs": {
        "description": "WoFS algorithm ... ",
        "bands": {
            "water": "water",
        }
    },
    "fractional_cover": {
        "description": "The Fractional Cover (FC)",
        "bands": {
            "PV": "Photosynthetic Vegetation",
            "NPV": "Non-Photosynthetic Vegetation",
            "BS": "Bare Soil",
            "UE": "Unmixing Error"
        }
    }
}


def stac_handler(event, context):
    """
    Receive Events about updated files in S3
    Assumed path structure would look like
    dea-public-data-dev/fractional-cover/fc/v2.2.0/ls5/x_-1/y_-11/2008/11/08/
            LS5_TM_FC_3577_-1_-11_20081108005928000000_v1508892769.yaml
    """

    s3_res = boto3.resource('s3')

    # Extract message, i.e. yaml file href's
    file_items = event.get('Records', [])

    for file_item in file_items:
        # Load yaml file from s3
        bucket, s3_key = get_bucket_and_key(file_item)
        obj = s3_res.Object(bucket, s3_key)
        metadata_doc = yaml.load(obj.get()['Body'].read().decode('utf-8'))

        # Generate STAC dict
        s3_key_ = Path(s3_key)
        stac_s3_key = f'{s3_key_.parent}/{s3_key_.stem}_STAC.json'
        item_abs_path = f'{GLOBAL_CONFIG["aws-domain"]}/{stac_s3_key}'
        parent_abs_path = get_stac_item_parent(s3_key)
        stac_item = stac_dataset(metadata_doc, item_abs_path, parent_abs_path)

        # Put STAC dict to s3
        obj = s3_res.Object(bucket, stac_s3_key)
        obj.put(Body=json.dumps(stac_item))


def get_bucket_and_key(message):
    """
    Parse the bucket and s3 key from the SQS message
    """

    s3_event = json.loads(message["body"])["Records"][0]
    return s3_event["s3"]["bucket"]["name"], s3_event["s3"]["object"]["key"]


def stac_dataset(metadata_doc, item_abs_path, parent_abs_path):
    """
    Returns a dict corresponding to a stac item catalog
    """

    product = metadata_doc['product_type']
    geodata = valid_coord_to_geojson(metadata_doc['grid_spatial']
                                     ['projection']['valid_data']
                                     ['coordinates'])

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
            'provider': GLOBAL_CONFIG['contact']['name'],
            'license': GLOBAL_CONFIG['licence']['name'],
            'copyright': GLOBAL_CONFIG['licence']['copyright'],
            'product_type': metadata_doc['product_type'],
            'homepage': GLOBAL_CONFIG['homepage']
        }),
        ('links', [
            {'href': item_abs_path, 'rel': 'self'},
            {'href': parent_abs_path, 'rel': 'parent'}
        ]),
        ('assets', {})
    ])
    bands = metadata_doc['image']['bands']
    for key in bands:
        path = metadata_doc['image']['bands'][key]['path']
        key = PRODUCT_CONFIG[product]['bands'][key]

        # "type"? "GeoTIFF" or image/vnd.stac.geotiff; cloud-optimized=true
        stac_item['assets'][key] = {
            'href': path,
            "required": 'true',
            "type": "GeoTIFF"
        }

    return stac_item


def valid_coord_to_geojson(valid_coord):
    """
        The polygon coordinates come in Albers' format, which must be converted to
        lat/lon as in universal format in EPSG:4326
    """

    albers = Proj(init='epsg:3577')
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
    template = '{prefix}/x_{x}/y_{y}/{}'
    params = pparse(template, s3_key).__dict__['named']
    key_parent_catalog = f'{params["prefix"]}/x_{params["x"]}/y_{params["y"]}/catalog.json'
    return f'{GLOBAL_CONFIG["aws-domain"]}/{key_parent_catalog}'


def main():
    import sys
    infile, outfile = sys.argv[1], sys.argv[2]
    with open(infile) as fin, open(outfile, 'w') as fout:
        metadata_doc = yaml.safe_load(fin)
        stac_doc = stac_dataset(metadata_doc, '/example_abspath', '/')
        json.dump(stac_doc, fout, indent=4)


if __name__ == '__main__':
    main()
