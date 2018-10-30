from collections import OrderedDict
from dateutil.parser import parse
from pyproj import Proj, transform
from pathlib import Path
import datetime
import yaml
import json
import boto3

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
    }
}

PRODUCT_CONFIG = {
    "wofs_filtered_summary": {
        "description": "Wofs Filtered Summary algorithm ... ",
        "bands": {
            "confidence": "confidence",
            "wofs_filtered_summary": "wofs filtered summary"
        }
    }
}


def stac_handler(event, context):
    """
    Assumed path structure would look like
    https://s3-ap-southeast-2.amazonaws.com/dea-public-data/fractional-cover/fc/v2.2.0/ls5/x_-1/y_-11/2008/11/08/
            LS5_TM_FC_3577_-1_-11_20081108005928000000_v1508892769.yaml
    """

    s3 = boto3.resource('s3')

    # Extract message, i.e. yaml file href's
    yaml_files = event.get(['Records'], [])

    for yaml_file in yaml_files:
        # Load yaml file from s3
        yaml_file_ = Path(yaml_file)
        # Is this robust?
        bucket = yaml_file_.parts[2]
        obj = s3.Object(bucket, yaml_file)
        metadata_doc = yaml.load(obj.get()['Body'].read().decode('utf-8'))

        # Generate STAC dict
        stac_json_path = f'{yaml_file_.parent}/{yaml_file_.stem}_STAC.json'
        stac_item = stac_dataset(metadata_doc, stac_json_path)

        # Put STAC dict to s3
        obj = s3.Object(bucket, stac_json_path)
        obj.put(Body=json.dumps(stac_item))


def stac_dataset(metadata_doc, stac_json_path):

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
            'license': GLOBAL_CONFIG['license']['name'],
            'copyright': GLOBAL_CONFIG['license']['copyright'],
            'product_type': metadata_doc['product_type'],
            'homepage': GLOBAL_CONFIG['homepage']
        }),
        ('provider', GLOBAL_CONFIG['provider']),
        ('links', {
            "self": {
                'rel': 'self',
                'href': stac_json_path
            }
        }),
        ('assets', {})
    ])
    bands = metadata_doc['image']['bands']
    for key in bands:
        path = metadata_doc['image']['bands'][key]['path']
        key = PRODUCT_CONFIG[product]['bands'][key] + ' GeoTIFF'

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
