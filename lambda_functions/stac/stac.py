from collections import OrderedDict
from dateutil.parser import parse
from pyproj import Proj, transform
from pathlib import Path
from parse import parse as pparse
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
    Assumed path structure would look like
    dea-public-data-dev/fractional-cover/fc/v2.2.0/ls5/x_-1/y_-11/2008/11/08/
            LS5_TM_FC_3577_-1_-11_20081108005928000000_v1508892769.yaml
    """

    s3 = boto3.resource('s3')

    # Extract message, i.e. yaml file href's
    file_items = event.get('Records', [])

    for file_item in file_items:
        # Load yaml file from s3
        bucket, s3_key = get_bucket_and_key(file_item)
        obj = s3.Object(bucket, s3_key)
        metadata_doc = yaml.load(obj.get()['Body'].read().decode('utf-8'))

        # Generate STAC dict
        s3_key_ = Path(s3_key)
        stac_s3_key = f'{s3_key_.parent}/{s3_key_.stem}_STAC.json'
        item_abs_path = f'{GLOBAL_CONFIG["aws-domain"]}/{stac_s3_key}'
        parent_abs_path = get_stac_item_parent(s3_key)
        stac_item = stac_dataset(metadata_doc, item_abs_path, parent_abs_path)

        # Put STAC dict to s3
        obj = s3.Object(bucket, stac_s3_key)
        obj.put(Body=json.dumps(stac_item))

        # Update parent catalogs
        update_parent_catalogs(stac_s3_key, s3, bucket)


def get_bucket_and_key(message):
    s3_event = json.loads(message["body"])["Records"][0]
    return s3_event["s3"]["bucket"]["name"], s3_event["s3"]["object"]["key"]


def stac_dataset(metadata_doc, item_abs_path, parent_abs_path):

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
            {'href': parent_abs_path, 'ref': 'parent'}
        ]),
        ('assets', {})
    ])
    bands = metadata_doc['image']['bands']
    for key in bands:
        path = metadata_doc['image']['bands'][key]['path']
        key = PRODUCT_CONFIG[product]['bands'][key] + ' GeoTIFF'

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
    template = '{prefix}/x_{x}/y_{y}/{}'
    params = pparse(template, s3_key).__dict__['named']
    key_parent_catalog = f'{params["prefix"]}/x_{params["x"]}/y_{params["y"]}/catalog.json'
    return f'{GLOBAL_CONFIG["aws-domain"]}/{key_parent_catalog}'


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
