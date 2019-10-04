import boto3
import csv
from collections import defaultdict
from datetime import datetime, date
import json
import os.path
from pathlib import Path
import re
from shapely.geometry import Polygon


CSV_FILE = 'data.csv'
S3_INPUT_BUCKET = 's3stat-monitoring'
S3_OUTPUT_BUCKET = 'dea-public-data-dev'

# Remove output file if it exists
if Path(CSV_FILE).is_file():
    Path(CSV_FILE).unlink()


def read_json(reader):
    '''
    Read the json input file
    '''
    reader_files = reader['Files']
    today = date.today()
    results = []
    for i, v in reader_files.items():
        if str(i).endswith('.TIF') or str(i).endswith('.tif') or str(i).endswith('.tiff'):
            results.append({"folder": str(i), "hits": int(v[0]), "bytes": int(v[1]), 'date': today})
    return results


def tile_index_from_path(path):
    def go(part):
        pattern = r'(?P<key>[xy])_(?P<num>.*)'
        match = re.match(pattern, part, re.IGNORECASE)
        if match is None:
            return {}
        gd = match.groupdict()
        return {gd['key']: gd['num']}

    result = {}
    for part in path.split('/'):
        result.update(go(part))
    return ((result['x']), (result['y']))


def product_name(folder):
    parts = Path(folder).parts
    if parts[0] == 'mangrove_cover':
        return parts[0]
    return os.path.join(*parts[:2])


def spatial_id(folder):
    parts = Path(folder).parts
    if parts[-2] in ['NBAR', 'NBART', 'QA', 'SUPPLEMENTARY', 'LAMBERTIAN']:
        pass
    if len(parts) > 2 and parts[0] == 'L2' and parts[1] == 'sentinel-2-nrt' and parts[-2] in ['NBAR', 'NBART', 'QA',
                                                                                              'SUPPLEMENTARY',
                                                                                              'LAMBERTIAN']:
        try:
            return parts[-3].split("_")[-2][1:]
        except IndexError:
            print(folder)
    if len(parts) > 2 and parts[0] == 'hltc' or parts[0] == 'item_v2':
        try:
            return parts[-1].split("_")[2]
        except IndexError:
            print(folder)
    if len(parts) > 2 and parts[0] == 'nidem':
        try:
            return parts[-1].split("_")[1]
        except IndexError:
            print(folder)
    elif len(parts) > 2 and parts[0] == 'bare-earth' or parts[0] == 'geomedian-australia' or parts[0] == 'WOfS' or \
            parts[0] == 'fractional-cover':
        return ','.join(tile_index_from_path(folder))
    elif len(parts) > 2 and parts[0] == 'projects' or parts[0] == 'weathering-intensity':
        return ' '
    elif len(parts) > 2 and parts[0] == 'multi-scale-topographic-position':
        return ' '
    else:
        return '<none>'


def coordsgeojson(spatialid):
    with open('australian-mgrs-tiles.geojson') as fl:
        input_gj = json.load(fl)
    feats = input_gj['features']
    for feat in feats:
        if 'MGRS' in feat['properties']:
            mgrs = feat['properties']['MGRS']
            if spatial_id == mgrs:
                polygon = Polygon(feat["geometry"]['coordinates'][0])
                lat = round(polygon.centroid.y, 1)
                lon = round(polygon.centroid.x, 1)
                return lat


def latcord(folder):
    parts = Path(folder).parts

    if len(parts) > 2 and parts[0] == 'hltc' or parts[0] == 'item_v2':
        return (parts[-1].split('.tif'))[0].split("_")[4]
    elif len(parts) > 2 and parts[0] == 'nidem':
        return parts[-1].split("_")[3]
    elif len(parts) > 2 and parts[0] == 'projects' or parts[0] == 'weathering-intensity':
        return ' '
    elif len(parts) > 2 and parts[0] == 'multi-scale-topographic-position':
        return ' '
    else:
        return '<none>'


def loncord(folder):
    parts = Path(folder).parts
    if len(parts) > 2 and parts[0] == 'hltc' or parts[0] == 'item_v2':
        return parts[-1].split("_")[3]
    elif len(parts) > 2 and parts[0] == 'nidem':
        return parts[-1].split("_")[2]
    elif len(parts) > 2 and parts[0] == 'projects' or parts[0] == 'weathering-intensity':
        return ' '
    elif len(parts) > 2 and parts[0] == 'multi-scale-topographic-position':
        return ' '
    else:
        return '<none>'


def merge_pre(folder_name, dicts, file_date):
    dt = datetime.strptime(file_date + '01', "%Y%m%d")
    return {
        'date': dt.strftime("%A, %d-%B-%Y"),
        'product': product_name(folder_name),
        'spatial_id': spatial_id(folder_name),
        'Lat': latcord(folder_name, spatial_id(folder_name)),
        'Lon': loncord(folder_name, spatial_id(folder_name)),
        'hits': max(int(d['hits']) for d in dicts),
        'bytes/GB': round(sum(int(d['bytes']) for d in dicts) / 1000000000, 2),
        'folder': folder_name
    }


def group(entry_list, key):
    lookup = defaultdict(list)

    for d in entry_list:
        lookup[d[key]].append(d)

    return lookup


def get_monthly_jsons(s3_client):
    every = [entry['Key']
             for entry in s3_client.list_objects(Bucket=S3_INPUT_BUCKET)['Contents']
             if entry['Key'].startswith('stats/month')]

    assert len(every) >= 2, "Not enough monthly .json files to pick the last completed one"
    return every[:-1]


def stats(monthly_json, s3_client, features):
    json_body = read_json(
        json.loads(s3_client.get_object(Bucket=S3_INPUT_BUCKET, Key=monthly_json)['Body'].read().decode('utf-8')))
    file_date = monthly_json.split('.')[0].split('/')[2]
    stage2 = [merge_pre(key, value, file_date) for key, value in group(json_body, 'folder').items()]

    products = [d for d in stage2]
    for feat in features:
        if 'label' in feat['properties']:
            label = feat['properties']['label']
        elif 'MGRS' in feat['properties']:
            label = feat['properties']['MGRS']
        else:
            raise

        for dict_item in products:
            if dict_item['spatial_id'] == label:
                polygon = Polygon(feat["geometry"]['coordinates'][0])
                dict_item['Lat'] = round(polygon.centroid.y, 1)
                dict_item['Lon'] = round(polygon.centroid.x, 1)

    with open(CSV_FILE, 'a+') as output_file:
        dict_writer = csv.DictWriter(output_file, list(stage2[0]))
        if Path(CSV_FILE).stat().st_size == 0:
            dict_writer.writeheader()  # file doesn't exist yet, write a header
        else:
            dict_writer.writerows(products)

    return products


def handler(event, context):
    """Main Entry Point"""
    session = boto3.Session(profile_name='devProfile')
    s3_client = session.client('s3')
    jsons = get_monthly_jsons(s3_client)

    # Extract MGRS tiles features
    with open('australian-mgrs-tiles.geojson') as fl:
        mgrs_features = json.load(fl)['features']

    # Extract albers grid features
    with open('albers_grid.geojson') as fl:
        albers_features = json.load(fl)['features']

    # Loop through files within s3stat-monitoring/stats/month bucket and process monthly
    for monthly_json in jsons:
        stats(monthly_json, s3_client, albers_features + mgrs_features)

    with open(CSV_FILE, 'rb') as newdata:
        # Upload a copy of the output csv file
        s3_client.put_object(Bucket=S3_OUTPUT_BUCKET,
                             Key=f's3-csv/data_{str(datetime.now().strftime("%Y%m%d_%H:%M:%S"))}.csv', Body=newdata)

        # Update the actual csv file
        s3_client.put_object(Bucket=S3_OUTPUT_BUCKET, Key=f's3-csv/data.csv', Body=newdata)

