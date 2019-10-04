import boto3
import datetime
import json
import logging
import os
import os.path
import requests

from collections import defaultdict
from datetime import datetime
from matplotlib import cm
from pathlib import Path


colormap = cm.spring

bucket_name = "s3stat-monitoring"

file_path = "/serverless/s3stat-download/dev/"


LOG = logging.getLogger(__name__)

DEFAULT_SSM_USER_PATH = os.environ.get('SSM_USER_PATH')


def product_name(folder):
    parts = Path(folder).parts
    if parts[0] == 'mangrove_cover':
        return parts[0]
    return os.path.join(*parts[:2])


def spatial_id(folder):
    parts = Path(folder).parts
    print(parts)
    print(parts[-1])

    if len(parts) > 2 and parts[0] == 'L2' and parts[1] == 'sentinel-2-nrt' and parts[-2] in ['NBAR', 'NBART', 'QA',
                                                                                              'SUPPLEMENTARY',
                                                                                              'LAMBERTIAN']:
        try:
            return parts[-3].split("_")[-2][1:]
        except IndexError:
            print(folder)
    else:
        return '<none>'


def merge_pre(folder, dicts):
    return {
        'hits': max(int(d['hits']) for d in dicts),
        'bytes': sum(int(d['bytes']) for d in dicts),
        'spatial_id': spatial_id(folder),
        'product': product_name(folder),
        'folder': folder
    }


def group(entry_list, key):
    lookup = defaultdict(list)

    for d in entry_list:
        lookup[d[key]].append(d)

    return lookup


def merge(dicts):
    return {
        'hits': sum(int(d['hits']) for d in dicts),
        'bytes': sum(int(d['bytes']) for d in dicts)
    }


def read_json(filename):
    with open(filename, encoding='utf-8') as File:
        reader = json.loads(File.read())
        reader_files = reader['Files']
        results = []
        for i,v in reader_files.items():
            if str(i).endswith('.TIF') or str(i).endswith('.tif') or str(i).endswith('.tiff'):
                results.append({"folder": str(i), "hits": int(v[0]), "bytes": int(v[1])})
        return results


def ssh_config_from_ssm_user_path(path=DEFAULT_SSM_USER_PATH):
    return {'userid': get_ssm_parameter(path + '.userid'),
            'password': get_ssm_parameter(path + '.password')}


SSM = None


def get_ssm_parameter(name, with_decryption=True):
    global SSM
    if SSM is None:
        SSM = boto3.client('ssm')

    response = SSM.get_parameter(Name=name, WithDecryption=with_decryption)

    if response:
        try:
            return response['Parameter']['Value']
        except (TypeError, IndexError):
            LOG.error("AWS SSM parameter not found in '%s'. Check regions match, SSM is region specific.", response)
            raise
    raise AttributeError("Key '{}' not found in SSM".format(name))


def handler(event, context):
    """Main Entry Point"""
    today=datetime.date.today()
    week = today.strftime("%Y%V")
    month = today.strftime("%Y%m")

    file_monthurl = "https://s3.amazonaws.com/reports.s3stat.com/17448/dea-public-data/stats/month" + month + ".json"
    file_url = "https://s3.amazonaws.com/reports.s3stat.com/17448/dea-public-data/stats/week" + week + ".json"
    o_week_file = week + ".json"
    o_month_file = month + ".json"

    # Create session
    s = requests.Session()

    # Next thing will be to visit URL for file to download
    # Download week file
    r = s.get(file_url)
    if r.status_code == requests.codes.ok:
        LOG.info("File downloaded successfully")

        s3 = boto3.client('s3')

        with open('/tmp/'+o_week_file, 'wb') as output:
            output.write(r.content)
            output.read()

        s3.upload_file("/tmp/" + o_week_file, bucket_name, 'stats/week/' + o_week_file)
        r.history.clear()

    # Download Month data
    r = s.get(file_monthurl)
    if r.status_code == requests.codes.ok:
        LOG.info("Month File downloaded successfully")

        s3 = boto3.client('s3')

        with open('/tmp/' + o_month_file, 'wb') as output:
            output.write(r.content)

        s3.upload_file("/tmp/" + o_month_file, bucket_name, 'stats/month/' + o_month_file)
        r.history.clear()
