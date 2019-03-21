"""
This Pytest  script tests stac_parent_update.py, notify_to_stac_queue.py as well as
the serverless lambda function given in stac.py
"""
import json

import boto3
import pytest
import time
import yaml
from pathlib import Path

# from .notify_to_stac_queue import s3_key_to_stac_queue
# from .stac_parent_update import CatalogUpdater


@pytest.fixture
def s3_dataset_yamls():
    """
    Return a list of dict of test dataset info from various products
    """

    return [
        {'name': 'fractional-cover/fc/v2.2.0/ls8/x_-12/y_-12/2018/02/22/'
                 'LS8_OLI_FC_3577_-12_-12_20180222015938000000_v1521547282.yaml',
         'prefixes': [
             'fractional-cover/fc/v2.2.0/ls8/x_-12',
             'fractional-cover/fc/v2.2.0/ls8/x_-12/y_-12'
         ]},
        {'name': 'fractional-cover/fc/v2.2.0/ls5/x_-10/y_-12/1998/10/22/'
                 'LS5_TM_FC_3577_-10_-12_1998_v20171127041658_47.yaml',
         'prefixes': [
             'fractional-cover/fc/v2.2.0/ls5/x_-10',
             'fractional-cover/fc/v2.2.0/ls5/x_-10/y_-12'
         ]},
        {'name': 'fractional-cover/fc-percentile/annual/v2.1.0/combined/x_-12/y_-24/2010/'
                 'LS_FC_PC_3577_-12_-24_20100101.yaml',
         'prefixes': [
             'fractional-cover/fc-percentile/annual/v2.1.0/combined/x_-12',
             'fractional-cover/fc-percentile/annual/v2.1.0/combined/x_-12/y_-24'
         ]},
        {'name': 'fractional-cover/fc-percentile/seasonal/v2.1.0/combined/x_-14/y_-22/201712/'
                 'LS_FC_PC_3577_-14_-22_20171201_20180228.yaml',
         'prefixes': [
             'fractional-cover/fc-percentile/seasonal/v2.1.0/combined/x_-14',
             'fractional-cover/fc-percentile/seasonal/v2.1.0/combined/x_-14/y_-22'
         ]},
        {'name': 'WOfS/WOFLs/v2.1.5/combined/x_-12/y_-15/2013/12/02/'
                 'LS_WATER_3577_-12_-15_20131202015607000000_v1526758996.yaml',
         'prefixes': [
             'WOfS/WOFLs/v2.1.5/combined/x_-12',
             'WOfS/WOFLs/v2.1.5/combined/x_-12/y_-15'
         ]},
        {'name': 'WOfS/annual_summary/v2.1.5/combined/x_-10/y_-12/2012/WOFS_3577_-10_-12_2012_summary.yaml',
         'prefixes': [
             'WOfS/annual_summary/v2.1.5/combined/x_-10',
             'WOfS/annual_summary/v2.1.5/combined/x_-10/y_-12'
         ]},
        {'name': 'WOfS/filtered_summary/v2.1.0/combined/x_-10/y_-12/wofs_filtered_summary_-10_-12.yaml',
         'prefixes': [
             'WOfS/filtered_summary/v2.1.0/combined/x_-10',
             'WOfS/filtered_summary/v2.1.0/combined/x_-10/y_-12'
         ]},
        {'name': 'WOfS/summary/v2.1.0/combined/x_-18/y_-22/WOFS_3577_-18_-22_summary.yaml',
         'prefixes': [
             'WOfS/summary/v2.1.0/combined/x_-18',
             'WOfS/summary/v2.1.0/combined/x_-18/y_-22'
         ]},
        {'name': 'item_v2/v2.0.1/relative/lon_114/lat_-22/ITEM_REL_271_114.36_-22.31.yaml',
         'prefixes': [
             'item_v2/v2.0.1/relative/lon_114',
             'item_v2/v2.0.1/relative/lon_114/lat_-22'
         ]},
        {'name': 'mangrove_cover/-11_-20/MANGROVE_COVER_3577_-11_-20_20170101.yaml',
         'prefixes': [
             'mangrove_cover/-11_-20'
         ]}
    ]


@pytest.fixture
def config():
    """
    yield the default config file of stac repo
    """

    with open(f'{Path(__file__).parent}/stac_config.yaml', 'r') as cfg_file:
        yield yaml.load(cfg_file)


@pytest.fixture
def upload_yamls_from_prod_to_dev(s3_dataset_yamls):
    """
    Upload yaml files to dea-public-data-dev from dea-public-data corresponding
    to given list of dataset info
    """

    s3_res = boto3.resource('s3')
    for dts in s3_dataset_yamls:
        # Copy from dea-public-data to dea-public-data-dev
        s3_res.meta.client.copy({'Bucket': 'dea-public-data', 'Key': dts['name']},
                                'dea-public-data-dev', dts['name'])


def delete_stac_items_in_s3(s3_dataset_yamls, bucket):
    """
    Given a list of datasets, delete the existing STAC item json's from the s3 bucket
    """

    stac_items = [str(Path(dts['name']).parent) + '/' + Path(dts['name']).stem + '_STAC.json'
                  for dts in s3_dataset_yamls]

    # Delete the catalog files in s3
    s3_client = boto3.client('s3')
    s3_client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': obj} for obj in stac_items]})


def list_of_catalog_files(s3_dataset_yamls):
    """
    Compute and return the expected STAC catalog file names corresponding to
    the given list of dataset info
    """

    s3_keys = []
    for dataset in s3_dataset_yamls:
        for prefix in dataset['prefixes']:
            s3_keys.append(f'{prefix}/catalog.json')
        collection_prefix = str(Path(dataset['prefixes'][0]).parent)
        s3_keys.append(f'{collection_prefix}/catalog.json')
    return s3_keys


def test_stac_parent_update(s3_dataset_yamls, config):
    """
    Test the stac_parent_update script
    """

    # Define the bucket to be used for updates/deletes
    bucket = 'dea-public-data-dev'

    # Get the list of catalogs catalogs
    catalogs = list_of_catalog_files(s3_dataset_yamls)

    # Delete the catalog files in s3
    s3_client = boto3.client('s3')
    s3_client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': obj} for obj in catalogs]})

    # Update all parent catalogs
    CatalogUpdater(config).update_parents_all([dts['name'] for dts in s3_dataset_yamls], bucket)

    # Now check to see whether list of catalogs exist in s3
    for catalog in catalogs:
        assert s3_client.head_object(Bucket=bucket,
                                     Key=catalog).get('ResponseMetadata', None) is not None


from moto import mock_s3, mock_sqs


@mock_s3
@mock_sqs
def test_generate_stac_item():
    bucket_name = 'dea-public-data-dev'
    key = "fractional-cover/fc/v2.2.0/ls5/x_-5/y_-23/2010/02/13/LS5_TM_FC_3577_-5_-23_20100213122216.yaml"
    s3 = boto3.resource('s3')
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    bucket = s3.create_bucket(Bucket=bucket_name)

    bucket.upload_file(str(Path(__file__).parent / 'tests/LS5_TM_FC_3577_-5_-23_20100213012240.yaml'), key)

    event_body = {'Records': [
        {"s3":
            {
                "bucket": {
                    "name": bucket_name},
                "object": {
                    "key": key}
            }
        }
    ]}
    event = {'Records': [{'body': json.dumps(event_body)}]}

    from stac import stac_handler
    stac_handler(event, {})

    expected_key = key.replace('.yaml', '_STAC.json')
    obj = bucket.Object(expected_key)

    assert obj.content_type == 'application/json'
    stac_json = json.load(obj.get()['Body'])
    assert 'id' in stac_json
    assert stac_json.get('type', None) == 'Feature'




def test_stac_items(s3_dataset_yamls, upload_yamls_from_prod_to_dev):
    """
    We upload datasets corresponding to given yaml files from prod to dev and
    send messeges to SQS queue to create STAC item catalogs
    """
    sqs = boto3.client('sqs')

    delete_stac_items_in_s3(s3_dataset_yamls, 'dea-public-data-dev')

    queue_url = 'https://sqs.ap-southeast-2.amazonaws.com/451924316694/static-stac-queue'

    for dts in s3_dataset_yamls:
        # send a message to SQS
        s3_key_to_stac_queue(sqs, queue_url, 'dea-public-data-dev', dts['name'])

    # We may need to wait here a bit until messages in the queue are delivered
    # This should be at least timeout of lambda
    time.sleep(500)

    s3_client = boto3.client('s3')
    for dts in s3_dataset_yamls:
        stac_item_file = str(Path(dts['name']).parent) + '/' + Path(dts['name']).stem + '_STAC.json'
        assert s3_client.head_object(Bucket='dea-public-data-dev',
                                     Key=stac_item_file).get('ResponseMetadata', None) is not None
