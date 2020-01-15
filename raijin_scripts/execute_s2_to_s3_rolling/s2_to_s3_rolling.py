#!/usr/bin/env python3
import datetime
import logging
import os
import subprocess
import sys

import boto3
import botocore
import yaml
from odc.index import odc_uuid

handler = logging.StreamHandler()
LOG = logging.getLogger("s3_to_s3_rolling")
LOG.setLevel(logging.DEBUG)
LOG.addHandler(handler)

NCI_DIR = '/g/data/if87/datacube/002/S2_MSI_ARD/packaged'
S3_PATH = 'L2/sentinel-2-nbar/S2MSIARD_NBAR'


def find_granules(num_days, end_date, root_path=NCI_DIR):
    # Find the dates between the input date and today, inclusive, formatted like the directories
    dates = [(end_date - datetime.timedelta(days=x)).strftime("%Y-%m-%d") for x in range(num_days + 1)]

    # The list of folders will be returned and will contain all the granules available for
    # the date range specified above. Format is yyyy-mm-dd/granule
    list_of_granules = []

    for date in dates:
        dir_for_date = os.path.join(root_path, date)
        if os.path.exists(dir_for_date):
            granules = [date + "/" + name for name in os.listdir(dir_for_date)]
            list_of_granules += granules

    return(list_of_granules)


def check_granule_exists(s3_bucket, s3_metadata_path):
    s3 = boto3.resource('s3')

    try:
        # This does a head request, so is fast
        s3.Object(s3_bucket, s3_metadata_path).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
    else:
        return True


def sync_granule(granule, s3_bucket):
    local_path = os.path.join(NCI_DIR, granule)
    s3_path = "s3://{s3_bucket}/{s3_path}/{granule}".format(
        s3_bucket=s3_bucket,
        s3_path=S3_PATH,
        granule=granule
    )

    # Remove any data that shouldn't be there and exclude the metadatta and NBART
    command = "aws s3 sync {local_path} {s3_path} --delete --exclude NBART/* --exclude ARD-METADATA.yaml".format(
        local_path=local_path,
        s3_path=s3_path
    )

    return_code = subprocess.call(command, shell=True)

    # If the return code is zero, we have success.
    return return_code == 0


def replace_metadata(granule, s3_bucket, s3_metadata_path):
    s3 = boto3.resource("s3").Bucket(s3_bucket)

    yaml_file = "{nci_path}/{granule}/ARD-METADATA.yaml".format(
        nci_path=NCI_DIR,
        granule=granule
    )

    with open(yaml_file) as config_file:
        temp_metadata = yaml.load(config_file, Loader=yaml.CSafeLoader)

    del temp_metadata['image']['bands']['nbart_blue']
    del temp_metadata['image']['bands']['nbart_coastal_aerosol']
    del temp_metadata['image']['bands']['nbart_contiguity']
    del temp_metadata['image']['bands']['nbart_green']
    del temp_metadata['image']['bands']['nbart_nir_1']
    del temp_metadata['image']['bands']['nbart_nir_2']
    del temp_metadata['image']['bands']['nbart_red']
    del temp_metadata['image']['bands']['nbart_red_edge_1']
    del temp_metadata['image']['bands']['nbart_red_edge_2']
    del temp_metadata['image']['bands']['nbart_red_edge_3']
    del temp_metadata['image']['bands']['nbart_swir_2']
    del temp_metadata['image']['bands']['nbart_swir_3']
    del temp_metadata['lineage']
    temp_metadata['creation_dt'] = temp_metadata['extent']['center_dt']
    temp_metadata['product_type'] = 'S2MSIARD_NBAR'

    # Create dataset ID based on Kirill's magic
    temp_metadata['id'] = str(odc_uuid("s2_to_s3_rolling", "1.0.0", [temp_metadata['id']]))

    # Write to S3 directly
    s3.Object(key=s3_metadata_path).put(Body=yaml.dump(
        temp_metadata, default_flow_style=False, Dumper=yaml.CSafeDumper)
    )


def sync_dates(num_days, s3_bucket, end_date, update=False):
    # Since all file paths are of the form:
    # /g/data/if87/datacube/002/S2_MSI_ARD/packaged/YYYY-mm-dd/<granule>
    # we can simply list all the granules per date and sync them

    LOG.info("Syncing from the last {} days until {}".format(num_days, end_date))

    if end_date == 'today':
        datetime_end = datetime.datetime.today()
    else:
        datetime_end = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    # Get list of granules
    list_of_granules = find_granules(num_days, datetime_end)

    LOG.info("Found {} files to process".format(len(list_of_granules)))

    # For each granule, sync it if it needs syncing
    if len(list_of_granules) > 0:
        for granule in list_of_granules:
            LOG.info("Processing {}".format(granule))
            # s3://dea-public-data/L2/sentinel-2-nbar/S2MSIARD_NBAR/2017-07-02/S2A_OPER_MSI_ARD_TL_SGS__20170702T022539_A010581_T54LTL_N02.05/ARD-METADATA.yaml
            s3_metadata_path = "{s3_path}/{granule}/ARD-METADATA.yaml".format(
                s3_path=S3_PATH,
                granule=granule
            )

            already_processed = check_granule_exists(s3_bucket, s3_metadata_path)

            # Maybe todo: include a flag to force replace
            if not already_processed or update:
                sync_success = sync_granule(granule, s3_bucket)
                if sync_success:
                    # Replace the metadata with a deterministic ID
                    replace_metadata(granule, s3_bucket, s3_metadata_path)
                else:
                    LOG.error("Failed to sync data... skipping")
            else:
                LOG.warning("Metadata exists, not syncing {}".format(granule))
    else:
        LOG.warning("Didn't find any granules to process...")

    # Return success indicator?


if __name__ == '__main__':
    # Arg 1 is numdays, 2 is bucket, 3 is enddate
    num_days = int(sys.argv[1])
    s3_bucket = sys.argv[2]
    end_date = sys.argv[3]
    LOG.info("Starting sync with days {} going back from {} and a bucket of {}".format(
        num_days, end_date, s3_bucket
    ))
    sync_dates(num_days, s3_bucket, end_date)
