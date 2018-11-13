from collections import OrderedDict
from dateutil.parser import parse
from pyproj import Proj, transform
from pathlib import Path
from parse import parse as pparse
import datetime
import yaml
import json
import boto3
from dea.aws import make_s3_client
from dea.aws.inventory import list_inventory


if __name__ == '__main__':
    manifest = 's3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/'
    # manifest += '2018-10-13T08-00Z/manifest.json' ## force for now, because of dev account permissions

    s3 = make_s3_client()

    full_inventory = list_inventory(manifest, s3=s3)

    for item in full_inventory:
        if Path(item.Key).suffix == '.yaml':
            # send a message to SQS
