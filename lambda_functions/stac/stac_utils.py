from pathlib import Path
from pandas import Timestamp


def yamls_in_inventory_list(keys, cfg):
    """
    Return generator of yaml files in s3 of products that belong to 'aws-products' in GLOBAL_CONFIG
    """
    prefixes = [p['prefix'] for p in cfg['products'] if p['prefix']]
    for item in keys:
        if bool(sum([item.Key.startswith(prefix) and Path(item.key).suffix == '.yaml' for prefix in prefixes])):
            yield item.Key


def incremental_list(inventory_s3_keys, from_date):
    """
    Filter the given generator list with items having LastModifiedDate attribute to a generator with the
    last modified date later than the given date
    """
    for item in inventory_s3_keys:
        time_modified = Timestamp(item.LastModifiedDate)
        if from_date < time_modified:
            yield item
