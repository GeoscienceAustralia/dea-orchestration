from pathlib import PurePosixPath


def yamls_in_inventory_list(keys, cfg):
    """
    Return generator of yaml files in s3 of products that belong to 'aws-products' in GLOBAL_CONFIG
    """
    prefixes = set(p['prefix'] for p in cfg['products'])
    for item in keys:
        if any(item.Key.startswith(prefix) for prefix in prefixes) and PurePosixPath(item.key).suffix == '.yaml':
            yield item.Key
