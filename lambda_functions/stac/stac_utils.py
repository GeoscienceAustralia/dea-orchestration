import dateutil.parser


def yamls_in_inventory_list(keys, cfg):
    """
    Return generator of yaml files in s3 of products that belong to 'aws-products' in GLOBAL_CONFIG
    """
    prefixes = set(p['prefix']
                   for p in cfg['products'])
    for item in keys:
        if item.key.endswith('.yaml') and any(item.Key.startswith(prefix)
                                              for prefix in prefixes):
            yield item.Key


def parse_date(context, param, value):
    """
    Click callback to parse a date string
    """
    if value is None:
        return None
    try:
        return dateutil.parser.parse(value)
    except ValueError as error:
        raise ValueError('unparseable date') from error
