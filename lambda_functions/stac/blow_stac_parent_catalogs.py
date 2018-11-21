from pathlib import Path
import boto3
from dea.aws import make_s3_client
from dea.aws.inventory import list_inventory

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
    "root-catalog": "https://data.dea.ga.gov.au/catalog.json",
    "aws-products": ['WOfS', 'fractional-cover', 'geomedian-australia']
}


def delete_stac_catalog_parents():
    bucket = 'dea-public-data-dev'
    s3_client = boto3.client('s3')
    delete_files = dict(Objects=[])
    for item in list_inventory('s3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/',
                               s3=make_s3_client()):
        s3_key_file = Path(item.Key)

        # add to delete list
        if s3_key_file.name == 'catalog.json' and s3_key_file.parts[0] in GLOBAL_CONFIG['aws-products']:
            delete_files['Objects'].append(dict(Key=item.Key))

        # flush out the delete list if aws limit (1000) reached
        if len(delete_files['Objects']) >= 1000:
            s3_client.delete_objects(Bucket=bucket, Delete=delete_files)
            delete_files = dict(Objects=[])

    # flush out the remaining
    if len(delete_files['Objects']) >= 1000:
        s3_client.delete_objects(Bucket=bucket, Delete=delete_files)


if __name__ == '__main__':
    delete_stac_catalog_parents()
