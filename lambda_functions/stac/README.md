# Convert ODC Metadata to STAC Catalogs

Listens to an SQS queue for updated YAML Dataset Metadata
and converts them to STAC format.

1. Set up an event on `s3://dea-public-data` to add items to an SQS queue

2. Update the SQS in `serverless.yaml`

3. Execute to deploy

```
    npm install serverless-pseudo-parameters serverless-python-requirements
    sls deploy
```

The above steps will deploy the serverless lambda function given in `stac.py`. 
This lambda function creates `STAC item` json files corresponding to each
dataset uploaded for a specific product. S3 bucket must be configured so that
each `.yaml` file uploaded corresponding to a dataset of a specific product 
generates an `object create` event that is sent to the `SQS stac queue`. 
The messages in this queue triggers the lambda function to generate `STAC item`
json files. 

In addition to the serverless lambda and SQS infrastructure, the following scripts 
could be used to maintain the STAC catalog:

1. `stac_parent_update.py`: This script create/update `STAC catalog` jsons based 
on a given list of `s3 keys` corresponding to `dataset item yaml` files. This list
could be derived from a `s3 inventory list` or given on `command line`. For example,
parent catalog update corresponding to a single dataset could be done with
the following command:

```bash
    python stac_parent_update.py -b dea-public-data-dev \
        fractional-cover/fc/v2.2.0/ls8/x_-12/y_-12/2018/02/22/LS8_OLI_FC_3577_-12_-12_20180222125938.yaml
```

In the absence of a command line `.yaml` file list, the script derives the list
from the default inventory list (or you can specify the inventory manifest).

2. `notify_to_stac_queue.py`: This script sends notification messages to 
`stac queue` corresponding to a given list of `dataset item yaml` files. 
This list could be derived from `s3 inventory list` or given on `command line`.
For example, a stac item catalog corresponding to a dataset could be created,
manually, using the following command:

```bash
    python notify_to_stac_queue.py -b dea-public-data-dev \
        fractional-cover/fc/v2.2.0/ls8/x_-12/y_-12/2018/02/22/LS8_OLI_FC_3577_-12_-12_20180222125938.yaml
```

As with the `stac_parent_update.py` script, in the absence of a command line `.yaml` 
file list, the script derives the list
from the default inventory list (or you can specify the inventory manifest). 

3. `delete_stac_parent_catalogs.py`: This script scans through a bucket looking for 
`catalog.json` having a specific prefix and deletes them. This prefix typically 
specify a product.

The serverless lambda and the scripts uses a common configuration file. 
An example such file is given in `stac_config.yaml`. Currently the products
to be processed is specified by `aws-products` field of the config file. 
Among other things, config file specify global details and product specific 
details required for catalog generation and various `aws specific` information.
