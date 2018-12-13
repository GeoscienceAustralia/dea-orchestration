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

4. `update_product_suite_catalogs.py`: Certain products belong to respective product
    suits. For example, `wofs_albers, wofs_filtered_summary, wofs_statistical_summary,` 
    and `wofs_annual_summary` belong to the suit `WOfS`. This script updates the
    `collection catalogs` corresponding to such suits. Example,
     
    ```bash
        python update_product_suit_catalogs.py -b dea-public-data-dev
    ```

#### Configuration Information
The default configuration information for the severless lambda function 
as well as all the scripts are contained in a single file: `stac_config.yaml`.

This file contains global information independent of specific product 
such as `license, contact, provider` and configuration information specific
to each product that part of the `STAC catalog`. A product is identified by its
'prefix' within a specific `bucket` (typically `dea-public-data`). Different vesions
of a same product is identified the corresponding `prefix`, and thus a different
version of the same product is viewed as a seperate product. The `name` of the 
product is typically `product name` of the corresponding `datacube indexed product`.
The `catalog structure` of the product must be specified. This structure is an 
ordered list of `templates` that are comformant of `python parse library`. Please,
ensure following rules when writing the catalog structure:

1. The last template specify a `STAC catalog` that hold `STAC item` links. Each of
these `STAC item` correspond to a `dataset` of the respective product and has a 
'.yaml' file holding the `metadata` information of the dataset.

2. The directory above the directory specified by the first template holds a 
`STAC Collection catalog`. If the product belongs to a `suit of products` such
as `WOfS`, there is a further top level collection catalog having links to each
of the products within that suit. 

3. When specifiying templates, please omit leading and trailing back-slashes (/). 
The following template structures were tested:

   ```
      - x_{x}
      - x_{x}/y_{y}
   ```

   ```
      - lon_{lon}
      - lon_{lon}/lat_{lat}
   ```

   ```
      - mangrove_cover/{x}_{y}
   ```

   ```
      - L2/sentinel-2-nrt/S2MSIARD
      - L2/sentinel-2-nrt/S2MSIARD/{year:4}-{month:2}-{day:2}
   ```