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