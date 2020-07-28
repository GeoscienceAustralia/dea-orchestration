import logging
import urllib.parse

import boto3

S3 = boto3.resource("s3")
LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


def make_public(event, context):
    new_objects = [
        (
            record["s3"]["bucket"]["name"],
            urllib.parse.unquote(record["s3"]["object"]["key"]),
        )
        for record in event["Records"]
    ]
    LOG.info("Changed/new objects: %s", str(new_objects))
    for bucket, obj in new_objects:
        object_acl = S3.ObjectAcl(bucket, obj)
        object_acl.put(ACL="public-read")
    LOG.info("Made all objects public")
