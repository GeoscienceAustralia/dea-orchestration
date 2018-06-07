import logging
import urllib
from os import path

import boto3

S3 = boto3.resource('s3')
LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


def generate_listing(event, context):
    new_objects = [(record['s3']['bucket']['name'], urllib.parse.unquote(record['s3']['object']['key'])) for record in
                   event['Records']]
    LOG.info("Changed/new objects: %s", str(new_objects))
    tasks = set()
    for bucket, obj in new_objects:
        if obj.endswith('index.html'):
            LOG.info("index.html: skipping")
            return

        currdir = path.dirname(obj)

        parent_dir = path.normpath(currdir + "/..")

        if currdir == ".":
            LOG.info("root directory: skipping")
            return

        tasks.add((bucket, currdir))

    for bucket, directory in tasks:
        process_directory(bucket, directory)


def process_directory(bucket, directory):
    LOG.info(f"Processing s3://{bucket}/{directory}")
    s3client = boto3.client('s3')
    response = s3client.list_objects_v2(Bucket=bucket, Prefix=f"{directory}/", Delimiter='/')

    files = [content['Key'] for content in response.get('Contents', [])]
    folders = [prefix['Prefix'] for prefix in response.get('CommonPrefixes', [])]

    index_path = path.join(directory, 'index.html')
    LOG.info(f"Found {len(files)} in {directory}. Updating '{index_path}'.")

    index_contents = generate_index_html(files)
    s3client.put_object(Bucket=bucket, Key=index_path, Body=index_contents, ContentType="text/html",
                        CacheControl='public, must-revalidate, proxy-revalidate, max-age=0')


def generate_index_html(objs):
    basename_only = [path.basename(obj) for obj in objs]
    no_index = [obj for obj in basename_only if obj != 'index.html']
    links = "\n".join(f'    <a href="{obj}">{obj}</a><br>' for obj in no_index)
    index_contents = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Package Index</title>
</head>
<body>
    {links}
</body>
</html>
"""
    return index_contents


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("bucket")
    parser.add_argument("directory")
    args = parser.parse_args()

    process_directory(args.bucket, args.directory)


if __name__ == '__main__':
    main()
