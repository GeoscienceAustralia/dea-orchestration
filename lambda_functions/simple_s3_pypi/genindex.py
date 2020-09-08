"""

"""
import logging
import urllib
from os import path
from operator import itemgetter

import boto3

S3 = boto3.resource("s3")
LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


def generate_listing(event, context):
    new_objects = [
        (
            record["s3"]["bucket"]["name"],
            urllib.parse.unquote(record["s3"]["object"]["key"]),
        )
        for record in event["Records"]
    ]
    LOG.info("Changed/new objects: %s", str(new_objects))
    tasks = set()
    for bucket, obj in new_objects:
        if obj.endswith("index.html"):
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
    LOG.info("Processing s3://%s/%s", bucket, directory)
    s3client = boto3.client("s3")
    response = s3client.list_objects_v2(
        Bucket=bucket, Prefix=f"{directory}/", Delimiter="/"
    )

    index_path = path.join(directory, "index.html")
    LOG.info(
        "Found '%s' in '%s'. Updating '%s'.",
        len(response["Contents"]),
        directory,
        index_path,
    )

    index_contents = generate_index_html(generate_links(response.get("Contents", [])))
    s3client.put_object(
        Bucket=bucket,
        Key=index_path,
        Body=index_contents,
        ContentType="text/html",
        CacheControl="public, must-revalidate, proxy-revalidate, max-age=0",
    )

def generate_links(objs):
    objs = [obj for obj in objs[1:]
            if not obj['Key'].endswith('index.html')]
    objs = sorted(objs, key=itemgetter('LastModified'))
    links = "\n".join(f"""
    <tr>
        <td align="left">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            <a href="{path.basename(obj['Key'])}"><tt>{path.basename(obj['Key'])}</tt></a></td>
        <td align="right"><tt>{sizeof_fmt(obj['Size'])}</tt></td>
        <td align="right"><tt>{obj['LastModified'].isoformat()}</tt></td>
    </tr>
    """ for obj in objs)
    return links

def regenerate_root_index(bucket):
    LOG.info("Regenerating root index in s3://%s", bucket)
    s3client = boto3.client("s3")
    response = s3client.list_objects_v2(Bucket=bucket, Delimiter="/")

    folders = [prefix["Prefix"][:-1] for prefix in response.get("CommonPrefixes", [])]
    LOG.info("Found '%s' folders.", len(folders))
    LOG.info("Folders: %s.", folders)

    links = "\n".join(f"""
    <tr>
        <td align="left">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
            <a href="{folder}"><tt>{folder}</tt></a></td>
        <td align="right"><tt></tt></td>
        <td align="right"><tt></tt></td>
    </tr>
    """ for folder in folders if folder)
    index_contents = generate_index_html(links, top_level=True)
    s3client.put_object(Bucket=bucket, Key='index.html', Body=index_contents, ContentType="text/html",
                        CacheControl='public, must-revalidate, proxy-revalidate, max-age=0')


def generate_index_html(links, top_level=False):
    if top_level:
        up_dir = ""
    else:
        up_dir = """
        <tr>
            <td align="left">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                <a href="../"><tt>../</tt></a></td>
            <td align="right"></td>
            <td align="right"></td>
        </tr>
        """
    index_contents = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Package Index</title>
    <style type="text/css">
        table tr:nth-child(2n) {{
            background: #e8e8e8;
        }}
    </style>
</head>
<body>
<table width="100%" cellspacing="0" cellpadding="5" align="center">
<tbody><tr>
<th align="left"><font size="+1">Name</font></th>
<th align="center"><font size="+1">Size</font></th>
<th align="right"><font size="+1">Last Modified</font></th>
</tr>
{up_dir}
{links}

</tbody></table>
</body>
</html>
"""
    return index_contents


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)


def main():
    logging.basicConfig()
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("bucket")
    parser.add_argument("directory", nargs="?", default=None)
    args = parser.parse_args()

    if args.directory is None:
        regenerate_root_index(args.bucket)
    else:
        process_directory(args.bucket, args.directory)


if __name__ == "__main__":
    main()
