import boto3
import datetime
import logging
import requests

from datetime import datetime

bucket_name = "s3stat-monitoring"

LOG = logging.getLogger(__name__)


def handler(event, context):
    """Main Entry Point"""
    today = datetime.date.today()
    week = today.strftime("%Y%V")
    month = today.strftime("%Y%m")

    file_monthurl = (
        "https://s3.amazonaws.com/reports.s3stat.com/17448/dea-public-data/stats/month"
        + month
        + ".json"
    )
    file_url = (
        "https://s3.amazonaws.com/reports.s3stat.com/17448/dea-public-data/stats/week"
        + week
        + ".json"
    )
    o_week_file = week + ".json"
    o_month_file = month + ".json"

    # Create session
    s = requests.Session()

    # Next thing will be to visit URL for file to download
    # Download week file
    r = s.get(file_url)
    if r.status_code == requests.codes.ok:
        LOG.info(f"Downloaded {file_url} successfully.")

        s3 = boto3.client("s3")

        with open("/tmp/" + o_week_file, "wb") as output:
            output.write(r.content)
            output.read()

        s3.upload_file("/tmp/" + o_week_file, bucket_name, "stats/week/" + o_week_file)
        r.history.clear()

    # Download Month data
    r = s.get(file_monthurl)
    if r.status_code == requests.codes.ok:
        LOG.info(f"Month data {file_monthurl} downloaded successfully")

        s3 = boto3.client("s3")

        with open("/tmp/" + o_month_file, "wb") as output:
            output.write(r.content)

        s3.upload_file(
            "/tmp/" + o_month_file, bucket_name, "stats/month/" + o_month_file
        )
        r.history.clear()
