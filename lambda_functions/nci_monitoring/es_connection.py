import logging
import os

import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

LOG = logging.getLogger()
ES_HOST = os.environ['DEA_AWS_ES_HOST']
ES_PORT = 443

AWS_REGION = 'ap-southeast-2'


def get_es_connection():
    LOG.info('Connecting to the ES Endpoint, {%s}:{%s}', ES_HOST, ES_PORT)
    credentials = boto3.Session().get_credentials()
    auth = AWS4Auth(credentials.access_key, credentials.secret_key,
                    AWS_REGION, 'es', session_token=credentials.token)

    return Elasticsearch(
        hosts=[{'host': ES_HOST, 'port': ES_PORT}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
