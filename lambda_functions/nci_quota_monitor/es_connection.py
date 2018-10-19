import os

import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

HOST = os.environ['DEA_AWS_ES_HOST']

AWS_REGION = 'ap-southeast-2'


def get_es_connection():
    credentials = boto3.Session().get_credentials()
    auth = AWS4Auth(credentials.access_key, credentials.secret_key,
                    AWS_REGION, 'es', session_token=credentials.token)

    return Elasticsearch(
        hosts=[{'host': HOST, 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
