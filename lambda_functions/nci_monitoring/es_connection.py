import logging
import os
from datetime import datetime

import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

from log_cfg import LOG

LOG = logging.getLogger()
ES_HOST = os.environ['AWS_ES_HOST']
ES_PORT = int(os.environ.get('ES_PORT', 443))

AWS_REGION = os.environ['AWS_REGION']

_ES_CONNECTION = None


def get_connection():
    global _ES_CONNECTION
    if _ES_CONNECTION is not None:
        return _ES_CONNECTION
    else:
        LOG.info('Connecting to the ES Endpoint, {%s}:{%s}', ES_HOST, ES_PORT)
        credentials = boto3.Session().get_credentials()
        auth = AWS4Auth(credentials.access_key, credentials.secret_key,
                        AWS_REGION, 'es', session_token=credentials.token)

        _ES_CONNECTION = Elasticsearch(
            hosts=[{'host': ES_HOST, 'port': ES_PORT}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
        return _ES_CONNECTION


def upload_to_elasticsearch(doc, index_prefix, index_time_suffix='%Y'):
    es_connection = get_connection()
    now = datetime.utcnow()

    doc = doc.copy()
    doc['@timestamp'] = now.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    summary = es_connection.index(index=index_prefix + now.strftime(index_time_suffix),
                                  doc_type='_doc',
                                  body=doc)
    LOG.info(summary)
