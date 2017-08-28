from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from requests_aws4auth import AWS4Auth

from .config import AWS_REGION, _AWS_ES_ACCESS_KEY, _AWS_PRIVATE_KEY, HOST

_AWSAUTH = AWS4Auth(_AWS_ES_ACCESS_KEY, _AWS_PRIVATE_KEY, AWS_REGION, 'es')

ES_CONNECTION = Elasticsearch(
    hosts=[{'host': HOST, 'port': 443}],
    http_auth=_AWSAUTH,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)
