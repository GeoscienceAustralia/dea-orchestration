import logging
import os

from dea_monitoring.elasticsearch import upload_to_elasticsearch
from .elasticsearch import get_connection, upload_es_template
from .github_metrics import GitHubStatsRetriever, ES_GH_MAPPING_DOC
from .log_cfg import setup_logging
from .utils import get_ssm_parameter

LOG = logging.getLogger(__name__)

_GH_TOKEN = get_ssm_parameter(os.environ['SSM_GH_TOKEN_PATH'])

INDEX_PREFIX = os.environ.get('GH_INDEX_PREFIX', 'github-stats-')

ES_CONN = get_connection()

stats_retriever = GitHubStatsRetriever(_GH_TOKEN)

setup_logging()


# Lambda EntryPoint
def record_repo_stats(event, context):
    owner = event['owner']
    repo = event['repo']

    upload_es_template(ES_CONN, INDEX_PREFIX, ES_GH_MAPPING_DOC)

    stats = stats_retriever.get_repo_stats(owner, repo)

    upload_to_elasticsearch(ES_CONN, stats, index_prefix=INDEX_PREFIX)
