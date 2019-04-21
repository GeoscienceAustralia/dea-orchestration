import logging
import os

from dea_monitoring.github_stats import get_repo_stats, GitHubRecorder
from .es_connection import get_connection
from .utils import get_ssm_parameter

LOG = logging.getLogger(__name__)

_GH_TOKEN = get_ssm_parameter(os.environ['SSM_GH_TOKEN_PATH'])

INDEX_PREFIX = 'github-stats-'

ES_CONN = get_connection()

recorder = GitHubRecorder(_GH_TOKEN, get_repo_stats, ES_CONN)


# Main EntryPoint
def record_repo_stats(event, context):
    owner = event['owner']
    repo = event['repo']

    recorder.gh_stats_to_es(owner, repo)
