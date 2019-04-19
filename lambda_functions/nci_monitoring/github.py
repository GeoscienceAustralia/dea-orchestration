import logging
import os

import requests

from es_connection import upload_to_elasticsearch, get_connection
from utils import get_ssm_parameter

LOG = logging.getLogger(__name__)

_GH_TOKEN = get_ssm_parameter(os.environ['SSM_GH_TOKEN_PATH'])
GH_HEADERS = {"Authorization": "token " + _GH_TOKEN}

INDEX_PREFIX = 'github-stats-'


# Main EntryPoint
def record_repo_stats(event, context):
    owner = event['owner']
    repo = event['repo']

    stats = get_repo_stats(owner, repo)

    repo = stats['data']['repository']

    upload_es_template()

    upload_to_elasticsearch(repo, index_prefix=INDEX_PREFIX)


def gh_graphql_query(query, variables):
    # A simple function to use requests.post to make the API call. Note the json= section.
    request = requests.post('https://api.github.com/graphql',
                            json={'query': query, 'variables': variables},
                            headers=GH_HEADERS)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception("GH GraphQL query failed to run by returning code of {}. {}".format(request.status_code, query))


def get_commits_on_repo():
    query = '''
    query CommitsOnRepo($owner: String!, $name: String!, 
                        $hash: GitObjectID!, $branch: String!) {
      repository(owner:$owner, name:$name) {
                    name
        id
        selectedCommit:object(oid: $hash) {
          ...commitAuthor
        }

        latestCommit:ref(qualifiedName: $branch) {
          name
          prefix

          target {
            ...commitAuthor
          }
        }
      }
    }

    fragment commitAuthor on Commit {
      id
      message
      author {
        name
        email
        date
      }
    }
    '''
    variables = {
        "owner": "opendatacube",
        "name": "datacube-core",
        "hash": "ae60b5c58428bdf0004db4ba6b843fe137db4a32",
        "branch": "develop"
    }
    return gh_graphql_query(query, variables)


def get_closed_issue_actors(owner, repo):
    query = '''
    query($owner: String!, $repo: String!) {
        repository(owner: $owner, name: $repo){
      issues(states: CLOSED, first:10){
        edges{
          node{
            ... on Issue{
          timeline(last: 100){
            edges{
              node{
                __typename
                  ... on ClosedEvent{
                      actor {
                        login
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    }'''

    variables = {
        "owner": owner,
        "repo": repo
    }

    return gh_graphql_query(query, variables)


def get_repo_stats(owner, repo):
    query = '''
        query RepoStats($owner: String!, $repo: String!) { 
          repository(owner:$owner, name:$repo) {
                name
            id
                diskUsage
            forkCount
            pushedAt
            stargazers {
              totalCount
            }
            watchers {
              totalCount
            }
            issues {
              totalCount
            }
            openIssues: issues(states:OPEN) {
              totalCount
            }
            closedIssues: issues(states:CLOSED) {
              totalCount
            }
            pullRequests {
              totalCount
            }
            openPullRequests: pullRequests(states:OPEN) {
              totalCount
            }
            branches: refs(refPrefix: "refs/heads/") {
              totalCount
            }
            collaborators { totalCount }
            releases { totalCount }
            commits: object(expression:"develop") {
              ... on Commit {
                history {
                  totalCount
                }
              }
            }
          }
        }
    '''
    variables = {
        "owner": owner,
        "name": repo
    }

    return gh_graphql_query(query, variables)


def getRepoTraffic(owner, name):
    r = requests.get('https://api.github.com/user', headers=GH_HEADERS)


def upload_es_template():
    es_connection = get_connection()
    LOG.debug('Uploading Elastic Search Mapping Template: %s', es_connection)
    template = {
        'template': INDEX_PREFIX + '*',
        'mappings': {
            '_doc': {
                DOC_MAPPING
            }
        }
    }

    es_connection.indices.put_template('github-stats', body=template)


DOC_MAPPING = {
    "properties": {
        "nameWithOwner": {"type": "keyword"},
        "name": {"type": "keyword"},
        "id": {"type": "keyword"},
        "diskUsage": {"type": "integer"},
        "forkCount": {"type": "integer"},
        "pushedAt": {"type": "date"},
        "stargazers": {
            "properties": {
                "totalCount": {"type": "integer"}
            }
        },
        "watchers": {
            "properties": {
                "totalCount": {"type": "integer"}
            }
        },
        "issues": {
            "properties": {
                "totalCount": {"type": "integer"}
            }
        },
        "openIssues": {
            "properties": {
                "totalCount": {"type": "integer"}
            }
        },
        "closedIssues": {
            "properties": {
                "totalCount": {"type": "integer"}
            }
        },
        "pullRequests": {
            "properties": {
                "totalCount": {"type": "integer"}
            }
        },
        "openPullRequests": {
            "properties": {
                "totalCount": {"type": "integer"}
            }
        },
        "branches": {
            "properties": {
                "totalCount": {"type": "integer"}
            }
        },
        "collaborators": {
            "properties": {
                "totalCount": {"type": "integer"}
            }
        },
        "releases": {
            "properties": {
                "totalCount": {"type": "integer"}
            }
        },
        "commits": {
            "properties": {
                "history": {
                    "properties": {
                        "totalCount": {"type": "integer"}
                    }
                }
            }
        },
    }
}
