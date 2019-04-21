import requests

from dea_monitoring.es_connection import upload_to_elasticsearch
from dea_monitoring.github_lambda import LOG, INDEX_PREFIX


def get_repo_stats(owner, repo):
    stats = graphql_stats(owner, repo)

    stats['traffic'] = get_repo_traffic(owner, repo)

    return stats


def graphql_stats(owner, repo):
    # language=GraphQL
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

    response = gh_graphql_query(query, variables)
    return response['data']['repository']


def get_repo_traffic(owner, repo, token):
    LOG.info('Requesting GitHub Repo Traffic information for %s/%s', owner, repo)
    gh_headers = {"Authorization": "token " + token}
    url_prefix = f'https://api.github.com/repos/{owner}/{repo}/traffic/'
    parts = ['popular/referrers', 'popular/paths', 'views', 'clones']

    traffic = {}
    for part in parts:
        r = requests.get(url_prefix + part, headers=gh_headers)
        traffic[part] = r.json()

    return traffic


def upload_es_template(es_connection):
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


class GitHubRecorder:
    def __init__(self, token, stats_retriever, es_connection):
        self.token = token
        self.stats_retriever = stats_retriever
        self.es_connection = es_connection

        upload_es_template(es_connection)

    def gh_stats_to_es(self, owner, repo):
        stats = self.stats_retriever(self.token, owner, repo)

        upload_to_elasticsearch(self.es_connection, stats, index_prefix=INDEX_PREFIX)


def gh_graphql_query(query, variables, token):
    # A simple function to use requests.post to make the API call. Note the json= section.
    gh_headers = {"Authorization": "token " + token}
    request = requests.post('https://api.github.com/graphql',
                            json={'query': query, 'variables': variables},
                            headers=gh_headers)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception("GH GraphQL query failed to run by returning code of {}. {}".format(request.status_code, query))
