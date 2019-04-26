import logging

import requests

LOG = logging.getLogger(__name__)


class GitHubStatsRetriever:
    """
    Retrieve stats about GitHub repositories

    :param string token: GitHub Authentication Token
    """

    def __init__(self, token):
        self.token = token

    def get_repo_stats(self, owner, repo):
        """
        Retrieve combined Fundamental and Traffic Stats
        :param owner:
        :param repo:
        :return:
        """
        stats = self.get_graphql_stats(owner, repo)

        stats['traffic'] = self.get_repo_traffic(owner, repo)

        return stats

    def get_graphql_stats(self, owner, repo):
        # language=GraphQL
        query = '''
            query RepoStats($owner: String!, $repo: String!) { 
              repository(owner:$owner, name:$repo) {
                name
                nameWithOwner
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
            "repo": repo
        }

        response = self.gh_graphql_query(query, variables)
        return response['data']['repository']

    def get_repo_traffic(self, owner, repo):
        LOG.info('Requesting GitHub Repo Traffic information for %s/%s', owner, repo)
        gh_headers = {"Authorization": "token " + self.token}
        url_prefix = f'https://api.github.com/repos/{owner}/{repo}/traffic/'
        parts = ['popular/referrers', 'popular/paths', 'views', 'clones']

        traffic = {}
        for part in parts:
            r = requests.get(url_prefix + part, headers=gh_headers)
            traffic[part] = r.json()

        return traffic

    def gh_graphql_query(self, query, variables):
        # A simple function to use requests.post to make the API call. Note the json= section.
        gh_headers = {"Authorization": "token " + self.token}
        request = requests.post('https://api.github.com/graphql',
                                json={'query': query, 'variables': variables},
                                headers=gh_headers)
        if request.status_code == 200:
            return request.json()
        else:
            raise Exception(
                "GH GraphQL query failed to run by returning code of {}. {}".format(request.status_code, query))


ES_GH_MAPPING_DOC = {
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
