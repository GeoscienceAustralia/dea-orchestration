import os

from dea_monitoring.github_metrics import GitHubStatsRetriever

EXAMPLE_RESPONSE = {
    "data": {
        "repository": {
            "nameWithOwner": "opendatacube/datacube-core",
            "name": "datacube-core",
            "id": "MDEwOlJlcG9zaXRvcnkzNTUzMTAyMg==",
            "diskUsage": 41879,
            "forkCount": 74,
            "pushedAt": "2019-04-18T06:47:13Z",
            "stargazers": {
                "totalCount": 156
            },
            "watchers": {
                "totalCount": 47
            },
            "issues": {
                "totalCount": 370
            },
            "openIssues": {
                "totalCount": 61
            },
            "closedIssues": {
                "totalCount": 309
            },
            "pullRequests": {
                "totalCount": 342
            },
            "openPullRequests": {
                "totalCount": 4
            },
            "branches": {
                "totalCount": 22
            },
            "collaborators": {
                "totalCount": 41
            },
            "releases": {
                "totalCount": 37
            },
            "commits": {
                "history": {
                    "totalCount": 4637
                }
            }
        }
    }
}


def test_get_repo_stats():
    """
    Integration Test - That we can retrieve public statistics about a GH repository

    Requires an environment variable `GH_TOKEN` to be set with a valid GitHub Token with `public` scope permission
    """
    token = os.environ['GH_TOKEN']

    retriever = GitHubStatsRetriever(token)
    stats = retriever.get_repo_stats('opendatacube', 'datacube-core')

    assert stats['name'] == 'datacube-core'
    assert stats['nameWithOwner'] == 'opendatacube/datacube-core'

    assert 'diskUsage' in stats
    assert 'forkCount' in stats
    assert 'pushedAt' in stats

    expected_metrics = 'stargazers watchers issues openIssues closedIssues ' \
                       'pullRequests openPullRequests branches collaborators releases'.split()

    for metric in expected_metrics:
        assert metric in stats
        assert isinstance(stats[metric]['totalCount'], int)

    assert 'traffic' in stats
    assert 'popular/referrers' in stats['traffic']
    assert 'popular/paths' in stats['traffic']
    assert 'views' in stats['traffic']
    assert 'clones' in stats['traffic']
