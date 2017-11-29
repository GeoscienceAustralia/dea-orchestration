import logging
import os
import shlex

from dea_raijin.auth import RaijinSession
from dea_raijin import OrchestratorException

LOG = logging.getLogger(__file__)


def handler(event, context):
    min_trash_age_hours = event['min_trash_age_hours']
    search_string = event['search_string']

    dea_module = os.environ['DEA_MODULE']
    project = os.environ['PROJECT']
    queue = os.environ['QUEUE']

    with RaijinSession(logger=LOG) as raijin:
        stdout, stderr, exit_code = raijin.exec_command(f'execute_clean'
                                                        f' --min-trash-age-hours {min_trash_age_hours}'
                                                        f' --search-string {shlex.quote(search_string)}'
                                                        f' --dea-module {dea_module}'
                                                        f' --queue {queue}'
                                                        f' --project {project}')
        if exit_code != 0:
            raise OrchestratorException(f"Error executing ",
                                        stdout=stdout, stderr=stderr, exit_code=exit_code)

    return dict(stdout=stdout, stderr=stderr, exit_code=exit_code)
