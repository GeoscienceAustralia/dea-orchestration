import logging
import os

from dea_raijin.auth import RaijinSession
from dea_raijin import OrchestratorException

LOG = logging.getLogger(__file__)


def handler(event, context):
    year = event['year']
    app_config_file = event['app_config_file']

    dea_module = os.environ['DEA_MODULE']
    project = os.environ['PROJECT']
    queue = os.environ['QUEUE']

    with RaijinSession(logger=LOG) as raijin:
        stdout, stderr, exit_code = raijin.exec_command(f'execute_stacker'
                                                        f' --year {year}'
                                                        f' --app-config-file {app_config_file}'
                                                        f' --dea-module {dea_module}'
                                                        f' --queue {queue}'
                                                        f' --project {project}')
        if exit_code != 0:
            raise OrchestratorException(f"Error executing stacker for {year} {app_config_file}",
                                        stdout=stdout, stderr=stderr, exit_code=exit_code)

    return dict(stdout=stdout, stderr=stderr, exit_code=exit_code)
