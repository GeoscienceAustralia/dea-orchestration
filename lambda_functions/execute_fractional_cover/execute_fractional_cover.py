import datetime
import logging
import os

from dea_raijin.auth import RaijinSession
from dea_raijin import OrchestratorException

LOG = logging.getLogger(__file__)


def handler(event, context):
    task_time = datetime.datetime.utcnow()

    year = event['year']
    output_product = event['output_product']
    tag = '{:%Y-%m-%dT%H%M%S}'.format(task_time)

    fc_module = os.environ['FC_MODULE']
    dea_module = os.environ['DEA_MODULE']
    project = os.environ['PROJECT']
    queue = os.environ['QUEUE']

    with RaijinSession(logger=LOG) as raijin:
        stdout, stderr, exit_code = raijin.exec_command(f'execute_fractional_cover --year {year}'
                                                        f' --output-product {output_product}'
                                                        f' --tag {tag}'
                                                        f' --fc-module {fc_module}'
                                                        f' --dea-module {dea_module}'
                                                        f' --queue {queue}'
                                                        f' --project {project}')
        if exit_code != 0:
            raise OrchestratorException(f"Error executing fractional cover for {year} {output_product}",
                                        stdout=stdout, stderr=stderr, exit_code=exit_code)

    return dict(stdout=stdout, stderr=stderr, exit_code=exit_code)
