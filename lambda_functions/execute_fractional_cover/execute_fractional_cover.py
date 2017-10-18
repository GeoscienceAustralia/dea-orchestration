import datetime
import os

from dea_raijin import RaijinCommand


class ExecuteFractionalCoverCommand(RaijinCommand):
    """
    This command triggers the execution of Fractional Cover processing on Raijin for a specified config (sensor)
    and year
    """

    COMMAND_NAME = 'ExecuteFractionalCoverCommand'

    def __init__(self):
        super().__init__(self)

    def command(self, output_product, year, tag):
        agdc_module = os.environ['AGDC_MODULE']
        fc_module = os.environ['FC_MODULE']
        dea_module = os.environ['DEA_MODULE']
        project = os.environ['PROJECT']
        queue = os.environ['QUEUE']
        stdout, stderr, exit_code = self.raijin.exec_command(f'execute_fractional_cover --year {year}'
                                                             f' --output-product {output_product}'
                                                             f' --tag {tag}'
                                                             f' --agdc-module {agdc_module}'
                                                             f' --fc-module {fc_module}'
                                                             f' --dea-module {dea_module}'
                                                             f' --queue {queue}'
                                                             f' --project {project}')

        return stdout


def handler(event, context):
    task_time = datetime.datetime.utcnow()

    year = event['year']
    output_product = event['output_product']
#    tag = context.aws_request_id
    tag = '{:%Y-%m-%dT%H%M%S}'.format(task_time)
    return ExecuteFractionalCoverCommand().run(output_product, year, tag)
