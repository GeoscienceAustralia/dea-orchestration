import datetime

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
        stdout, stderr, exit_code = self.raijin.exec_command(f'execute_fractional_cover {year} {output_product} {tag}')

        return stdout


def handler(event, context):
    task_time = datetime.datetime.utcnow()

    year = event['year']
    output_product = event['output_product']
#    tag = context.aws_request_id
    tag = '{:%Y-%m-%dT%H%M%S}'.format(task_time)
    return ExecuteFractionalCoverCommand().run(output_product, year, tag)
