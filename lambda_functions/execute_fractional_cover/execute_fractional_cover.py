from dea_raijin import RaijinCommand


class ExecuteFractionalCoverCommand(RaijinCommand):
    """
    This command triggers the execution of Fractional Cover processing on Raijin for a specified config (sensor)
    and year
    """

    COMMAND_NAME = 'ExecuteFractionalCoverCommand'

    def __init__(self):
        super().__init__(self)

    def command(self, configuration_file, year):
        stdout, stderr, exit_code = self.raijin.exec_command(f'execute_fractional_cover {configuration_file} {year}')

        return stdout


def handler(event, context):
    # TODO: Pull year + sensor/config from the event
    year = None
    configuration_file = None
    return ExecuteFractionalCoverCommand().run(configuration_file, year)
