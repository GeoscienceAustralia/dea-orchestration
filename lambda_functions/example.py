from dea_raijin import RaijinCommand


class ExampleCommand(RaijinCommand):

    COMMAND_NAME = 'ExampleCommand'

    def __init__(self):
        super().__init__(self)

    def command(self):
        stdout, stderr, exit_code = self.raijin.exec_command('example')
        if exit_code == 0:
            self.logger.info('SUCCESS:')

        self.logger.info('OUT:')
        self.logger.info(stdout)
        self.logger.info('Done')

        return stdout


def handler(event, context):
    return ExampleCommand().run()
