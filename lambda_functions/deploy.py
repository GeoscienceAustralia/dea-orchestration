import urllib

from dea_raijin import RaijinCommand


class DeployCommand(RaijinCommand):

    COMMAND_NAME = 'DeployCommand'

    def __init__(self, event):
        super().__init__(self)
        self.event = event

    def command(self):
        new_objects = [urllib.parse.unquote(record['s3']['object']['key']) for record in self.event['Records']]
        self.logger.info('Deploying objects: ' + ' '.join(new_objects))

        for obj in new_objects:
            stdout, stderr, exit_code = self.raijin.exec_command('deploy ' + obj)
            if exit_code != 0:
                self.logger.error('Error: exit code %s.', str(exit_code))
            else:
                self.logger.info('Done')


def handler(event, context):
    return DeployCommand(event).run()
