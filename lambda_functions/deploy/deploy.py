import urllib

from dea_raijin import RaijinCommand, OrchestratorException


class DeployCommand(RaijinCommand):
    COMMAND_NAME = 'DeployCommand'

    def __init__(self, event):
        super().__init__(self)
        self.event = event

    def command(self, *args, **kwargs):
        new_objects = [urllib.parse.unquote(record['s3']['object']['key']) for record in self.event['Records']]
        self.logger.info('Deploying objects: %s', ' '.join(new_objects))

        for obj in new_objects:
            stdout, stderr, exit_code = self.raijin.exec_command('deploy ' + obj)
            if exit_code != 0:
                self.logger.error('Error: exit code %s.', str(exit_code))
                raise OrchestratorException("Error deploying %s" % obj,
                                            stdout=stdout,
                                            stderr=stderr,
                                            exit_code=exit_code)
            else:
                self.logger.info('Done')

        return {
            'result': 'Success',
            'deployed_objects': new_objects
        }


def handler(event, context):
    return DeployCommand(event).run()
