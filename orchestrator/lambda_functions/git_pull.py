from datetime import datetime

from dea_raijin import BaseCommand
from dea_raijin.auth import RaijinSession

GIT_PULL_SSM_KEY = 'orchestrator.raijin.pull_key'


class GitPullCommand(BaseCommand):

    COMMAND_NAME = 'GitPullCommand'

    def __init__(self):
        super().__init__(self, self)

    def command(self):
        exit_code = 1

        with RaijinSession(logger=self.logger, ssm_key=GIT_PULL_SSM_KEY) as raijin:
            stdout, err_output, exit_code = raijin.exec_command('#cmd is ignored')

        now = datetime.now().isoformat()

        if exit_code:
            self.logger.error('Unable to update producion: %s', now)
            return 1

        self.logger.info('Successfully updated production: %s', now)
        return 0


def handler(event, context):
    return GitPullCommand().run()

if __name__ == '__main__':
    handler(None, None)
