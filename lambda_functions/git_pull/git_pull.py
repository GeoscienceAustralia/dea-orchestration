from datetime import datetime

from dea_raijin import BaseCommand
from dea_raijin.auth import RaijinSession, SSHConfig

SSM_PATH = 'orchestrator.raijin.users.git_pull'


class GitPullCommand(BaseCommand):

    COMMAND_NAME = 'GitPullCommand'

    def __init__(self):
        super().__init__(self, self)

    def command(self, *args, **kwargs):
        exit_code = 1

        with RaijinSession(logger=self.logger, ssh_config=SSHConfig().from_ssm_user_path(path=SSM_PATH)) as raijin:
            stdout, err_output, exit_code = raijin.exec_command('#cmd is ignored')

        now = datetime.now().isoformat()

        if exit_code:
            self.logger.error('Unable to update production: %s', now)
            return 1

        self.logger.info('Successfully updated production: %s', now)
        return 0


def handler(event, context):
    return GitPullCommand().run()
