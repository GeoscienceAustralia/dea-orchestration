from dea_raijin import RaijinCommand


class RaijinLogIngestCommand(RaijinCommand):
    """RaijinLogIngestCommand
    This command triggers the sync of the log files from raijin into s3 for ingestion into es
    """

    COMMAND_NAME = 'RaijinLogIngestCommand'

    def __init__(self):
        super().__init__(self)

    def command(self, *args, **kwargs):
        stdout, stderr, exit_code = self.raijin.exec_command('raijin_log_ingest')

        return stdout


def handler(event, context):
    return RaijinLogIngestCommand().run()
