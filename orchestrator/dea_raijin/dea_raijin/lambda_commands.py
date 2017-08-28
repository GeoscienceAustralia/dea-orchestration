import logging

from . import config
from .auth import RaijinSession


class BaseCommand(object):
    """BaseCommand for Command inheritance.

    The function command should be overwritten by the inheriting class.

    Attributes:
        logger (obj): The logging obj
    """

    COMMAND_NAME = 'UNDEFINED'

    def __init__(self, command_instance=None, *args, **kwargs):
        self.logger = logging.getLogger(".".join([config.LOGGING_PREFIX, command_instance.COMMAND_NAME]))

    def command(self):
        """
        Raises:
            NotImplementedError: Method should be overwritten by inheriting class.
        """

        raise NotImplementedError('Command must override the command method')

    def run(self):
        """Runs self.command

        Returns:
            Statements returned by the command function

        """

        return self.command()


class RaijinCommand(BaseCommand):
    """RaijinCommand ssh's into Raijin servers before executing scripts.

    Attributes:
        raijin (obj): Hosts ssh_client to access ssh session directly and helper methods.
        logger (obj): logger obj defined by BaseCommand
    """

    def __init__(self, command_instance, *args, **kwargs):
        super().__init__(command_instance, args, kwargs)
        self.raijin = None

    def run(self):
        """Executes subclassed commands

        Returns:
            Statements returned by the command function
        """
        with RaijinSession(logger=self.logger) as raijin:
            self.raijin = raijin
            return self.command()

    def command(self):
        """
        Raises:
            NotImplementedError: Method should be overwritten by inheriting class.
        """

        raise NotImplementedError('Command must override the command method')
