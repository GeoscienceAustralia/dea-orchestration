import boto3
import paramiko
import logging
from io import StringIO

from .ssm_key_retrieve import get_ssm_parameter

from .. import config


class _IgnorePolicy(paramiko.MissingHostKeyPolicy):
    """Suppress error and warning for missing host key."""
    def missing_host_key(self, client, hostname, key):
        pass


class RaijinSession(object):
    """Establishes an ssh connection with raijin login node.

    Attributes:
        username (str): account name for ssh
        host (str): host to ssh into
        logger (obj): Logging object for raijin commands
    """

    def __init__(self, username=config.RAIJIN_USER, host=config.RAIJIN_HOST, logger=None,
                 ssm_key=config.DEFAULT_SSM_KEY):
        """Constructs an ssh session to raijin.
        Args:
            username (str): account name for ssh
            host (str): host to ssh into
            logger (obj): Logging Object which to descend from; raijin if unset
            ssm_key (str): parameter key to load ssh private key
        """

        self.username = username
        self.host = host
        self.ssh_client = None
        self.__private_key = None
        self._ssm_key = ssm_key

        if logger:
            self.logger = logger.getChild('raijin')
        else:
            self.logger = logging.getLogger('raijin')

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *exc):
        self.close()

    def connect(self):
        """Establishes ssh session; stores on class obj.

        Returns:
            (bool): True if a connection was established else False
        """

        if not self.__private_key:
            with StringIO() as f:
                f.write(get_ssm_parameter(self._ssm_key))
                f.seek(0)
                self.__private_key = paramiko.rsakey.RSAKey.from_private_key(f)

        if not self.ssh_client:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(_IgnorePolicy)

            ssh_client.connect(self.host, username=self.username, pkey=self.__private_key, look_for_keys=False)
            self.ssh_client = ssh_client

            return True

        return False

    def close(self):
        """Closes ssh session if it exists.
        Returns:
            (bool): True if connection was closed else False
        """
        if self.__private_key:
            self.__private_key = None

        if self.ssh_client:
            self.ssh_client.close()
            self.ssh_client = None
            return True

        return False

    def exec_command(self, command):
        """Runs commands in the raijin ssh session; provides rudimentary logging

        stdout logged to debug
        stderr logged to error if exit code is non zero use warn

        Args:
            command (str): text to be "executed" in raijin environment.

        Returns:
            (str): Output of command to stdout
            (str): Output of command stderr
            (int): Exit code of the command

        """
        if not self.ssh_client:
            self.connect()
        stdin, stdout, stderr, exit_code = None, None, None, None
        try:

            stdin, stdout, stderr = self.ssh_client.exec_command(command)

            exit_code = stdout.channel.recv_exit_status()
            script_output = stdout.read().decode('ascii')
            script_error = stderr.read().decode('ascii')
        except Exception as e:
            self.logger.error('%s', e.message)
            return None, None, exit_code
        finally:
            if stdin:
                stdin.close()
                stdout.close()
                stderr.close()

        if exit_code != 0:
            self.logger.error('%s: EXIT_CODE: %s', command, str(exit_code))
            if stderr:
                self.logger.error('%s: %s', command, script_error)
        else:
            if stderr:
                # command exits successfully; treat messages as warnings
                self.logger.warning('%s: %s', command, script_error)

        if stdout:
            self.logger.debug('%s: %s', command, script_output)

        return script_output, script_error, exit_code
