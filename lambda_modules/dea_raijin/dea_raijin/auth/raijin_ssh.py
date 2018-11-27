import boto3
import paramiko
import logging
import json
from io import StringIO

from .ssm_key_retrieve import get_ssm_parameter

from .. import config


class _IgnorePolicy(paramiko.MissingHostKeyPolicy):
    """Suppress error and warning for missing host key."""

    def missing_host_key(self, client, hostname, key):
        pass


class SSHConfig():
    """Class to hold ssh credentials for raijin sessions

    Attributes:
        user (str): user account to ssh as
        host (str): host account to ssh into
        private_key (str): private_key used to ssh

    """

    def __init__(self, user=None, host=None, private_key=None):
        """ Object initialised from parameters"""
        self.host = host
        self.user = user
        self.private_key = private_key

    def from_ssm_user_path(self, path=config.DEFAULT_SSM_USER_PATH, user_suffix=".user", host_suffix=".host",
                           pkey_suffix=".pkey"):
        """
        Args:
            path (str): ssm key prefix that hosts ssh user information. default (from dea_raijin.config)
                        orchestrator.raijin.users.default
            user_suffix (str): suffix on ssm path to find user for ssh. default ".user"
            host_suffix (str): suffix on ssm path to find host for ssh. default ".host"
            pkey_suffix (str): suffix on ssm path to find private key for ssh. default ".pkey"

        Returns:
            self
        """

        self.user = get_ssm_parameter(path + user_suffix)
        self.host = get_ssm_parameter(path + host_suffix)
        self.private_key = get_ssm_parameter(path + pkey_suffix)

        return self

    def from_json_obj(self, json_obj):
        """
        Args:
            json_obj (str): JSON decodable string expanding to a dictionary with host, user and privateKey keys

        Returns:
            self
        """

        obj = json.loads(json_obj)

        self.host = obj['host']
        self.user = obj['user']
        self.private_key = obj['privateKey']

        return self

    def from_ssm_json_obj(self, ssm_key):
        """
        Args:
            ssm_key (str): ssm key to fetch json object from. See self.from_json_obj for details

        Returns:
            self
        """
        json_obj = get_ssm_parameter(ssm_key)

        return self.from_json_obj(json_obj)


class RaijinSession():
    """Establishes an ssh connection with raijin login node.

    Attributes:
        ssh_config (obj): Config object holding user, host, private_key information
        logger (obj): Logging object for raijin commands
    """

    def __init__(self, ssh_config=None, logger=None):
        """Constructs an ssh session to raijin.
        Args:
            ssh_config: raijin_ssh.SSHConfig object that holds user, host, private_key parameters
            logger (obj): Logging Object which to descend from; raijin if unset
        """

        self.ssh_config = ssh_config or SSHConfig().from_ssm_user_path()
        self.ssh_client = None
        self.__private_key = None

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

        if not self.ssh_client:
            with StringIO() as f:
                f.write(self.ssh_config.private_key)
                f.seek(0)
                self.__private_key = paramiko.rsakey.RSAKey.from_private_key(f)

            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(_IgnorePolicy)  # Host will change with lambda
            ssh_client.connect(
                self.ssh_config.host, username=self.ssh_config.user, pkey=self.__private_key, look_for_keys=False
            )

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
        # pylint: disable=broad-except
        if not self.ssh_client:
            self.connect()
        stdin, stdout, stderr, exit_code = None, None, None, None
        try:

            stdin, stdout, stderr = self.ssh_client.exec_command(command)

            exit_code = stdout.channel.recv_exit_status()
            script_output = stdout.read().decode('ascii')
            script_error = stderr.read().decode('ascii')
        except Exception as e:
            self.logger.error(str(e))
            raise e
        finally:
            if stdin:
                stdin.close()
                stdout.close()
                stderr.close()

        if exit_code != 0:
            self.logger.error('%s: EXIT_CODE: %s', command, exit_code)
            if script_error:
                self.logger.error('%s: %s', command, script_error)
        else:
            if script_error:
                # command exited successfully; treat messages as warnings
                self.logger.warning('%s: %s', command, script_error)

        if script_output:
            self.logger.debug('%s: %s', command, script_output)

        return script_output, script_error, exit_code
