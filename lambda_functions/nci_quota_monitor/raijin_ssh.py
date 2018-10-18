import logging
import os
from io import StringIO

import paramiko

from .utils import get_ssm_parameter

DEFAULT_SSM_USER_PATH = os.environ['']

logger = logging.getLogger(__name__)


class _IgnorePolicy(paramiko.MissingHostKeyPolicy):
    """Suppress error and warning for missing host key."""

    def missing_host_key(self, client, hostname, key):
        pass


def ssh_config_from_ssm_user_path(path=DEFAULT_SSM_USER_PATH):
    return {'user': get_ssm_parameter(path + '.user'),
            'host': get_ssm_parameter(path + '.host'),
            'pkey': get_ssm_parameter(path + '.pkey')}


def connect():
    ssh_config = ssh_config_from_ssm_user_path()

    with StringIO() as f:
        f.write(ssh_config['pkey'])
        f.seek(0)
        private_key = paramiko.rsakey.RSAKey.from_private_key(f)

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(_IgnorePolicy)  # Host will change with lambda
    ssh_client.connect(
        ssh_config['host'],
        username=ssh_config['user'],
        pkey=private_key,
        look_for_keys=False
    )

    return ssh_client


def exec_command(command):
    """Runs commands in the raijin ssh session

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
    ssh_client = connect()
    stdin, stdout, stderr, exit_code = None, None, None, None
    try:

        stdin, stdout, stderr = ssh_client.exec_command(command)

        exit_code = stdout.channel.recv_exit_status()
        script_output = stdout.read().decode('ascii')
        script_error = stderr.read().decode('ascii')
    except Exception as e:
        logger.error(str(e))
        raise e
    finally:
        if stdin:
            stdin.close()
            stdout.close()
            stderr.close()

    if exit_code != 0:
        logger.error('%s: EXIT_CODE: %s', command, exit_code)
        if script_error:
            logger.error('%s: %s', command, script_error)
    else:
        if script_error:
            # command exited successfully; treat messages as warnings
            logger.warning('%s: %s', command, script_error)

    if script_output:
        logger.debug('%s: %s', command, script_output)

    return script_output, script_error, exit_code
