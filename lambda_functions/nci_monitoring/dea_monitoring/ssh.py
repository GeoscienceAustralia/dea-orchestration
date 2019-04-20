import logging
import os
from io import StringIO

import paramiko

from .utils import get_ssm_parameter

LOG = logging.getLogger(__name__)

DEFAULT_SSM_USER_PATH = os.environ['SSM_USER_PATH']


def exec_command(command):
    """
    Connect and run a command on a remote host

    stdout logged to debug
    stderr logged to error if exit code is non zero use warn

    Args:
        command (str): command to be "executed" on the ssh host

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
        LOG.error(str(e))
        raise e
    finally:
        if stdin:
            stdin.close()
            stdout.close()
            stderr.close()

    if exit_code != 0:
        LOG.error('COMMAND: %s, EXIT_CODE: %s', command, exit_code)
        if script_error:
            LOG.error('%s: %s', command, script_error)
    else:
        if script_error:
            # command exited successfully; treat messages as warnings
            LOG.warning('COMMAND: %s, STD_ERR: %s', command, script_error)

    if script_output:
        LOG.debug('COMMAND: %s, OUTPUT: %s', command, script_output)

    return script_output, script_error, exit_code


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

    _sshclient = paramiko.SSHClient()
    _sshclient.set_missing_host_key_policy(_IgnorePolicy)  # Host will change with lambda
    _sshclient.connect(
        ssh_config['host'],
        username=ssh_config['user'],
        pkey=private_key,
        look_for_keys=False
    )

    return _sshclient
