from io import BytesIO

import unittest
from unittest.mock import patch, Mock

from dea_raijin.auth.raijin_ssh import RaijinSession
from dea_raijin import config


class TestRaijinSSH(unittest.TestCase):

    @patch('dea_raijin.auth.raijin_ssh.paramiko.rsakey.RSAKey')
    @patch('dea_raijin.auth.raijin_ssh.paramiko.SSHClient')
    @patch('dea_raijin.auth.raijin_ssh.get_ssm_parameter')
    def test_context_manager(self, ssm, ssh, rsa_key):
        rs = None
        ssm.return_value = 'Hello, World!'  # Dummy value

        with self.assertLogs('raijin', level='DEBUG') as cm:
            with RaijinSession() as rs:
                assert ssm.call_count == 3
                rs.ssh_client.connect.assert_called_once()
                assert rs.ssh_client is not None

                stdout_mock = Mock()
                stdout_mock.read.return_value = b'stdout'
                stdout_mock.channel.recv_exit_status.return_value = 0

                rs.ssh_client.exec_command.return_value = (BytesIO(b'stdin'), stdout_mock, BytesIO(b'stderr'))
                stdout, stderr, _ = rs.exec_command('test')
                assert stdout == 'stdout'
                assert stderr == 'stderr'

            assert cm.output[0] == 'WARNING:raijin:test: stderr'
            assert cm.output[1] == 'DEBUG:raijin:test: stdout'

        assert rs.ssh_client is None

    @patch('dea_raijin.auth.raijin_ssh.paramiko.rsakey.RSAKey')
    @patch('dea_raijin.auth.raijin_ssh.paramiko.SSHClient')
    @patch('dea_raijin.auth.raijin_ssh.get_ssm_parameter')
    def test_manual_management(self, ssm, ssh, rsa_key):
        rs = None
        ssm.return_value = 'Hello, World!'  # Dummy value

        rs = RaijinSession()
        assert rs.logger is not None

        connect_rv = rs.connect()
        assert connect_rv

        # assert reuses the same connection
        assert not rs.connect()

        assert ssm.call_count == 3
        rs.ssh_client.connect.assert_called_once()
        assert rs.ssh_client is not None

        stdout_mock = Mock()
        stdout_mock.read.return_value = b'stdout'
        stdout_mock.channel.recv_exit_status.return_value = 0

        with self.assertLogs('raijin', level='DEBUG') as cm:
            rs.ssh_client.exec_command.return_value = (BytesIO(b'stdin'), stdout_mock, BytesIO(b'stderr'))
            stdout, stderr, _ = rs.exec_command('test')
            assert stdout == 'stdout'
            assert stderr == 'stderr'

            close_rv = rs.close()
            assert close_rv

            # assert closing twice does nothing
            assert not rs.close()
            assert cm.output[0] == 'WARNING:raijin:test: stderr'
            assert cm.output[1] == 'DEBUG:raijin:test: stdout'
