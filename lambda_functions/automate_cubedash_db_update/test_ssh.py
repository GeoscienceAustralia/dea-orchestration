import mock
import paramiko
import unittest
from ssh import _connect, _ssh_config_from_ssm_user_path, _get_ssm_parameter


class TestSsh(unittest.TestCase):

    @mock.patch('ssh.boto3.client')
    def test_get_ssm_parameter(self, ssm_client):
        ssm_client.get_parameter.Parameter.Value = "test"
        name = "Temp"
        ret_val = _get_ssm_parameter(name)
        ssm_client.assert_called_once()
        self.assertEqual(ret_val.Parameter.Value,
                         ssm_client().get_parameter().__getitem__().__getitem__().Parameter.Value)

    @mock.patch('ssh._get_ssm_parameter')
    def test_ssh_config_from_ssm_user_path(self, mock_get_ssm):
        mock_get_ssm.return_value = "nci_orchestration.test"
        ret_val = _ssh_config_from_ssm_user_path(path='nci_orchestration.test')
        assert mock_get_ssm.call_count == 3
        expected_value = {'user': 'nci_orchestration.test',
                          'host': 'nci_orchestration.test',
                          'pkey': 'nci_orchestration.test'}
        self.assertEqual(ret_val, expected_value)

    @mock.patch('ssh._ssh_config_from_ssm_user_path',
                return_value={'user': 'nci_orchestration.user',
                              'host': 'nci_orchestration.host',
                              'pkey': 'nci_orchestration.pkey'})
    @mock.patch('ssh.paramiko.SSHClient')
    def test_connect(self, mock_get_ssm, mock_ssh_client):
        client_tmp = paramiko.SSHClient

        def client_mock():
            client = client_tmp()
            client.connect = mock.Mock(name='connect', )
            return client

        paramiko.SSHClient = client_mock
        paramiko.rsakey.RSAKey.from_private_key = mock.Mock()
        paramiko.SSHClient.set_missing_host_key_policy = mock.Mock()
        _connect()
        mock_get_ssm.assert_called_once()
        mock_ssh_client.assert_called_once()
