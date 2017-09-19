import unittest
from unittest.mock import patch

import dea_raijin
from dea_raijin.lambda_commands import BaseCommand, RaijinCommand


class BC(BaseCommand):
    '''Blank class to test default behaviour'''
    def __init__(self):
        super().__init__(self)


class RC(RaijinCommand):
    '''Blank class to test default behaviour'''
    def __init__(self):
        super().__init__(self)


class TestBaseCommand(unittest.TestCase):

    def test_logger(self):
        bc = BC()
        assert bc.logger is not None

    def test_command_error(self):
        bc = BC()
        with self.assertRaises(NotImplementedError):
            bc.run()


class TestRaijinCommand(unittest.TestCase):

    @patch('dea_raijin.lambda_commands.RaijinSession')
    def test_logger(self, raijin_session):
        rc = RC()
        assert rc.logger is not None

    @patch('dea_raijin.lambda_commands.RaijinSession')
    def test_command_error(self, raijin_session):
        rc = RC()
        with self.assertRaises(NotImplementedError):
            rc.run()
