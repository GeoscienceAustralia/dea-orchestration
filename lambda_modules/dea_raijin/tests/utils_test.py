import unittest

from dea_raijin.utils import timestr_to_seconds, human2bytes


class TestTimeToSeconds(unittest.TestCase):
    HOURS = 5
    MINUTES = 10
    SECONDS = 20

    def test_2_part_timestr(self):
        assert timestr_to_seconds("{}:{}".format(self.HOURS, self.MINUTES)) == self.HOURS * (60**2) + self.MINUTES * 60

    def test_3_part_timestr(self):
        time_str = "{}:{}:{}".format(self.HOURS, self.MINUTES, self.SECONDS)
        assert timestr_to_seconds(time_str) == self.HOURS * (60**2) + self.MINUTES * 60 + self.SECONDS

    def test_1_part_timestr(self):
        with self.assertRaises(NotImplementedError):
            timestr_to_seconds("{}".format(self.HOURS))

    def test_none(self):
        assert timestr_to_seconds(None) is None


class TestHuman2Bytes(unittest.TestCase):

    def test_illegible(self):
        with self.assertRaises(ValueError):
            human2bytes('hello, world!')

    def test_smoke_k(self):
        assert human2bytes('1k') == 2 ** 10
