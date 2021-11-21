import unittest
from unittest.mock import MagicMock

import main


class TestMainRedis(unittest.TestCase):

    def test_GetMaster(self):
        # expect
        expect = "10.10.10.121"

        main.getMaster = MagicMock()
        main.getMaster.return_value = expect
        result = main.getMaster()

        self.assertEqual(result, expect)
