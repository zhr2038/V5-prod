from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from main import _get_env_epoch_sec


class TestEnvEpochSec(unittest.TestCase):
    def test_seconds(self):
        with patch.dict(os.environ, {"TEST_TS": "1700000000"}, clear=False):
            self.assertEqual(_get_env_epoch_sec("TEST_TS"), 1700000000)

    def test_milliseconds(self):
        with patch.dict(os.environ, {"TEST_TS": "1700000000000"}, clear=False):
            self.assertEqual(_get_env_epoch_sec("TEST_TS"), 1700000000)

    def test_microseconds(self):
        with patch.dict(os.environ, {"TEST_TS": "1700000000000000"}, clear=False):
            self.assertEqual(_get_env_epoch_sec("TEST_TS"), 1700000000)

    def test_2033_ms_boundary(self):
        # 2033+ 的 13 位毫秒时间戳仍应按毫秒解析
        with patch.dict(os.environ, {"TEST_TS": "2000000000000"}, clear=False):
            self.assertEqual(_get_env_epoch_sec("TEST_TS"), 2000000000)

    def test_invalid(self):
        with patch.dict(os.environ, {"TEST_TS": "not-a-number"}, clear=False):
            self.assertIsNone(_get_env_epoch_sec("TEST_TS"))


if __name__ == "__main__":
    unittest.main()
