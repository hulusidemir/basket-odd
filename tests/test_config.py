import os
import unittest
from unittest.mock import patch

import config


class ConfigTests(unittest.TestCase):
    def tearDown(self):
        config._CONFIG_ERRORS.clear()

    def test_invalid_number_is_reported_by_validate_instead_of_import_crash(self):
        config._CONFIG_ERRORS.clear()
        with patch.dict(os.environ, {"BROKEN_INTEGER": "not-a-number"}):
            value = config._int_env("BROKEN_INTEGER", 7)

        self.assertEqual(value, 7)
        instance = config.Config()
        instance.TELEGRAM_TOKEN = "valid-token"
        instance.TELEGRAM_CHAT_ID = "1"
        with self.assertRaisesRegex(ValueError, "BROKEN_INTEGER"):
            instance.validate()

    def test_runtime_limits_are_validated(self):
        instance = config.Config()
        instance.TELEGRAM_TOKEN = "valid-token"
        instance.TELEGRAM_CHAT_ID = "1"
        instance.PAGE_TIMEOUT_MS = 1000

        with self.assertRaisesRegex(ValueError, "PAGE_TIMEOUT_MS"):
            instance.validate()


if __name__ == "__main__":
    unittest.main()
