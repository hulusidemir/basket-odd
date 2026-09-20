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

    def test_live_scrape_timeout_must_exceed_match_timeout(self):
        instance = config.Config()
        instance.TELEGRAM_TOKEN = "valid-token"
        instance.TELEGRAM_CHAT_ID = "1"
        instance.LIVE_MATCH_TIMEOUT_SECONDS = 90
        instance.LIVE_SCRAPE_TIMEOUT_SECONDS = 90

        with self.assertRaisesRegex(ValueError, "LIVE_SCRAPE_TIMEOUT_SECONDS"):
            instance.validate()

    def test_boolean_environment_parses_false_and_reports_invalid_value(self):
        for value in ("false", "0", "no", "OFF"):
            with patch.dict(os.environ, {"TEST_FLAG": value}):
                self.assertFalse(config._bool_env("TEST_FLAG", True))
        with patch.dict(os.environ, {"TEST_FLAG": "maybe"}):
            self.assertTrue(config._bool_env("TEST_FLAG", True))
        self.assertTrue(any("TEST_FLAG" in error for error in config._CONFIG_ERRORS))

    def test_threshold_settings_reject_non_finite_and_invalid_values(self):
        for name, value in (
            ("THRESHOLD_MODE", "invalid"), ("THRESHOLD_PERCENT", 0),
            ("THRESHOLD_PERCENT", float("nan")), ("THRESHOLD", float("inf")),
            ("Q1_THRESHOLD_MULTIPLIER", -1), ("Q2_THRESHOLD_MULTIPLIER", 0),
            ("Q2_ALT_THRESHOLD_MULTIPLIER", float("inf")),
        ):
            with self.subTest(name=name, value=value):
                instance = config.Config()
                instance.TELEGRAM_TOKEN = "valid-token"
                instance.TELEGRAM_CHAT_ID = "1"
                setattr(instance, name, value)
                with self.assertRaisesRegex(ValueError, name):
                    instance.validate()


if __name__ == "__main__":
    unittest.main()
