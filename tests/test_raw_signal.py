import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from config import Config
from db import Database
from main import _ConsecutiveFailureAlertLatch, _normalize_match_payload, process_match

class RawSignalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.tmp.name) / "test.db"))
        self.db.init()
        self.notifier = type("Notifier", (), {})()
        self.notifier.send_alert = AsyncMock(return_value={"chat": 1})

    def tearDown(self):
        self.tmp.cleanup()

    def test_without_history_returns_pas_and_no_alert(self):
        asyncio.run(process_match({
            "match_id": "m1",
            "match_name": "Home - Away",
            "tournament": "League",
            "status": "Q2 05:00",
            "score": "40 - 35",
            "opening_total": 160,
            "inplay_total": 171,
            "url": "https://m.aiscore.com/basketball/match-x/m1",
        }, self.db, self.notifier, Config()))

        row = self.db.get_alert(1)
        self.assertIsNone(row)
        self.notifier.send_alert.assert_not_awaited()

    def test_payload_rejects_non_finite_total(self):
        with self.assertRaises(ValueError):
            _normalize_match_payload({
                "match_id": "m4", "match_name": "A - B",
                "opening_total": float("nan"), "inplay_total": 170,
            })

    def test_failure_latch_rearms_after_success(self):
        latch = _ConsecutiveFailureAlertLatch(threshold=2)
        self.assertEqual(latch.record_failure(), (1, False))
        self.assertEqual(latch.record_failure(), (2, True))
        self.assertEqual(latch.record_failure(), (3, False))
        self.assertEqual(latch.record_success(), 3)
        self.assertEqual(latch.record_failure(), (1, False))

if __name__ == "__main__":
    unittest.main()
