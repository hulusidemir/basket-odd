import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from db import Database
from main import _ConsecutiveFailureAlertLatch, _normalize_match_payload, process_match


class _Config:
    THRESHOLD = 10
    MAX_SIGNALS_PER_MATCH = 4
    SAME_DIRECTION_MIN_LIVE_DELTA = 3
    BLACKLIST = []


class RawSignalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.tmp.name) / "test.db"))
        self.db.init()
        self.notifier = type("Notifier", (), {})()
        self.notifier.send_alert = AsyncMock(return_value={"chat": 1})

    def tearDown(self):
        self.tmp.cleanup()

    def test_upward_line_change_creates_alt_signal(self):
        asyncio.run(process_match({
            "match_id": "m1",
            "match_name": "Home - Away",
            "tournament": "League",
            "status": "Q2 05:00",
            "score": "40 - 35",
            "opening_total": 160,
            "inplay_total": 171,
            "url": "https://m.aiscore.com/basketball/match-x/m1",
        }, self.db, self.notifier, _Config()))

        row = self.db.get_alert(1)
        self.assertEqual(row["direction"], "ALT")
        self.assertEqual(row["diff"], 11)
        self.notifier.send_alert.assert_awaited_once()

    def test_downward_line_change_creates_ust_signal(self):
        asyncio.run(process_match({
            "match_id": "m2",
            "match_name": "Home - Away",
            "status": "Q3 04:00",
            "score": "65 - 60",
            "opening_total": 175,
            "inplay_total": 164,
        }, self.db, self.notifier, _Config()))
        self.assertEqual(self.db.get_alert(1)["direction"], "ÜST")

    def test_below_threshold_does_not_create_signal(self):
        asyncio.run(process_match({
            "match_id": "m3",
            "match_name": "Home - Away",
            "status": "Q1 08:00",
            "score": "5 - 4",
            "opening_total": 160,
            "inplay_total": 169.5,
        }, self.db, self.notifier, _Config()))
        self.assertIsNone(self.db.get_alert(1))
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
