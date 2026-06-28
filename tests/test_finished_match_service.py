import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from db import Database
from finished_match_service import run_deleted_match_result_cycle


class FinishedMatchServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp_dir.name) / "test.db"))
        self.db.init()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_deleted_result_cycle_settles_from_stored_final_score(self):
        alert_id = self.db.save_alert(
            "match-1",
            "Home - Away",
            160,
            170,
            "ALT",
            10,
            status="Full Time",
            score="70 - 80",
            url="https://example.test/match-1",
        )
        self.db.delete_match_data("match-1")

        summary = asyncio.run(run_deleted_match_result_cycle(self.db, _Config()))

        self.assertEqual(summary["tracked_count"], 1)
        self.assertEqual(summary["checked_count"], 1)
        self.assertEqual(summary["finished_match_count"], 1)
        self.assertEqual(summary["updated_count"], 1)
        row = self.db.get_deleted_alert_by_id(alert_id)
        self.assertEqual(row["result"], "Başarılı")
        self.assertEqual(row["status"], "Full Time")
        self.assertEqual(row["score"], "70 - 80")

    def test_deleted_result_cycle_does_not_settle_non_final_stored_score(self):
        alert_id = self.db.save_alert(
            "match-1",
            "Home - Away",
            160,
            170,
            "ALT",
            10,
            status="Q3 04:00",
            score="70 - 80",
            url="https://example.test/match-1",
        )
        self.db.delete_match_data("match-1")

        with patch(
            "finished_match_service.AiscoreFinishedMatchChecker.check_matches",
            new=AsyncMock(return_value=[]),
        ):
            summary = asyncio.run(run_deleted_match_result_cycle(self.db, _Config()))

        self.assertEqual(summary["tracked_count"], 1)
        self.assertEqual(summary["checked_count"], 0)
        self.assertEqual(summary["updated_count"], 0)
        self.assertIn("ulaşılamadı", summary["message"])
        row = self.db.get_deleted_alert_by_id(alert_id)
        self.assertEqual(row["result"], "")


class _Config:
    PAGE_TIMEOUT_MS = 100


if __name__ == "__main__":
    unittest.main()
