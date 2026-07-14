import sqlite3
import tempfile
import unittest
from pathlib import Path

from db import Database


class DatabaseMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "legacy.db")
        self.db = Database(self.db_path)
        self.db.init()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_legacy_active_period_duplicates_do_not_break_startup_or_get_deleted(self):
        alert_id = self.db.save_alert(
            "legacy-match",
            "Home - Away",
            160,
            170,
            "ALT",
            10,
            status="Q2 05:00",
            score="40 - 35",
            alert_period=2,
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DROP INDEX idx_alerts_active_match_period")
            conn.execute(
                """
                INSERT INTO alerts (
                    match_id, match_name, opening, live, direction, diff,
                    status, score, alert_period, signal_count
                )
                SELECT match_id, match_name, opening, live, direction, diff,
                       status, score, alert_period, 2
                FROM alerts
                WHERE id = ?
                """,
                (alert_id,),
            )

        with self.assertLogs("db", level="WARNING") as captured:
            self.db.init()

        self.assertIn("unique index postponed", " ".join(captured.output))
        with sqlite3.connect(self.db_path) as conn:
            count = conn.execute(
                """
                SELECT COUNT(*)
                FROM alerts
                WHERE match_id = 'legacy-match' AND alert_period = 2
                """
            ).fetchone()[0]
            index_exists = conn.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'index' AND name = 'idx_alerts_active_match_period'
                """
            ).fetchone()

        self.assertEqual(count, 2)
        self.assertIsNone(index_exists)


if __name__ == "__main__":
    unittest.main()
