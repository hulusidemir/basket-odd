import sqlite3
import tempfile
import unittest
from pathlib import Path

from db import Database


class DatabaseSchemaTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "clean.db")
        self.db = Database(self.db_path)
        self.db.init()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_schema_contains_only_current_tables_and_columns(self):
        with sqlite3.connect(self.db_path) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            alert_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(alerts)")
            }
            upcoming_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(upcoming_matches)")
            }

        self.assertEqual(
            tables,
            {
                "alerts",
                "bankroll_sessions",
                "match_actions",
                "saved_match_lists",
                "signal_lists",
                "upcoming_match_actions",
                "upcoming_matches",
            },
        )
        self.assertNotIn("ai_analysis", alert_columns)
        self.assertNotIn("expected_total", upcoming_columns)
        self.assertNotIn("direction", upcoming_columns)
        self.assertNotIn("diff", upcoming_columns)

    def test_active_period_is_unique(self):
        self.db.save_alert(
            "match-1",
            "Home - Away",
            160,
            170,
            "ALT",
            10,
            alert_period=2,
        )

        with self.assertRaises(RuntimeError):
            self.db.save_alert(
                "match-1",
                "Home - Away",
                160,
                171,
                "ALT",
                11,
                signal_count=2,
                alert_period=2,
            )


if __name__ == "__main__":
    unittest.main()
