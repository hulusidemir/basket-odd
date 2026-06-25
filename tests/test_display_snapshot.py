import tempfile
import unittest
from pathlib import Path

from db import Database


class DisplaySnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp_dir.name) / "test.db"))
        self.db.init()
        self.alert_id = self.db.save_alert(
            "match-1", "Home - Away", 160, 172, "ALT", 12,
            score="75 - 70", status="Q3 04:00",
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_profiles_are_only_updated_while_active(self):
        self.assertTrue(self.db.update_active_alert_profiles(
            self.alert_id,
            hundred_profile=True,
            hundred_profile_rule="rule-1",
            claude_ai="TRUE_UNDER",
            claude_ai_rule="B1",
        ))
        self.db.delete_match_data("match-1")
        self.assertFalse(self.db.update_active_alert_profiles(
            self.alert_id,
            hundred_profile=False,
            claude_ai="",
        ))
        row = self.db.get_deleted_alert_by_id(self.alert_id)
        self.assertEqual(row["hundred_profile"], 1)
        self.assertEqual(row["claude_ai"], "TRUE_UNDER")

    def test_display_snapshot_survives_deletion_unchanged(self):
        snapshot = {
            "id": self.alert_id,
            "direction": "ALT",
            "fair_line": 166.5,
            "hundred_profile": 1,
            "claude_ai": "TRUE_UNDER",
        }
        self.assertEqual(self.db.save_active_alert_display_snapshots({self.alert_id: snapshot}), 1)
        self.db.delete_match_data("match-1")
        row = self.db.get_deleted_alert_by_id(self.alert_id)
        self.assertIn('"fair_line": 166.5', row["display_snapshot"])
        self.assertIn('"claude_ai": "TRUE_UNDER"', row["display_snapshot"])


if __name__ == "__main__":
    unittest.main()
