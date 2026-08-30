import tempfile
import unittest
from pathlib import Path

from db import Database


class MatchActionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp_dir.name) / "actions.db"))
        self.db.init()

    def tearDown(self):
        self.temp_dir.cleanup()

    def _save_signal(self, number: int) -> int:
        return self.db.save_alert(
            "same-match",
            "Home - Away",
            160,
            170 + number,
            "ALT",
            10 + number,
            signal_count=number,
        )

    def test_match_action_updates_existing_signals_and_is_inherited_by_new_signal(self):
        self._save_signal(1)
        self._save_signal(2)

        self.assertEqual(
            self.db.set_match_statuses(
                "same-match", bet_placed=False, ignored=False, followed=True
            ),
            2,
        )
        self._save_signal(3)

        rows = self.db.active_alerts_for_match("same-match")
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(row["followed"] == 1 for row in rows))
        self.assertTrue(all(row["bet_placed"] == 0 for row in rows))
        self.assertTrue(all(row["ignored"] == 0 for row in rows))


if __name__ == "__main__":
    unittest.main()
