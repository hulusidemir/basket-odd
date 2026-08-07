import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from db import Database


def upcoming_row(match_id: str) -> dict:
    return {
        "match_id": match_id,
        "match_name": f"Home {match_id} - Away {match_id}",
        "home_team": f"Home {match_id}",
        "away_team": f"Away {match_id}",
        "tournament": "Test League",
        "kickoff": f"{date.today().isoformat()} 20:00",
        "opening_total": 160.0,
        "prematch_total": 161.0,
        "expected_total": 166.0,
        "url": f"https://example.test/{match_id}",
    }


class UpcomingReconcileTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp_dir.name) / "test.db"))
        self.db.init()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_partial_detail_scrape_does_not_delete_other_current_rows(self):
        self.db.save_upcoming_matches_and_signals([
            upcoming_row("match-a"),
            upcoming_row("match-b"),
        ])
        self.db.set_upcoming_match_statuses("match-b", followed=True)

        summary = self.db.save_upcoming_matches_and_signals([
            upcoming_row("match-a"),
        ])

        self.assertFalse(summary["reconciled"])
        self.assertEqual(
            {row["match_id"] for row in self.db.list_upcoming_matches()},
            {"match-a", "match-b"},
        )
        self.assertTrue(self.db.get_upcoming_match_action_status("match-b")["followed"])

    def test_authoritative_listing_reconcile_preserves_user_action(self):
        self.db.save_upcoming_matches_and_signals([
            upcoming_row("match-a"),
            upcoming_row("match-b"),
        ])
        self.db.set_upcoming_match_statuses("match-b", followed=True)

        summary = self.db.save_upcoming_matches_and_signals(
            [upcoming_row("match-a")],
            seen_match_ids={"match-a"},
            reconcile=True,
        )

        self.assertTrue(summary["reconciled"])
        self.assertEqual(summary["removed_missing"], 1)
        self.assertEqual(
            [row["match_id"] for row in self.db.list_upcoming_matches()],
            ["match-a"],
        )
        self.assertTrue(self.db.get_upcoming_match_action_status("match-b")["followed"])

    def test_rows_outside_rolling_24_hours_are_removed(self):
        now = datetime.now(ZoneInfo("Europe/Istanbul"))
        inside = upcoming_row("inside")
        outside = upcoming_row("outside")
        inside["kickoff"] = (now + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M")
        outside["kickoff"] = (now + timedelta(hours=25)).strftime("%Y-%m-%d %H:%M")

        summary = self.db.save_upcoming_matches_and_signals([inside, outside])

        self.assertEqual(summary["removed_expired"], 1)
        self.assertEqual(
            [row["match_id"] for row in self.db.list_upcoming_matches()],
            ["inside"],
        )


if __name__ == "__main__":
    unittest.main()
