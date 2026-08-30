import unittest

from match_state import current_pace_projection


class CurrentPaceProjectionTests(unittest.TestCase):
    def test_projects_fiba_total_from_elapsed_game_time(self):
        projection = current_pace_projection(
            "40 - 35", "Q2 05:00", "Home - Away", "EuroLeague"
        )

        self.assertEqual(projection["score_total"], 75)
        self.assertEqual(projection["elapsed_minutes"], 15)
        self.assertEqual(projection["game_minutes"], 40)
        self.assertEqual(projection["total"], 200.0)

    def test_uses_48_minutes_for_nba(self):
        projection = current_pace_projection(
            "48 - 42", "Q2 06:00", "Home - Away", "NBA"
        )

        self.assertEqual(projection["elapsed_minutes"], 18)
        self.assertEqual(projection["game_minutes"], 48)
        self.assertEqual(projection["total"], 240.0)

    def test_final_regulation_projection_equals_final_total(self):
        projection = current_pace_projection(
            "90 - 80", "Q4-Ended", "Home - Away", "League"
        )

        self.assertEqual(projection["total"], 170.0)

    def test_does_not_project_overtime_or_missing_clock(self):
        self.assertIsNone(current_pace_projection("90 - 90", "OT", "A - B", "League")["total"])
        self.assertIsNone(current_pace_projection("40 - 35", "Q2", "A - B", "League")["total"])


if __name__ == "__main__":
    unittest.main()
