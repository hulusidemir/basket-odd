import unittest

from match_state import current_pace_projection, required_pace_comparison


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

    def test_required_pace_compares_both_directions_and_level_tempo(self):
        for line, required, status in ((160, 2.5, "above"), (176, 4.5, "below"), (168, 3.5, "level")):
            with self.subTest(line=line):
                result = required_pace_comparison(line, 140, 40, 48)
                self.assertEqual(result["remaining_minutes"], 8)
                self.assertEqual(result["remaining_points"], line - 140)
                self.assertEqual(result["required_ppm"], required)
                self.assertEqual(result["current_ppm"], 3.5)
                self.assertEqual(result["status"], status)

    def test_required_pace_handles_half_lines_and_seconds(self):
        result = current_pace_projection(
            "60 - 60", "Q4 05:30", "A - B", "EuroLeague", live_total=150.5,
        )["comparison"]
        self.assertEqual(result["remaining_minutes"], 5.5)
        self.assertEqual(result["remaining_points"], 30.5)
        self.assertEqual(result["required_ppm"], 5.55)
        self.assertEqual(result["current_ppm"], 3.48)
        self.assertEqual(result["status"], "below")

    def test_required_percentage_uses_current_pace_as_baseline(self):
        # 60 points in 20 minutes = 3 PPM. The remaining 20 minutes need
        # 3.6, 2.4 or 3 PPM respectively.
        for line, expected in ((132, 20), (108, -20), (120, 0)):
            with self.subTest(line=line):
                self.assertEqual(required_pace_comparison(
                    line, 60, 20, 40,
                )["required_change_pct"], expected)
        self.assertIsNone(required_pace_comparison(120, 0, 5, 40)["required_change_pct"])
        self.assertIsNone(required_pace_comparison(120, 60, 40, 40)["required_change_pct"])

    def test_percentage_is_calculated_before_display_rounding(self):
        result = required_pace_comparison(128.5, 27, 10 + 19 / 60, 40)
        expected = round(((101.5 / (40 - 10 - 19 / 60)) / (27 / (10 + 19 / 60)) - 1) * 100, 1)
        self.assertEqual(result["required_change_pct"], expected)

    def test_required_pace_handles_missing_data_start_and_end(self):
        for line, score, elapsed, duration in (
            (None, 100, 20, 40), (float("nan"), 100, 20, 40),
            (True, 100, 20, 40), (160, None, 20, 40),
            (160, 0, 0, 40), (160, 100, 41, 40),
        ):
            with self.subTest(values=(line, score, elapsed, duration)):
                result = required_pace_comparison(line, score, elapsed, duration)
                self.assertEqual(result["status"], "unavailable")
                self.assertIsNone(result["required_ppm"])
        ended = required_pace_comparison(160, 150, 40, 40)
        self.assertEqual(ended["status"], "ended")
        self.assertIsNone(ended["required_ppm"])
        for status in ("OT", "Q2", "Full Time"):
            self.assertEqual(current_pace_projection(
                "60 - 60", status, "A - B", "League", live_total=160,
            )["comparison"]["status"], "unavailable")

    def test_reached_and_exceeded_lines_never_need_negative_points(self):
        for score, status in ((160, "reached"), (161, "exceeded")):
            result = required_pace_comparison(160, score, 35, 40)
            self.assertEqual(result["status"], status)
            self.assertEqual(result["remaining_points"], 0)
            self.assertEqual(result["required_ppm"], 0)


if __name__ == "__main__":
    unittest.main()
