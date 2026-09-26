import unittest

from match_state import (
    confirmed_12_minute_quarters, current_pace_projection, game_clock,
    quarter_clock_seconds, required_pace_comparison,
)


class CurrentPaceProjectionTests(unittest.TestCase):
    def test_fiba_format_comes_from_tournament_not_team_name(self):
        fiba = game_clock("Q2 05:00", "NBA G League United - Club", "FIBA Intercontinental Cup")
        self.assertEqual((fiba["quarter_length"], fiba["period_count"]), (10, 4))
        college_name = game_clock("Q2 05:00", "NCAA Alumni - Club", "FIBA Intercontinental Cup")
        self.assertEqual((college_name["quarter_length"], college_name["period_count"]), (10, 4))

    def test_nba_league_keeps_48_minute_format(self):
        nba = game_clock("Q2 05:00", "Club - Club", "NBA")
        self.assertEqual((nba["quarter_length"], nba["period_count"]), (12, 4))

    def test_pba_uses_four_twelve_minute_quarters_from_tournament_only(self):
        for tournament in (
            "Philippine Basketball Association",
            "PBA",
            "Philippines PBA",
            "Philippines Philippine Basketball Association",
            "  philippines   pba  ",
        ):
            with self.subTest(tournament=tournament):
                clock = game_clock("Q2 12:00", "Club - Club", tournament)
                self.assertEqual(
                    (clock["period"], clock["quarter_length"], clock["period_count"]),
                    (2, 12, 4),
                )
                projection = current_pace_projection(
                    "30 - 30", "Q2 12:00", "Club - Club", tournament,
                )
                self.assertEqual((projection["elapsed_minutes"], projection["game_minutes"]), (12, 48))

    def test_other_philippines_leagues_keep_existing_formats(self):
        for tournament, expected in (
            ("Philippines MPBL", (10, 4)),
            ("Philippines University Athletic Association", (10, 4)),
            ("Philippines UAAP", (10, 4)),
            ("Philippines National Collegiate Athletic Association", (10, 4)),
            ("Philippines NCAA", (20, 2)),
            ("Philippines Regional Basketball League", (10, 4)),
            ("Philippines PBA Developmental League", (10, 4)),
        ):
            with self.subTest(tournament=tournament):
                clock = game_clock("Q2 05:00", "PBA Club - NBA Club", tournament)
                self.assertEqual((clock["quarter_length"], clock["period_count"]), expected)

    def test_nba_summer_league_keeps_forty_minute_format(self):
        clock = game_clock("Q2 05:00", "Club - Club", "NBA Summer League")
        self.assertEqual((clock["quarter_length"], clock["period_count"]), (10, 4))

    def test_database_tournament_names_keep_their_period_formats(self):
        # Exact tournament values observed in alerts; do not infer from team or country.
        for tournament in (
            "Philippines MPBL",
            "Philippines University Athletic Association",
            "Philippines National Collegiate Athletic Association",
            "FIBA Intercontinental Cup",
            "Women's National Basketball Association",
            "National Basketball League",
            "Club Friendship",
        ):
            with self.subTest(tournament=tournament):
                clock = game_clock("Q2 05:00", "PBA Club - NBA Club", tournament)
                self.assertEqual((clock["quarter_length"], clock["period_count"]), (10, 4))

    def test_runtime_twelve_minute_observation_requires_valid_quarter_clock(self):
        for tournament, status, expected in (
            ("Unknown League", "Q2 11:34", (12, 4)),
            ("Unknown League", "Q2 08:45", (10, 4)),
            ("FIBA Intercontinental Cup", "Q2 09:00", (10, 4)),
            ("FIBA Intercontinental Cup", "Q3 10:45", (12, 4)),
            ("NBA", "Q2 08:45", (12, 4)),
            ("NBA Summer League", "Q2 08:45", (10, 4)),
            ("Philippine Basketball Association", "Q2 08:45", (12, 4)),
            ("NCAA", "2H 11:34", (20, 2)),
            ("NCAA", "Q2 11:34", (20, 2)),
            ("Unknown League", "Q2 12:01", (10, 4)),
            ("Unknown League", "Q2 11:60", (10, 4)),
        ):
            with self.subTest(tournament=tournament, status=status):
                clock = game_clock(status, "NBA Club - PBA Club", tournament)
                self.assertEqual((clock["quarter_length"], clock["period_count"]), expected)
        self.assertEqual(quarter_clock_seconds("Q3 10:45"), 645)
        self.assertIsNone(quarter_clock_seconds("2H 11:34"))

    def test_runtime_duration_context_is_reset_after_match(self):
        with confirmed_12_minute_quarters(True):
            self.assertEqual(game_clock("Q3 08:00", tournament="Unknown League")["quarter_length"], 12)
        self.assertEqual(game_clock("Q3 08:00", tournament="Unknown League")["quarter_length"], 10)

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
