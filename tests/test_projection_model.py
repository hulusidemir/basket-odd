import unittest

from projection import calculate_live_projection, calculate_projected_total, game_clock
from signal_analysis import calculate_fair_line


class ProjectionModelTests(unittest.TestCase):
    def test_four_by_ten_projection_uses_calibrated_pace_response(self):
        result = calculate_live_projection(
            "40 - 35",
            "Q2 05:00",
            "Home - Away",
            "FIBA",
            market_total=160,
        )
        self.assertEqual(result["projected_total"], 178.5)
        self.assertEqual(result["raw_projected_total"], 200.0)
        self.assertEqual(result["components"]["observed_pace_weight"], 0.14)

    def test_projection_converges_to_current_score_at_regulation_end(self):
        projected = calculate_projected_total(
            "80 - 75",
            "Q4-Ended",
            "Home - Away",
            "FIBA",
            prior_total=160,
        )
        self.assertEqual(projected, 155.0)

    def test_missing_clock_and_overtime_do_not_invent_projection(self):
        self.assertIsNone(
            calculate_projected_total("40 - 35", "Live", prior_total=160)
        )
        self.assertIsNone(
            calculate_projected_total("80 - 80", "OT 03:00", prior_total=160)
        )

    def test_period_only_status_keeps_period_but_does_not_invent_clock(self):
        cases = (
            ("Q3", "FIBA", 3),
            ("3rd", "FIBA", 3),
            ("2H", "NCAA", 2),
        )
        for status, tournament, expected_period in cases:
            with self.subTest(status=status, tournament=tournament):
                clock = game_clock(status, "Home - Away", tournament)
                self.assertEqual(clock["period"], expected_period)
                self.assertIsNone(clock["remaining_min"])
                self.assertIsNone(
                    calculate_projected_total(
                        "40 - 35",
                        status,
                        "Home - Away",
                        tournament,
                        prior_total=160,
                    )
                )

    def test_invalid_prior_uses_truthful_neutral_fallback_metadata(self):
        result = calculate_live_projection(
            "40 - 35",
            "Q2 05:00",
            "Home - Away",
            "FIBA",
            market_total=50,
        )
        self.assertEqual(result["components"]["prior_total"], 160.0)
        self.assertEqual(result["components"]["prior_source"], "neutral_fallback")
        self.assertFalse(result["components"]["prior_valid"])
        self.assertLess(result["data_quality"], 80)
        self.assertTrue(any("geçersiz" in note for note in result["notes"]))

    def test_valid_opening_replaces_invalid_market_prior(self):
        result = calculate_live_projection(
            "40 - 35",
            "Q2 05:00",
            "Home - Away",
            "FIBA",
            market_total=float("nan"),
            opening_total=150,
        )
        self.assertEqual(result["components"]["prior_total"], 150.0)
        self.assertEqual(result["components"]["prior_source"], "opening_total")
        self.assertTrue(result["components"]["prior_valid"])

    def test_nba_uses_four_by_twelve_but_is_not_yet_validated_for_gate(self):
        clock = game_clock("Q3 06:00", "A - B", "NBA")
        self.assertEqual(clock["quarter_length"], 12)
        self.assertEqual(clock["period_count"], 4)
        self.assertEqual(clock["format"], "4x12")
        self.assertFalse(clock["model_validated"])

    def test_ncaa_men_and_women_use_different_period_structures(self):
        men = game_clock("2H 10:00", "A - B", "NCAA")
        women = game_clock("Q3 05:00", "A Women - B Women", "NCAA Women")
        self.assertEqual((men["period_count"], men["quarter_length"]), (2, 20))
        self.assertEqual((women["period_count"], women["quarter_length"]), (4, 10))
        self.assertFalse(men["model_validated"])
        self.assertFalse(women["model_validated"])

    def test_period_outside_format_is_rejected(self):
        clock = game_clock("Q3 05:00", "A - B", "NCAA")
        self.assertIsNone(clock["period"])
        self.assertIsNone(clock["remaining_min"])

    def test_unknown_competition_is_not_model_validated(self):
        clock = game_clock("Q2 05:00", "A - B", "Unknown")
        self.assertEqual(clock["format"], "4x10")
        self.assertFalse(clock["model_validated"])

    def test_fair_line_uses_live_market_calibration(self):
        fair, meta = calculate_fair_line(
            prematch=160,
            pure_pace_projection=178.5,
            elapsed_minutes=15,
            total_game_minutes=40,
            data_quality=90,
            live_line=170,
            period=2,
            current_total=75,
        )
        self.assertEqual(fair, 172.8)
        self.assertEqual(meta["live_market_weight"], 0.66)
        self.assertEqual(meta["model_weight"], 0.34)

    def test_fair_line_converges_at_final_score(self):
        fair, meta = calculate_fair_line(
            prematch=160,
            pure_pace_projection=155,
            elapsed_minutes=40,
            total_game_minutes=40,
            live_line=153,
            period=4,
            current_total=155,
        )
        self.assertEqual(fair, 155.0)
        self.assertEqual(meta["anchor"], "final_score")


if __name__ == "__main__":
    unittest.main()
