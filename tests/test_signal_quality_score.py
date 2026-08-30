import unittest

from signal_quality_score import calculate_signal_quality


class SignalQualityScoreTests(unittest.TestCase):
    def test_alt_is_strong_when_score_and_speed_both_leave_live_line_high(self):
        quality = calculate_signal_quality({
            "match_name": "Home - Away",
            "tournament": "EuroLeague",
            "status": "Q2 10:00",
            "score": "20 - 20",
            "opening": 160,
            "live": 171,
            "direction": "ALT",
        })

        self.assertGreaterEqual(quality["score"], 75)
        self.assertEqual(quality["tone"], "strong")
        self.assertGreater(quality["state_support_points"], 0)
        self.assertGreater(quality["pace_support_points"], 0)

    def test_alt_is_not_promoted_when_score_explains_the_line_move(self):
        quality = calculate_signal_quality({
            "match_name": "Home - Away",
            "tournament": "EuroLeague",
            "status": "Q2 10:00",
            "score": "25 - 25",
            "opening": 160,
            "live": 171,
            "direction": "ALT",
        })

        self.assertLess(quality["score"], 60)
        self.assertLess(quality["pace_support_points"], 0)

    def test_ust_is_strong_when_live_line_is_low_for_score_and_clock(self):
        quality = calculate_signal_quality({
            "match_name": "Home - Away",
            "tournament": "EuroLeague",
            "status": "Q2 10:00",
            "score": "20 - 20",
            "opening": 160,
            "live": 149,
            "direction": "ÜST",
        })

        self.assertGreaterEqual(quality["score"], 75)
        self.assertEqual(quality["tone"], "strong")

    def test_very_early_signal_is_penalized(self):
        early = calculate_signal_quality({
            "match_name": "Home - Away",
            "tournament": "EuroLeague",
            "status": "Q1 08:00",
            "score": "4 - 4",
            "opening": 160,
            "live": 171,
            "direction": "ALT",
        })
        later = calculate_signal_quality({
            "match_name": "Home - Away",
            "tournament": "EuroLeague",
            "status": "Q2 10:00",
            "score": "20 - 20",
            "opening": 160,
            "live": 171,
            "direction": "ALT",
        })

        self.assertLess(early["score"], later["score"])
        self.assertIn("çok erken", early["factors"][2]["text"])

    def test_missing_clock_returns_unavailable_instead_of_guessing(self):
        quality = calculate_signal_quality({
            "score": "20 - 20",
            "status": "",
            "opening": 160,
            "live": 171,
            "direction": "ALT",
        })

        self.assertIsNone(quality["score"])
        self.assertEqual(quality["tone"], "unavailable")


if __name__ == "__main__":
    unittest.main()
