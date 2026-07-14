import unittest

from signal_analysis import _market_total
from signal_quality import calculate_signal_quality


def quality(**overrides):
    payload = {
        "direction": "ALT",
        "opening": 160.0,
        "prematch": 160.0,
        "live": 170.0,
        "score": "52 - 48",
        "status": "Q3 04:00",
        "match_name": "Home - Away",
        "tournament": "FIBA",
        "url": "https://example.test/match",
        "pure_projected_total": 155.0,
        "fair_line": 160.0,
        "projection_quality": 85,
        "sustainable_ppm": 3.4,
        "odds_snapshot": {
            "bookmaker_count": 2,
            "paired_bookmaker_count": 2,
            "opening_min": 159.5,
            "opening_max": 160.5,
            "inplay_min": 169.5,
            "inplay_max": 170.5,
        },
    }
    payload.update(overrides)
    return calculate_signal_quality(payload)


class SignalQualityTests(unittest.TestCase):
    def test_high_quality_requires_complete_basketball_evidence(self):
        result = quality()
        self.assertEqual(result["quality_label"], "GÜÇLÜ MODEL UYUMU")
        self.assertGreaterEqual(result["quality_score"], 80)
        self.assertEqual(result["score_kind"], "heuristic_not_probability")

    def test_missing_clock_is_capped_below_watch(self):
        result = quality(status="Live")
        self.assertLessEqual(result["quality_score"], 49)
        self.assertIn("kesin maç saati yok", result["risk_note"])

    def test_fair_line_without_projection_is_a_safe_data_failure(self):
        result = quality(
            pure_projected_total=None,
            score="",
            status="Live",
            fair_line=160.0,
        )
        self.assertIsNone(result["projection"])
        self.assertIsNone(result["projection_diff"])
        self.assertTrue(result["data_hard_fail"])
        self.assertLessEqual(result["quality_score"], 49)

    def test_first_four_minutes_are_not_actionable(self):
        result = quality(status="Q1 07:00", score="4 - 3")
        self.assertLessEqual(result["quality_score"], 49)
        self.assertIn("Q1 ilk 4 dakika", result["risk_note"])

    def test_late_close_game_penalizes_under_more_than_over(self):
        under = quality(status="Q4 06:00", score="70 - 67", direction="ALT")
        over = quality(status="Q4 06:00", score="70 - 67", direction="ÜST", fair_line=180.0)
        self.assertLess(under["components"]["game_script"], over["components"]["game_script"])

    def test_same_direction_repeat_is_not_bonus(self):
        first = quality()
        repeated = quality(previous_directions=["ALT"])
        self.assertEqual(first["quality_score"], repeated["quality_score"])

    def test_model_support_does_not_count_fair_edge_as_a_second_signal(self):
        strong_fair = quality(fair_line=155.0)
        weak_fair = quality(fair_line=168.0)

        self.assertEqual(
            strong_fair["model_support_score"],
            weak_fair["model_support_score"],
        )
        self.assertNotEqual(
            strong_fair["expert_heuristic_score"],
            weak_fair["expert_heuristic_score"],
        )

    def test_direction_flip_is_capped(self):
        result = quality(previous_directions=["ÜST"])
        self.assertLessEqual(result["quality_score"], 59)

    def test_prematch_line_has_priority_over_opening(self):
        match = {
            "opening": 150,
            "prematch": 162,
            "odds_snapshot": {"opening_median": 151, "prematch_median": 164},
        }
        self.assertEqual(_market_total(match, 150), 164.0)

    def test_single_bookmaker_or_wide_market_is_a_hard_data_failure(self):
        single = quality(odds_snapshot={
            "bookmaker_count": 1,
            "paired_bookmaker_count": 1,
            "opening_min": 160,
            "opening_max": 160,
            "inplay_min": 170,
            "inplay_max": 170,
        })
        self.assertTrue(single["data_hard_fail"])
        self.assertFalse(single["data_checks"]["paired_market_consensus"])

        wide = quality(odds_snapshot={
            "bookmaker_count": 2,
            "paired_bookmaker_count": 2,
            "opening_min": 150,
            "opening_max": 170,
            "inplay_min": 155,
            "inplay_max": 180,
        })
        self.assertTrue(wide["data_hard_fail"])
        self.assertFalse(wide["data_checks"]["market_dispersion_valid"])

    def test_missing_result_source_url_cannot_enter_prospective_evidence(self):
        result = quality(url="")

        self.assertTrue(result["data_hard_fail"])
        self.assertFalse(result["data_checks"]["result_source_present"])


if __name__ == "__main__":
    unittest.main()
