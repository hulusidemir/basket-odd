import unittest

from signal_analysis import _market_total
from signal_quality import (
    SIGNAL_SCORE_VERSION,
    build_league_signal_profile,
    calculate_signal_quality,
)


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
        "projected_total": 155.0,
        "fair_line": 160.0,
        "league_signal_stats": {
            "wins": 21,
            "resolved": 30,
            "rate": 70.0,
            "adjusted_rate": 65.0,
            "scope": "league_direction",
        },
    }
    payload.update(overrides)
    return calculate_signal_quality(payload)


class SignalQualityTests(unittest.TestCase):
    def test_strong_evidence_is_prospectively_capped_at_three_stars(self):
        result = quality()

        self.assertEqual(result["quality_score"], 74)
        self.assertEqual(result["quality_label"], "ORTA-GÜÇLÜ")
        self.assertEqual(result["stars"], 3)
        self.assertEqual(result["signal_score_version"], SIGNAL_SCORE_VERSION)
        self.assertEqual(result["score_kind"], "evidence_ranking_not_probability")
        self.assertEqual(sum(item["score"] for item in result["score_components"].values()), 100)
        self.assertIn("ileri tarihli kanıt", result["risk_note"])

    def test_small_league_sample_cannot_receive_five_stars(self):
        result = quality(league_signal_stats={
            "wins": 4,
            "resolved": 4,
            "rate": 100.0,
            "adjusted_rate": 64.3,
        })

        self.assertLessEqual(result["quality_score"], 74)
        self.assertLessEqual(result["stars"], 3)
        self.assertIn("lig örneklemi", result["risk_note"])

    def test_fair_line_against_direction_is_hard_capped(self):
        result = quality(fair_line=175.0)

        self.assertLessEqual(result["quality_score"], 39)
        self.assertIn("adil barem sinyal yönünü desteklemiyor", result["risk_note"])

    def test_projection_against_direction_is_hard_capped(self):
        result = quality(projected_total=180.0)

        self.assertLessEqual(result["quality_score"], 49)
        self.assertIn("tempo projeksiyonu sinyal yönüne ters", result["risk_note"])

    def test_fair_edge_below_four_is_risky(self):
        result = quality(fair_line=167.0)

        self.assertLessEqual(result["quality_score"], 59)
        self.assertIn("4 sayının altında", result["risk_note"])

    def test_missing_clock_is_capped(self):
        result = quality(status="Live")

        self.assertLessEqual(result["quality_score"], 39)
        self.assertIn("kesin maç saati yok", result["risk_note"])

    def test_first_four_minutes_are_capped(self):
        result = quality(status="Q1 07:00", score="4 - 3")

        self.assertLessEqual(result["quality_score"], 49)
        self.assertIn("Q1 ilk 4 dakika", result["risk_note"])

    def test_direction_flip_is_capped(self):
        result = quality(previous_directions=["ÜST"])

        self.assertLessEqual(result["quality_score"], 59)
        self.assertIn("yön değişimi", result["risk_note"])

    def test_bookmaker_metadata_does_not_change_score(self):
        self.assertEqual(
            quality(odds_snapshot={"bookmaker_count": 1})["quality_score"],
            quality(odds_snapshot={"bookmaker_count": 5})["quality_score"],
        )

    def test_league_profile_counts_each_match_once_with_current_strategy(self):
        base = {
            "match_name": "Home - Away",
            "tournament": "FIBA",
            "opening": 160,
            "prematch": 160,
            "live": 170,
            "score": "40 - 35",
            "status": "Q2 05:00",
            "result": "Başarılı",
        }
        rows = [
            {**base, "id": 1, "match_id": "m1", "signal_count": 1, "final_score": "90 - 90"},
            {**base, "id": 2, "match_id": "m1", "signal_count": 2, "final_score": "90 - 90"},
            {**base, "id": 3, "match_id": "m2", "signal_count": 1, "final_score": "80 - 80"},
        ]

        profile = build_league_signal_profile(rows)

        self.assertEqual(profile["leagues"]["FIBA"]["overall"]["resolved"], 2)
        self.assertEqual(profile["leagues"]["FIBA"]["overall"]["wins"], 1)

    def test_global_fallback_cannot_create_a_strong_signal(self):
        result = quality(league_signal_stats={
            "wins": 70,
            "resolved": 100,
            "rate": 70.0,
            "adjusted_rate": 68.2,
            "scope": "global_fallback",
        })

        self.assertLessEqual(result["quality_score"], 64)
        self.assertIn("bu lig için", result["risk_note"])

    def test_prematch_line_has_priority_over_opening(self):
        match = {
            "opening": 150,
            "prematch": 162,
            "odds_snapshot": {"opening_median": 151, "prematch_median": 164},
        }
        self.assertEqual(_market_total(match, 150), 164.0)


if __name__ == "__main__":
    unittest.main()
