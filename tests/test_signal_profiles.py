import unittest

from claude_ai_filter import evaluate_claude_ai, scenario_play_direction
from finished_match_service import canonical_alert_direction
from signal_profiles import evaluate_hundred_profile, evaluate_telegram_profiles


class HistoricalSignalProfileTests(unittest.TestCase):
    def test_hundred_profile_under_rule(self):
        result = evaluate_hundred_profile(
            {"direction": "ALT", "score": "70 - 65", "diff": 12.5},
            {},
        )
        self.assertTrue(result["hundred_profile"])
        self.assertEqual(result["hundred_profile_rule"], "alt_total_110_140_diff_12_14")

    def test_hundred_profile_becomes_ca_confirmation(self):
        result = evaluate_claude_ai(
            {"direction": "ÜST", "hundred_profile": 1},
            {},
        )
        self.assertEqual(result["claude_ai"], "TRUE_OVER")
        self.assertEqual(result["claude_ai_rule"], "100 Profili onayı")

    def test_ca_b1_historical_rule(self):
        result = evaluate_claude_ai(
            {
                "direction": "ALT",
                "hundred_profile": 0,
                "opening": 165,
                "live": 172,
                "score": "80 - 75",
            },
            {"fair_edge": -5},
        )
        self.assertEqual(result["claude_ai"], "TRUE_UNDER")
        self.assertTrue(result["claude_ai_rule"].startswith("B1:"))

    def test_ca_fade_under_play_direction_is_under(self):
        result = evaluate_claude_ai(
            {
                "direction": "ÜST",
                "hundred_profile": 0,
                "opening": 165,
                "live": 172,
                "score": "65 - 60",
                "diff": 13,
            },
            {},
        )
        self.assertEqual(result["claude_ai"], "FADE_UNDER")
        self.assertEqual(scenario_play_direction(result["claude_ai"]), "ALT")

    def test_ca_fade_direction_is_used_for_settlement(self):
        self.assertEqual(
            canonical_alert_direction({
                "direction": "ÜST",
                "claude_ai": "FADE_UNDER",
                "ai_analysis": '{"direction":"ÜST","final_direction":"ÜST"}',
            }),
            "ALT",
        )

    def test_telegram_requires_both_hundred_profile_and_ca(self):
        ca_only = evaluate_telegram_profiles(
            {
                "direction": "ALT",
                "opening": 165,
                "live": 172,
                "score": "80 - 75",
                "diff": 7,
            },
            {"fair_edge": -5},
        )
        self.assertFalse(ca_only["hundred_profile"])
        self.assertEqual(ca_only["claude_ai"], "TRUE_UNDER")

        both = evaluate_telegram_profiles(
            {"direction": "ALT", "score": "70 - 65", "diff": 12.5},
            {},
        )
        self.assertTrue(both["hundred_profile"])
        self.assertEqual(both["claude_ai"], "TRUE_UNDER")


if __name__ == "__main__":
    unittest.main()
