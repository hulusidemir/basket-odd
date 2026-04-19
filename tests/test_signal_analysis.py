import unittest
from unittest.mock import patch

from signal_quality import build_signal_analysis


class SignalAnalysisTest(unittest.TestCase):
    def base_match(self):
        return {
            "opening_total": 160.0,
            "inplay_total": 174.0,
            "baseline": 160.0,
            "baseline_label": "Açılış",
            "match_name": "Home - Away",
            "tournament": "Test League",
            "status": "Q2 05:00",
            "score": "44-40",
        }

    @patch("signal_quality._extract_h2h_metrics")
    @patch("signal_quality.calculate_projected_total")
    def test_fair_line_weights_projection_60_history_40(self, projected, h2h):
        projected.return_value = 180.0
        h2h.return_value = {
            "expected_total": 160.0,
            "h2h_avg_total": None,
            "home_last5": {},
            "away_last5": {},
        }

        analysis = build_signal_analysis(self.base_match(), {"h2h": {"body_text": ""}}, 10)

        self.assertEqual(analysis["fair_line"], 172.0)
        self.assertEqual(analysis["weights"], {"projection": 60, "history": 40})
        self.assertEqual(analysis["direction"], "ALT")
        self.assertIn("Adil Barem", analysis["summary"])

    @patch("signal_quality._extract_h2h_metrics")
    @patch("signal_quality.calculate_projected_total")
    def test_direction_still_uses_opening_live_movement_when_fair_line_above_live(self, projected, h2h):
        projected.return_value = 190.0
        h2h.return_value = {
            "expected_total": 180.0,
            "h2h_avg_total": None,
            "home_last5": {},
            "away_last5": {},
        }

        analysis = build_signal_analysis(self.base_match(), {"h2h": {"body_text": ""}}, 10)

        self.assertEqual(analysis["fair_line"], 186.0)
        self.assertEqual(analysis["direction"], "ALT")
        self.assertGreater(analysis["fair_edge"], 0)

    @patch("signal_quality._extract_h2h_metrics")
    @patch("signal_quality.calculate_projected_total")
    def test_line_down_creates_over_even_if_fair_line_below_live(self, projected, h2h):
        projected.return_value = 150.0
        h2h.return_value = {
            "expected_total": 145.0,
            "h2h_avg_total": None,
            "home_last5": {},
            "away_last5": {},
        }
        match = self.base_match()
        match["opening_total"] = 174.0
        match["inplay_total"] = 160.0
        match["baseline"] = 174.0

        analysis = build_signal_analysis(match, {"h2h": {"body_text": ""}}, 10)

        self.assertEqual(analysis["direction"], "ÜST")
        self.assertLess(analysis["fair_edge"], 0)


if __name__ == "__main__":
    unittest.main()
