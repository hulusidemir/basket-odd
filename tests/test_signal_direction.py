import unittest

from signal_analysis import _classify_signal, _decision_from_components


class SignalDirectionTests(unittest.TestCase):
    def decision(self, **overrides):
        payload = {
            "legacy_direction": "ALT",
            "live": 170.0,
            "opening": 160.0,
            "diff": 10.0,
            "status": "Q2 05:00",
            "projected_total": 170.0,
            "fair_edge": 0.0,
        }
        payload.update(overrides)
        return _decision_from_components(**payload)

    def test_fair_edge_alone_does_not_flip_direction(self):
        self.assertEqual(self.decision(fair_edge=2.0)["direction"], "ALT")
        self.assertEqual(self.decision(fair_edge=-2.0)["direction"], "ALT")

    def test_projection_changes_direction_only_when_material_and_calibrated(self):
        self.assertEqual(
            self.decision(fair_edge=2.0, projected_total=176.0)["direction"],
            "ÜST",
        )
        self.assertEqual(
            self.decision(fair_edge=2.0, projected_total=175.0)["direction"],
            "ALT",
        )

    def test_q4_intercept_cannot_flip_zero_projection_edge(self):
        result = self.decision(
            status="Q4 06:00",
            projected_total=170.0,
            fair_edge=1.1,
        )
        self.assertEqual(result["direction"], "ALT")

    def test_research_candidate_is_reachable_in_q2_but_not_q1(self):
        q2 = self.decision(
            tournament="FIBA",
            projected_total=176.0,
            fair_edge=2.0,
            projection_quality=90,
        )
        self.assertTrue(_classify_signal(q2)["candidate_eligible"])

        q1 = self.decision(
            tournament="FIBA",
            status="Q1 05:00",
            projected_total=176.0,
            fair_edge=3.0,
            projection_quality=90,
        )
        self.assertFalse(_classify_signal(q1)["candidate_eligible"])

    def test_research_backtest_is_descriptive_not_a_direction_override(self):
        profile = {
            "sample_size": 100,
            "buckets": {
                "all": {
                    "ALT": {"wins": 0, "total": 100},
                    "ÜST": {"wins": 100, "total": 100},
                }
            },
        }
        result = self.decision(fair_edge=-5.0, backtest_profile=profile)
        self.assertEqual(result["direction"], "ALT")
        self.assertIn("backtest", result)

    def test_invalid_legacy_direction_falls_back_to_market_move(self):
        result = self.decision(legacy_direction="broken", fair_edge=0.0)
        self.assertEqual(result["direction"], "ALT")

        result = self.decision(
            legacy_direction="broken",
            opening=180.0,
            live=170.0,
            diff=-10.0,
            fair_edge=0.0,
        )
        self.assertEqual(result["direction"], "ÜST")


if __name__ == "__main__":
    unittest.main()
