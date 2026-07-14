import json
import unittest
from datetime import datetime, timedelta, timezone

from signal_gate import (
    DEFAULT_GATE_POLICY,
    build_gate_evidence,
    evaluate_signal_gate,
    wilson_lower_95,
)
from signal_analysis import build_signal_analysis
from signal_quality import calculate_signal_quality


def gate_row(index, *, success=True, match_id=None, version=None, eligible=True):
    moment = datetime(2026, 7, 14, tzinfo=timezone.utc) + timedelta(minutes=index)
    gate = {
        "policy_id": DEFAULT_GATE_POLICY.policy_id,
        "strategy_id": DEFAULT_GATE_POLICY.strategy_id,
        "strategy_version": DEFAULT_GATE_POLICY.strategy_version if version is None else version,
        "strategy_fingerprint": DEFAULT_GATE_POLICY.strategy_fingerprint,
        "evidence_epoch": DEFAULT_GATE_POLICY.evidence_epoch,
        "evaluated_at": moment.isoformat(),
        "trial_eligible": eligible,
    }
    return {
        "id": index + 1,
        "match_id": match_id or f"match-{index}",
        "result": "Başarılı" if success else "Başarısız",
        "ai_analysis": json.dumps({"signal_gate": gate}),
    }


class SignalGateTests(unittest.TestCase):
    def test_wilson_threshold_is_conservative(self):
        self.assertGreaterEqual(wilson_lower_95(79, 100), 70.0)
        self.assertLess(wilson_lower_95(78, 100), 70.0)

    def test_same_match_counts_once_and_wrong_version_is_ignored(self):
        rows = [
            gate_row(0, match_id="same"),
            gate_row(1, match_id="same", success=False),
            gate_row(2, version=DEFAULT_GATE_POLICY.strategy_version + 1),
        ]
        evidence = build_gate_evidence(
            rows,
            as_of=datetime(2026, 7, 15, tzinfo=timezone.utc),
        )
        self.assertEqual(evidence["eligible_unique"], 1)
        self.assertEqual(evidence["resolved_unique"], 1)
        self.assertEqual(evidence["wins"], 1)

    def test_future_rows_are_excluded(self):
        rows = [gate_row(0), gate_row(10)]
        evidence = build_gate_evidence(
            rows,
            as_of=datetime(2026, 7, 14, 0, 5, tzinfo=timezone.utc),
        )
        self.assertEqual(evidence["eligible_unique"], 1)

    def test_zero_evidence_is_shadow_for_eligible_candidate(self):
        gate = evaluate_signal_gate(
            {"match_id": "new", "signal_count": 1},
            {"candidate_eligible": True},
            {
                "quality_score": 90,
                "model_support_score": 90,
                "expert_heuristic_score": 90,
                "data_reliability_score": 95,
                "data_hard_fail": False,
            },
        )
        self.assertEqual(gate["state"], "SHADOW")
        self.assertFalse(gate["telegram_allowed"])

    def test_stable_79_percent_prospective_sample_is_trusted(self):
        # 40/50 + 39/50: both blocks >=70%, Wilson >=60%, gap <=10%.
        outcomes = [True] * 40 + [False] * 10 + [True] * 39 + [False] * 11
        rows = [gate_row(index, success=success) for index, success in enumerate(outcomes)]
        evidence = build_gate_evidence(
            rows,
            as_of=datetime(2026, 7, 16, tzinfo=timezone.utc),
        )
        gate = evaluate_signal_gate(
            {"match_id": "next", "signal_count": 1},
            {"candidate_eligible": True},
            {
                "quality_score": 90,
                "model_support_score": 90,
                "expert_heuristic_score": 90,
                "data_reliability_score": 95,
                "data_hard_fail": False,
            },
            evidence,
        )
        self.assertEqual(gate["state"], "TRUSTED")
        self.assertTrue(gate["telegram_allowed"])

    def test_unstable_blocks_remain_shadow(self):
        outcomes = [True] * 45 + [False] * 5 + [True] * 34 + [False] * 16
        rows = [gate_row(index, success=success) for index, success in enumerate(outcomes)]
        evidence = build_gate_evidence(
            rows,
            as_of=datetime(2026, 7, 16, tzinfo=timezone.utc),
        )
        gate = evaluate_signal_gate(
            {"match_id": "next", "signal_count": 1},
            {"candidate_eligible": True},
            {
                "quality_score": 90,
                "model_support_score": 90,
                "expert_heuristic_score": 90,
                "data_reliability_score": 95,
                "data_hard_fail": False,
            },
            evidence,
        )
        self.assertEqual(gate["state"], "SHADOW")
        self.assertIn("TIME_BLOCK_RATE_BELOW_70", gate["reason_codes"])

    def test_duplicate_signal_is_blocked(self):
        gate = evaluate_signal_gate(
            {"match_id": "same", "signal_count": 2},
            {"candidate_eligible": True},
            {
                "quality_score": 90,
                "model_support_score": 90,
                "expert_heuristic_score": 90,
                "data_reliability_score": 95,
                "data_hard_fail": False,
            },
        )
        self.assertEqual(gate["state"], "BLOCKED")
        self.assertIn("BLOCKED_DUPLICATE", gate["reason_codes"])

    def test_manual_and_future_settlements_do_not_enter_historical_evidence(self):
        manual = gate_row(0)
        manual["result_source"] = "manual"
        manual["settled_at"] = "2026-07-14T00:01:00+00:00"
        future = gate_row(1)
        future["result_source"] = "automatic_final_score"
        future["settled_at"] = "2026-07-15T00:00:00+00:00"

        evidence = build_gate_evidence(
            [manual, future],
            as_of=datetime(2026, 7, 14, 12, tzinfo=timezone.utc),
        )

        self.assertEqual(evidence["eligible_unique"], 2)
        self.assertEqual(evidence["resolved_unique"], 0)

    def test_adverse_game_script_is_blocked_even_with_model_support(self):
        gate = evaluate_signal_gate(
            {"match_id": "blowout", "signal_count": 1},
            {"candidate_eligible": True},
            {
                "model_support_score": 100,
                "expert_heuristic_score": 44,
                "data_reliability_score": 100,
                "data_hard_fail": False,
                "components": {"game_script": -12},
            },
        )
        self.assertEqual(gate["state"], "BLOCKED")
        self.assertIn("ADVERSE_GAME_SCRIPT", gate["reason_codes"])

    def test_low_expert_score_alone_does_not_block_a_clean_candidate(self):
        gate = evaluate_signal_gate(
            {"match_id": "clean", "signal_count": 1},
            {"candidate_eligible": True},
            {
                "model_support_score": 85,
                "expert_heuristic_score": 20,
                "data_reliability_score": 100,
                "data_hard_fail": False,
                "components": {"game_script": 0},
            },
        )

        self.assertEqual(gate["state"], "SHADOW")
        self.assertTrue(gate["trial_eligible"])
        self.assertNotIn("EXPERT_RISK_SCORE_BELOW_THRESHOLD", gate["reason_codes"])

    def test_clean_q2_and_q3_candidates_reach_prospective_shadow(self):
        cases = (
            ("Q2 05:00", "40 - 35", {"home": [20, 20], "away": [18, 17]}),
            ("Q3 05:00", "60 - 55", {"home": [20, 20, 20], "away": [18, 17, 20]}),
        )
        for status, score, quarter_scores in cases:
            with self.subTest(status=status):
                match = {
                    "match_id": f"clean-{status[:2].lower()}",
                    "match_name": "Home - Away",
                    "tournament": "FIBA",
                    "url": f"https://example.test/{status[:2].lower()}",
                    "opening_total": 160.0,
                    "prematch_total": 160.0,
                    "inplay_total": 170.0,
                    "status": status,
                    "score": score,
                    "direction": "ALT",
                    "signal_count": 1,
                    "quarter_scores": quarter_scores,
                    "odds_snapshot": {
                        "bookmaker_count": 3,
                        "paired_bookmaker_count": 3,
                        "opening_min": 159.5,
                        "opening_max": 160.5,
                        "inplay_min": 169.5,
                        "inplay_max": 170.5,
                    },
                }
                analysis = build_signal_analysis(match, {}, threshold=10)
                quality = calculate_signal_quality(
                    {
                        **match,
                        **analysis,
                        "opening": match["opening_total"],
                        "prematch": match["prematch_total"],
                        "live": match["inplay_total"],
                        "direction": analysis["direction"],
                    }
                )
                gate = evaluate_signal_gate(match, analysis, quality)

                self.assertTrue(analysis["candidate_eligible"])
                self.assertGreaterEqual(quality["model_support_score"], 80)
                self.assertGreaterEqual(quality["data_reliability_score"], 85)
                self.assertEqual(quality["components"]["game_script"], 0)
                self.assertEqual(gate["state"], "SHADOW")
                self.assertTrue(gate["trial_eligible"])


if __name__ == "__main__":
    unittest.main()
