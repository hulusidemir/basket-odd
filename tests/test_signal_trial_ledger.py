import asyncio
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

from db import Database
from finished_match_service import run_deleted_match_result_cycle
from signal_gate import DEFAULT_GATE_POLICY, build_gate_evidence


def trial_analysis(match_id: str) -> str:
    policy = DEFAULT_GATE_POLICY
    gate = {
        "policy_id": policy.policy_id,
        "strategy_id": policy.strategy_id,
        "strategy_version": policy.strategy_version,
        "strategy_fingerprint": policy.strategy_fingerprint,
        "evidence_epoch": policy.evidence_epoch,
        "evaluated_at": "2026-07-14T10:00:00+00:00",
        "trial_key": f"{policy.strategy_id}:{policy.strategy_version}:{match_id}",
        "match_id": match_id,
        "trial_eligible": True,
        "state": "SHADOW",
        "telegram_allowed": False,
    }
    return json.dumps({"signal_gate": gate}, ensure_ascii=False)


class SignalTrialLedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp_dir.name) / "test.db"))
        self.db.init()

    def tearDown(self):
        self.temp_dir.cleanup()

    def save_trial(self, match_id: str = "match-1") -> int:
        return self.db.save_alert(
            match_id,
            "Home - Away",
            160,
            170,
            "ALT",
            10,
            tournament="FIBA",
            status="Q2 05:00",
            score="40 - 35",
            url=f"https://example.test/{match_id}",
            ai_analysis=trial_analysis(match_id),
            alert_period=2,
        )

    def test_automatic_result_survives_deleted_page_purge(self):
        alert_id = self.save_trial()
        self.db.delete_match_data("match-1")
        self.assertTrue(self.db.update_deleted_alert_final_result(
            alert_id,
            result="Başarılı",
            final_score="80 - 75",
            final_status="Full Time",
        ))

        before_purge = self.db.signal_trial_rows()
        self.assertEqual(before_purge[0]["result_source"], "automatic_final_score")
        self.assertEqual(build_gate_evidence(
            before_purge,
            as_of=datetime.now(timezone.utc) + timedelta(seconds=1),
        )["resolved_unique"], 1)

        self.assertEqual(
            self.db.purge_deleted_matches(),
            {"deleted_count": 1, "protected_count": 0},
        )
        after_purge = self.db.signal_trial_rows()
        self.assertEqual(len(after_purge), 1)
        self.assertEqual(after_purge[0]["result"], "Başarılı")

    def test_manual_result_never_enters_evidence_ledger(self):
        alert_id = self.save_trial()
        self.db.delete_match_data("match-1")
        self.assertTrue(self.db.update_deleted_alert_result(alert_id, "Başarılı"))

        trial_rows = self.db.signal_trial_rows()
        self.assertEqual(trial_rows[0]["result"], "")
        evidence = build_gate_evidence(
            trial_rows,
            as_of=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
        self.assertEqual(evidence["eligible_unique"], 1)
        self.assertEqual(evidence["resolved_unique"], 0)

    def test_unresolved_trial_and_its_settlement_source_survive_purge(self):
        alert_id = self.save_trial("pending-match")
        self.db.delete_match_data("pending-match")

        outcome = self.db.purge_deleted_matches()

        self.assertEqual(outcome, {"deleted_count": 0, "protected_count": 1})
        self.assertIsNotNone(self.db.get_deleted_alert_by_id(alert_id))
        self.assertFalse(self.db.delete_alert(alert_id))
        self.assertTrue(self.db.is_deleted_alert_protected(alert_id))
        self.assertEqual(len(self.db.signal_trial_rows()), 1)

    def test_manual_display_result_does_not_block_automatic_trial_settlement(self):
        alert_id = self.save_trial("manual-pending")
        self.db.delete_match_data("manual-pending")
        self.db.update_deleted_alert_result(alert_id, "Başarısız")

        with patch(
            "finished_match_service.AiscoreFinishedMatchChecker.check_matches",
            new=AsyncMock(return_value=[{
                "match_id": "manual-pending",
                "match_name": "Home - Away",
                "status": "Full Time",
                "score": "80 - 75",
                "is_finished": True,
            }]),
        ):
            summary = asyncio.run(run_deleted_match_result_cycle(
                self.db,
                _Config(),
            ))

        alert = self.db.get_deleted_alert_by_id(alert_id)
        self.assertEqual(alert["result"], "Başarısız")
        self.assertEqual(alert["result_source"], "manual")
        self.assertEqual(alert["final_score"], "80 - 75")
        self.assertEqual(summary["trial_updated_count"], 1)
        trial = self.db.signal_trial_rows()[0]
        self.assertEqual(trial["result"], "Başarılı")
        self.assertEqual(trial["result_source"], "automatic_final_score")
        evidence = build_gate_evidence(
            [trial],
            as_of=datetime.now(timezone.utc) + timedelta(seconds=1),
        )
        self.assertEqual(evidence["resolved_unique"], 1)

    def test_evidence_query_can_filter_and_bound_recent_policy_rows(self):
        for index in range(3):
            self.save_trial(f"bounded-{index}")

        policy = DEFAULT_GATE_POLICY
        rows = self.db.signal_trial_rows(
            policy_id=policy.policy_id,
            strategy_id=policy.strategy_id,
            strategy_version=policy.strategy_version,
            evidence_epoch=policy.evidence_epoch,
            limit=2,
        )

        self.assertEqual(
            [row["match_id"] for row in rows],
            ["bounded-1", "bounded-2"],
        )

    def test_archived_tombstone_rejects_new_active_alert(self):
        self.save_trial()
        self.db.delete_match_data("match-1")

        with self.assertRaisesRegex(RuntimeError, "archived match"):
            self.db.save_alert(
                "match-1", "Home - Away", 160, 172, "ALT", 12,
                signal_count=1,
            )
        self.assertEqual(self.db.count_match_alerts("match-1"), 0)


class _Config:
    PAGE_TIMEOUT_MS = 100


if __name__ == "__main__":
    unittest.main()
