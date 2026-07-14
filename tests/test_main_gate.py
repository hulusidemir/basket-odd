import json
import logging
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from main import (
    _normalize_match_payload,
    _scraper_health_summary,
    process_match,
    process_match_batch,
    setup_logging,
)
from pace_tracker import PaceTracker


class FakeDatabase:
    def __init__(self):
        self.saved_analysis = None
        self.telegram_required = None
        self.telegram_sent = None
        self.telegram_failed = None

    def is_match_deleted(self, match_id):
        return False

    def count_match_alerts(self, match_id):
        return 0

    def was_alerted_in_period(self, match_id, period):
        return False

    def latest_match_alert_in_direction(self, match_id, direction):
        return None

    def save_alert(self, *args, **kwargs):
        self.saved_analysis = json.loads(kwargs["ai_analysis"])
        self.telegram_required = kwargs.get("telegram_required")
        return 1

    def mark_telegram_delivery_sent(self, alert_id, message_ids):
        self.telegram_sent = (alert_id, message_ids)
        return True

    def mark_telegram_delivery_failed(self, alert_id, error):
        self.telegram_failed = (alert_id, error)
        return True

    def is_upcoming_followed(self, match_id):
        return False


def match_payload():
    return {
        "match_id": "match-1",
        "match_name": "Home - Away",
        "tournament": "FIBA",
        "opening_total": 160.0,
        "prematch_total": 160.0,
        "inplay_total": 170.0,
        "status": "Q2 05:00",
        "score": "40 - 35",
        "url": "",
    }


class MainGateTests(unittest.IsolatedAsyncioTestCase):
    def test_transport_request_logs_are_suppressed_to_protect_tokens(self):
        setup_logging("INFO")

        self.assertGreaterEqual(logging.getLogger("httpx").level, logging.WARNING)
        self.assertGreaterEqual(logging.getLogger("httpcore").level, logging.WARNING)
        self.assertGreaterEqual(
            logging.getLogger("telegram.request").level,
            logging.WARNING,
        )

    def test_optional_text_fields_are_normalized(self):
        payload = match_payload()
        payload.update({"status": None, "score": None, "tournament": None, "url": None})
        normalized = _normalize_match_payload(payload)
        self.assertEqual(normalized["status"], "")
        self.assertEqual(normalized["score"], "")
        self.assertEqual(normalized["tournament"], "")
        self.assertEqual(normalized["url"], "")

    async def run_case(self, state, telegram_allowed):
        db = FakeDatabase()
        notifier = SimpleNamespace(send_alert=AsyncMock(return_value={"chat": 1}))
        config = SimpleNamespace(
            BLACKLIST=[],
            THRESHOLD=10,
            MAX_SIGNALS_PER_MATCH=3,
            SAME_DIRECTION_MIN_LIVE_DELTA=10,
        )
        analysis = {
            "direction": "ALT",
            "final_direction": "ALT",
            "candidate_eligible": True,
            "fair_line": 165.0,
        }
        quality = {
            "quality_score": 90,
            "model_support_score": 90,
            "data_reliability_score": 95,
            "data_hard_fail": False,
        }
        gate = {
            "state": state,
            "telegram_allowed": telegram_allowed,
            "trial_eligible": True,
        }
        with (
            patch("main.build_signal_analysis", return_value=analysis),
            patch("main.calculate_signal_quality", return_value=quality),
            patch("main.evaluate_signal_gate", return_value=gate),
        ):
            await process_match(match_payload(), db, notifier, config)
        return db, notifier

    async def test_shadow_is_saved_but_not_sent(self):
        db, notifier = await self.run_case("SHADOW", False)
        self.assertEqual(db.saved_analysis["signal_gate"]["state"], "SHADOW")
        self.assertFalse(db.telegram_required)
        self.assertIsNone(db.telegram_sent)
        self.assertIsNone(db.telegram_failed)
        notifier.send_alert.assert_not_awaited()

    async def test_trusted_is_saved_and_sent(self):
        db, notifier = await self.run_case("TRUSTED", True)
        self.assertEqual(db.saved_analysis["signal_gate"]["state"], "TRUSTED")
        self.assertTrue(db.telegram_required)
        self.assertEqual(db.telegram_sent, (1, {"chat": 1}))
        self.assertIsNone(db.telegram_failed)
        notifier.send_alert.assert_awaited_once()

    async def test_empty_delivery_is_marked_for_retry(self):
        db = FakeDatabase()
        notifier = SimpleNamespace(send_alert=AsyncMock(return_value={}))
        config = SimpleNamespace(
            BLACKLIST=[],
            THRESHOLD=10,
            MAX_SIGNALS_PER_MATCH=3,
            SAME_DIRECTION_MIN_LIVE_DELTA=10,
        )
        analysis = {
            "direction": "ALT",
            "final_direction": "ALT",
            "candidate_eligible": True,
            "fair_line": 165.0,
        }
        quality = {
            "quality_score": 90,
            "model_support_score": 90,
            "data_reliability_score": 95,
            "data_hard_fail": False,
        }
        gate = {
            "state": "TRUSTED",
            "telegram_allowed": True,
            "trial_eligible": True,
        }
        with (
            patch("main.build_signal_analysis", return_value=analysis),
            patch("main.calculate_signal_quality", return_value=quality),
            patch("main.evaluate_signal_gate", return_value=gate),
        ):
            await process_match(match_payload(), db, notifier, config)

        self.assertTrue(db.telegram_required)
        self.assertIsNone(db.telegram_sent)
        self.assertEqual(db.telegram_failed[0], 1)

    async def test_one_bad_match_does_not_stop_the_batch(self):
        matches = [
            {"match_id": "bad", "match_name": "Bad"},
            match_payload(),
        ]
        with patch(
            "main.process_match",
            new=AsyncMock(side_effect=[ValueError("bad payload"), None]),
        ) as mocked:
            summary = await process_match_batch(
                matches,
                object(),
                object(),
                object(),
                PaceTracker(),
            )

        self.assertEqual(mocked.await_count, 2)
        self.assertEqual(summary["received"], 2)
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(summary["failed"], 1)

    def test_match_payload_rejects_non_finite_required_totals(self):
        payload = match_payload()
        payload["inplay_total"] = float("nan")
        with self.assertRaisesRegex(ValueError, "out-of-range"):
            _normalize_match_payload(payload)

    def test_scraper_health_summary_keeps_counts_but_not_error_payloads(self):
        scraper = SimpleNamespace(last_report={
            "status": "partial",
            "discovered_count": 8,
            "attempted_count": 8,
            "parsed_count": 6,
            "failed_count": 2,
            "coverage_pct": 75.0,
            "errors": ["sensitive upstream detail"],
        })
        summary = _scraper_health_summary(scraper)
        self.assertEqual(summary["parsed_count"], 6)
        self.assertEqual(summary["coverage_pct"], 75.0)
        self.assertNotIn("errors", summary)


if __name__ == "__main__":
    unittest.main()
