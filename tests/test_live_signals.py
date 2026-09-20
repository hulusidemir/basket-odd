import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from config import Config
from db import Database
from live_signals import evaluate_live_signal
from main import _normalize_match_payload, process_match, retry_pending_telegram_deliveries


def match(**overrides):
    return _normalize_match_payload({
        "match_id": "test-match", "match_name": "Home - Away", "tournament": "FIBA",
        "status": "Q2 05:00", "score": "40 - 35", "opening_total": 160,
        "prematch_total": 165, "inplay_total": 180, **overrides,
    })


class LiveSignalRuleTests(unittest.TestCase):
    def test_insufficient_data_returns_pas(self):
        decision = evaluate_live_signal(match(), [], Config())
        self.assertEqual(decision.direction, "PAS")
        self.assertEqual(decision.skip_reason, "PAS_INSUFFICIENT_FUTURE_PACE")

    def test_overtime_never_signals(self):
        decision = evaluate_live_signal(match(status="OT 03:00"), [], Config())
        self.assertEqual(decision.direction, "PAS")
        self.assertEqual(decision.skip_reason, "overtime_disabled")

    def test_missing_score_returns_pas(self):
        decision = evaluate_live_signal(match(score=""), [], Config())
        self.assertEqual(decision.direction, "PAS")
        self.assertEqual(decision.skip_reason, "missing_score")


class LiveSignalIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Database(str(Path(self.tmp.name) / "test.db"))
        self.db.init()
        self.notifier = type("Notifier", (), {})()
        self.notifier.send_alert = AsyncMock(return_value={"recipient": 1})

    def process(self, payload, config=None):
        asyncio.run(process_match(payload, self.db, self.notifier, config or Config()))

    def test_filters_do_not_save_or_send(self):
        for payload in (match(status="Q4 01:00"), match(status="OT 01:00"), match(prematch_total=175)):
            self.process(payload)
        self.assertIsNone(self.db.get_alert(1))
        self.notifier.send_alert.assert_not_awaited()

    def test_stale_market_observation_is_not_saved_or_sent(self):
        payload = match(_market_captured_monotonic=time.monotonic() - 21)

        self.process(payload)

        self.assertEqual(self.db.count_match_alerts(payload["match_id"]), 0)
        self.notifier.send_alert.assert_not_awaited()

    def test_source_stale_market_is_not_saved_or_sent(self):
        payload = match(
            _market_stale=True,
            _market_stale_reason="stale_inplay_total",
        )

        self.process(payload)

        self.assertEqual(self.db.count_match_alerts(payload["match_id"]), 0)
        self.notifier.send_alert.assert_not_awaited()

    def test_signal_delivery_uses_notifier_pace_parameter_names(self):
        class StrictNotifier:
            async def send_alert(
                self,
                match_name,
                tournament,
                opening,
                live,
                direction,
                diff,
                status,
                *,
                score,
                signal_count,
                prematch,
                period,
                followed_upcoming,
                reference_used,
                reference_total,
                effective_threshold,
                market_future_pace,
                future_pace_lower,
                future_pace_upper,
                fair_total,
            ):
                return {"recipient": 1}

        self.notifier = StrictNotifier()
        snapshots = [
            {
                "elapsed_game_seconds": 600,
                "total_score": 44,
                "live_total": 170,
            },
            {
                "elapsed_game_seconds": 720,
                "total_score": 54,
                "live_total": 172,
            },
        ]
        for snapshot in snapshots:
            self.db.save_snapshot_if_changed(
                match_id="test-match",
                period=2,
                game_clock="05:00",
                remaining_minutes=20,
                home_score=snapshot["total_score"] // 2,
                away_score=snapshot["total_score"] - snapshot["total_score"] // 2,
                pregame_total=165,
                heartbeat_seconds=0,
                **snapshot,
            )

        config = Config()
        config.MIN_ELAPSED_MINUTES = 0
        config.MIN_VALID_FUTURE_PACES = 1
        config.MIN_EDGE_POINTS = 0
        config.MIN_EDGE_RATIO = 0
        self.process(match(score="40 - 35", inplay_total=130), config)

        row = self.db.get_alert(1)
        self.assertIsNotNone(row)
        self.assertEqual(row["telegram_status"], "sent")
