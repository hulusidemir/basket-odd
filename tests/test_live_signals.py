import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from config import Config
from db import Database
from live_signals import evaluate_live_signal
from main import _normalize_match_payload, process_match, retry_pending_telegram_deliveries
from match_state import game_clock
from pace_calculator import chronological_snapshots


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

    def test_q1_end_to_q2_start_keeps_equal_elapsed_snapshots(self):
        for tournament, quarter_minutes in (("FIBA Intercontinental Cup", 10), ("NBA", 12)):
            with self.subTest(tournament=tournament):
                match_id = f"quarter-boundary-{quarter_minutes}"
                payload = match(
                    match_id=match_id, match_name="NBA G League United - Club",
                    tournament=tournament, score="20 - 20", inplay_total=160,
                )
                self.process({**payload, "status": "Q1 00:00"})
                self.process({**payload, "status": f"Q2 {quarter_minutes:02d}:00"})

                snapshots = self.db.get_match_snapshots(match_id)
                elapsed = quarter_minutes * 60
                self.assertEqual([row["period"] for row in snapshots], [1, 2])
                self.assertEqual([row["elapsed_game_seconds"] for row in snapshots], [elapsed, elapsed])
                self.assertEqual(game_clock(f"Q2 {quarter_minutes:02d}:00", payload["match_name"], tournament)["period"], 2)
                self.assertEqual(
                    chronological_snapshots(
                        snapshots,
                        {"elapsed_game_seconds": elapsed, "total_score": 40, "period": 2},
                    ),
                    snapshots,
                )

    def test_runtime_48_format_freezes_from_existing_snapshot_after_restart(self):
        payload = match(tournament="Club Friendship", score="30 - 30", status="Q2 11:34")
        with self.assertLogs("main", level="WARNING") as logs:
            self.process(payload)
        self.assertTrue(any("DURATION_FORMAT_OVERRIDE" in line and "configured=40 observed=48" in line
                            for line in logs.output))
        first = self.db.get_match_snapshots("test-match")
        self.assertEqual((first[0]["game_clock"], first[0]["elapsed_game_seconds"],
                          first[0]["remaining_minutes"]), ("11:34", 746, 48 - 746 / 60))

        self.db = Database(self.db.db_path)
        self.process({**payload, "status": "Q3 08:00", "score": "60 - 60"})
        snapshots = self.db.get_match_snapshots("test-match")
        self.assertEqual([row["elapsed_game_seconds"] for row in snapshots], [746, 1680])
        self.assertEqual(snapshots[-1]["remaining_minutes"], 20)
        self.assertEqual(self.db.count_match_alerts("test-match"), 0)

    def test_runtime_override_excludes_old_forty_minute_snapshots(self):
        payload = match(tournament="FIBA Intercontinental Cup", status="Q2 09:00", score="25 - 25")
        self.process(payload)
        with self.assertLogs("main", level="WARNING") as logs, patch(
            "main.evaluate_live_signal", wraps=evaluate_live_signal,
        ) as evaluate:
            self.process({**payload, "status": "Q3 10:45", "score": "45 - 45"})
        self.assertTrue(any("DURATION_FORMAT_OVERRIDE" in line for line in logs.output))
        self.assertEqual(len(evaluate.call_args.args[1]), 1)
        snapshots = self.db.get_match_snapshots("test-match")
        self.assertEqual([row["elapsed_game_seconds"] for row in snapshots], [660, 1515])
        self.assertEqual(snapshots[-1]["remaining_minutes"], 48 - 1515 / 60)
        self.assertEqual(self.db.count_match_alerts("test-match"), 0)

    def test_static_nba_and_ncaa_histories_are_not_reset_by_long_clocks(self):
        for tournament, first_status, next_status in (
            ("NBA", "Q2 12:00", "Q2 11:34"),
            ("NCAA", "Q2 12:00", "Q2 11:34"),
        ):
            with self.subTest(tournament=tournament):
                payload = match(match_id=tournament, tournament=tournament, score="30 - 30")
                self.process({**payload, "status": first_status})
                with patch("main.evaluate_live_signal", wraps=evaluate_live_signal) as evaluate:
                    self.process({**payload, "status": next_status, "score": "31 - 31"})
                self.assertEqual(len(evaluate.call_args.args[1]), 2)

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

    def test_regressed_clock_and_score_do_not_enter_signal_history(self):
        self.db.save_snapshot_if_changed(
            match_id="test-match", period=2, game_clock="", elapsed_game_seconds=900,
            remaining_minutes=25, home_score=40, away_score=35, total_score=75,
            pregame_total=165, live_total=180,
        )
        self.process(match(status="Q2 05:30", score="41 - 35"))
        self.process(match(status="Q2 04:30", score="39 - 35"))
        self.assertEqual(len(self.db.get_match_snapshots("test-match")), 2)
        self.assertEqual(self.db.count_match_alerts("test-match"), 0)
        self.notifier.send_alert.assert_not_awaited()

        self.process(match(status="Q2 04:00", score="41 - 37"))
        self.assertEqual(len(self.db.get_match_snapshots("test-match")), 3)

    def test_observation_captured_before_latest_snapshot_is_ignored(self):
        self.db.save_snapshot_if_changed(
            match_id="test-match", period=2, game_clock="", elapsed_game_seconds=900,
            remaining_minutes=25, home_score=40, away_score=35, total_score=75,
            pregame_total=165, live_total=180,
        )
        self.process(match(market_captured_at="2020-01-01T00:00:00+00:00"))
        self.assertEqual(self.db.count_match_alerts("test-match"), 0)
        self.assertEqual(len(self.db.get_match_snapshots("test-match")), 1)
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
