import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from db import Database
from config import Config
from live_signals import SignalDecision
from main import (
    _deliver_signal, _run_live_loop, _telegram_outbox_worker,
    retry_pending_telegram_deliveries, run,
)
from tests.market_fixture import verified_payload


class TelegramOutboxTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp_dir.name) / "test.db"))
        self.db.init()
        self.alert_id = self.db.save_alert(
            "match-1",
            "Home - Away",
            160,
            170,
            "ALT",
            10,
            tournament="FIBA",
            status="Q2 05:00",
            score="40 - 35",
            telegram_required=True,
            quality_score=80,
            market_provenance=verified_payload({
                "match_id": "match-1", "opening_total": 160, "inplay_total": 170,
                "status": "Q2 05:00", "score": "40 - 35",
            })["market_provenance"],
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_pending_delivery_is_retried_and_marked_sent(self):
        notifier = type("Notifier", (), {})()
        notifier.send_alert = AsyncMock(return_value={"chat": 123})

        summary = asyncio.run(
            retry_pending_telegram_deliveries(self.db, notifier)
        )

        self.assertEqual(
            summary,
            {"pending": 1, "sent": 1, "failed": 0, "cancelled": 0},
        )
        self.assertEqual(self.db.pending_telegram_alerts(), [])
        row = self.db.get_alert(self.alert_id)
        self.assertEqual(row["telegram_status"], "sent")
        self.assertEqual(json.loads(row["telegram_message_ids"]), {"chat": 123})

    def test_failed_delivery_remains_retryable(self):
        notifier = type("Notifier", (), {})()
        notifier.send_alert = AsyncMock(return_value={})

        summary = asyncio.run(
            retry_pending_telegram_deliveries(self.db, notifier)
        )

        self.assertEqual(
            summary,
            {"pending": 1, "sent": 0, "failed": 1, "cancelled": 0},
        )
        row = self.db.get_alert(self.alert_id)
        self.assertEqual(row["telegram_status"], "retry")
        self.assertEqual(row["telegram_retry_count"], 1)
        self.assertNotIn("chat", row["telegram_last_error"].lower())

    def test_retry_line_movement_does_not_assume_direction_matches_movement(self):
        # A Future Pace OVER can be above the pregame line; movement is still +10.
        with self.db._conn() as conn:
            conn.execute("UPDATE alerts SET direction='ÜST' WHERE id=?", (self.alert_id,))
        notifier = type('Notifier', (), {'send_alert': AsyncMock(return_value={'chat': 123})})()
        summary = asyncio.run(retry_pending_telegram_deliveries(self.db, notifier))
        self.assertEqual(summary['sent'], 1)
        self.assertEqual(notifier.send_alert.call_args.args[4], 'ÜST')
        self.assertEqual(notifier.send_alert.call_args.args[5], 10)

    def test_retry_during_slow_scan_recovers_initial_timeout_without_duplicate_send(self):
        async def scenario():
            started = asyncio.Event()
            release = asyncio.Event()
            delivered = asyncio.Event()
            attempts = 0
            in_flight = set()
            match = verified_payload({
                "match_id": "match-1", "match_name": "Home - Away", "tournament": "FIBA",
                "opening_total": 160, "prematch_total": None, "inplay_total": 170,
                "status": "Q2 05:00", "score": "40 - 35",
            })
            decision = SignalDecision("opening", 160, 10, 0, "ALT", 2)

            async def send(*args, **kwargs):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    started.set()
                    await release.wait()
                    raise TimeoutError()
                delivered.set()
                return {"chat": 123}

            notifier = type("Notifier", (), {"send_alert": AsyncMock(side_effect=send)})()
            async def slow_scan(**kwargs):
                await asyncio.Event().wait()

            scraper = type("Scraper", (), {
                "get_live_basketball_totals": AsyncMock(side_effect=slow_scan),
            })()
            # Keep a real live scan waiting while initial delivery and retries run.
            scan = asyncio.create_task(_run_live_loop(Config(), self.db, notifier, scraper, in_flight))
            initial = asyncio.create_task(_deliver_signal(
                match, decision, 1, self.alert_id, self.db, notifier, Config(), in_flight,
            ))
            worker = None
            try:
                await asyncio.wait_for(started.wait(), 1)
                self.assertEqual(in_flight, {self.alert_id})
                summary = await retry_pending_telegram_deliveries(
                    self.db, notifier, in_flight_alert_ids=in_flight,
                )
                self.assertEqual(summary["pending"], 0)
                self.assertEqual(attempts, 1)
                release.set()
                with self.assertRaises(TimeoutError):
                    await initial
                self.assertEqual(in_flight, set())
                worker = asyncio.create_task(_telegram_outbox_worker(
                    self.db, notifier, in_flight, interval=0.01,
                ))
                await asyncio.wait_for(delivered.wait(), 1)
                self.assertFalse(scan.done())
                self.assertEqual(attempts, 2)
                self.assertEqual(self.db.get_alert(self.alert_id)["telegram_status"], "sent")
            finally:
                tasks = [task for task in (scan, initial, worker) if task is not None]
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

        asyncio.run(scenario())

    def test_run_cancellation_stops_outbox_and_closes_scraper(self):
        async def scenario():
            config = Config()
            config.validate = lambda: None
            scraper = type("Scraper", (), {"close": AsyncMock()})()
            notifier = type("Notifier", (), {"send_startup": AsyncMock()})()
            worker_started = asyncio.Event()
            worker_stopped = asyncio.Event()

            async def worker(*args):
                worker_started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    worker_stopped.set()

            async def scan(*args):
                await asyncio.Event().wait()

            with patch("main.Config", return_value=config), \
                    patch("main.Database", return_value=self.db), \
                    patch("main.TelegramNotifier", return_value=notifier), \
                    patch("main.AiscoreScraper", return_value=scraper), \
                    patch("main._telegram_outbox_worker", side_effect=worker), \
                    patch("main._run_live_loop", side_effect=scan), \
                    patch("main.asyncio.create_task", wraps=asyncio.create_task) as create_task:
                task = asyncio.create_task(run())
                await asyncio.wait_for(worker_started.wait(), 1)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertTrue(worker_stopped.is_set())
                scraper.close.assert_awaited_once()
                # The run task and Telegram outbox are the only spawned tasks.
                self.assertEqual(create_task.call_count, 2)

        asyncio.run(scenario())

    def test_fresh_low_quality_signal_remains_retryable(self):
        with self.db._conn() as conn:
            conn.execute("UPDATE alerts SET quality_score=0 WHERE id=?", (self.alert_id,))
        notifier = type("Notifier", (), {"send_alert": AsyncMock(return_value={"chat": 123})})()
        summary = asyncio.run(retry_pending_telegram_deliveries(self.db, notifier))
        self.assertEqual(summary["sent"], 1)
        self.assertEqual(self.db.get_alert(self.alert_id)["live"], 170)

    def test_legacy_or_expired_source_is_cancelled_without_changing_saved_line(self):
        for proof in (None, '{"version":"bet365_history_v1","verified":true,"provider_updated_at":1}'):
            with self.db._conn() as conn:
                conn.execute("UPDATE alerts SET market_provenance_json=?, telegram_status='pending' WHERE id=?",
                             (proof, self.alert_id))
            notifier = type("Notifier", (), {"send_alert": AsyncMock()})()
            summary = asyncio.run(retry_pending_telegram_deliveries(self.db, notifier))
            assert summary["cancelled"] == 1
            notifier.send_alert.assert_not_awaited()
            assert self.db.get_alert(self.alert_id)["live"] == 170

    def test_partial_delivery_retries_only_missing_recipient(self):
        self.db.mark_telegram_delivery_failed(
            self.alert_id,
            "partial",
            message_ids={"recipient-a": 111},
        )
        notifier = type("Notifier", (), {})()
        notifier.recipient_keys = {"recipient-a", "recipient-b"}
        notifier.delivery_complete = lambda ids: notifier.recipient_keys.issubset(ids)
        notifier.send_alert = AsyncMock(return_value={"recipient-b": 222})

        summary = asyncio.run(retry_pending_telegram_deliveries(self.db, notifier))

        self.assertEqual(
            summary,
            {"pending": 1, "sent": 1, "failed": 0, "cancelled": 0},
        )
        self.assertEqual(
            notifier.send_alert.await_args.kwargs["pending_recipient_keys"],
            {"recipient-b"},
        )
        row = self.db.get_alert(self.alert_id)
        self.assertEqual(row["telegram_status"], "sent")
        self.assertEqual(
            json.loads(row["telegram_message_ids"]),
            {"recipient-a": 111, "recipient-b": 222},
        )

    def test_blacklisted_pending_delivery_is_cancelled_without_sending(self):
        self.db.add_signal_list_entry("black", "team", "Home")
        notifier = type("Notifier", (), {})()
        notifier.send_alert = AsyncMock(return_value={"chat": 123})

        summary = asyncio.run(retry_pending_telegram_deliveries(self.db, notifier))

        self.assertEqual(
            summary,
            {"pending": 1, "sent": 0, "failed": 0, "cancelled": 1},
        )
        notifier.send_alert.assert_not_awaited()
        row = self.db.get_alert(self.alert_id)
        self.assertEqual(row["telegram_status"], "cancelled")
        self.assertIn("team=Home", row["telegram_last_error"])

    def test_retry_exhaustion_becomes_explicit_failure(self):
        for _ in range(8):
            self.db.mark_telegram_delivery_failed(self.alert_id, "still failing")

        row = self.db.get_alert(self.alert_id)
        self.assertEqual(row["telegram_status"], "failed")
        self.assertEqual(row["telegram_retry_count"], 8)
        self.assertEqual(self.db.pending_telegram_alerts(), [])


if __name__ == "__main__":
    unittest.main()
