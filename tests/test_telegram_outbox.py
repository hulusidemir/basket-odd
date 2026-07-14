import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from db import Database
from main import retry_pending_telegram_deliveries


class TelegramOutboxTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp_dir.name) / "test.db"))
        self.db.init()
        analysis = {
            "direction": "ALT",
            "final_direction": "ALT",
            "signal_gate": {
                "state": "BLOCKED",
                "telegram_allowed": False,
            },
        }
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
            ai_analysis=json.dumps(analysis),
            telegram_required=True,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_pending_delivery_is_retried_and_marked_sent(self):
        notifier = type("Notifier", (), {})()
        notifier.send_alert = AsyncMock(return_value={"chat": 123})

        summary = asyncio.run(
            retry_pending_telegram_deliveries(self.db, notifier)
        )

        self.assertEqual(summary, {"pending": 1, "sent": 1, "failed": 0})
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

        self.assertEqual(summary, {"pending": 1, "sent": 0, "failed": 1})
        row = self.db.get_alert(self.alert_id)
        self.assertEqual(row["telegram_status"], "retry")
        self.assertEqual(row["telegram_retry_count"], 1)
        self.assertNotIn("chat", row["telegram_last_error"].lower())

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

        self.assertEqual(summary, {"pending": 1, "sent": 1, "failed": 0})
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

    def test_retry_exhaustion_becomes_explicit_failure(self):
        for _ in range(8):
            self.db.mark_telegram_delivery_failed(self.alert_id, "still failing")

        row = self.db.get_alert(self.alert_id)
        self.assertEqual(row["telegram_status"], "failed")
        self.assertEqual(row["telegram_retry_count"], 8)
        self.assertEqual(self.db.pending_telegram_alerts(), [])


if __name__ == "__main__":
    unittest.main()
