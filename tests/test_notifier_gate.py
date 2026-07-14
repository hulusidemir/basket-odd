import unittest
from unittest.mock import AsyncMock

from notifier import TelegramNotifier


class NotifierGateTests(unittest.IsolatedAsyncioTestCase):
    def notifier(self):
        notifier = object.__new__(TelegramNotifier)
        notifier._send_to_all = AsyncMock(return_value={"chat": 1})
        return notifier

    def kwargs(self, gate):
        return {
            "match_name": "Home - Away",
            "tournament": "FIBA",
            "opening": 160,
            "live": 170,
            "direction": "ALT",
            "diff": 10,
            "status": "Q2 05:00",
            "score": "40 - 35",
            "analysis": {
                "final_direction": "ALT",
                "selection_reason": "test",
                "signal_gate": gate,
            },
            "period": 2,
        }

    async def test_shadow_signal_never_calls_telegram(self):
        notifier = self.notifier()
        result = await notifier.send_alert(
            **self.kwargs({"state": "SHADOW", "telegram_allowed": False})
        )
        self.assertEqual(result, {})
        notifier._send_to_all.assert_not_awaited()

    async def test_missing_gate_never_calls_telegram(self):
        notifier = self.notifier()
        result = await notifier.send_alert(**self.kwargs({}))
        self.assertEqual(result, {})
        notifier._send_to_all.assert_not_awaited()

    async def test_trusted_signal_is_sent(self):
        notifier = self.notifier()
        result = await notifier.send_alert(
            **self.kwargs(
                {
                    "state": "TRUSTED",
                    "telegram_allowed": True,
                    "evidence": {"resolved_unique": 100, "wilson_low_95": 70.02},
                }
            )
        )
        self.assertEqual(result, {"chat": 1})
        notifier._send_to_all.assert_awaited_once()
        text = notifier._send_to_all.await_args.args[0]
        self.assertIn("ONAYLI", text)
        self.assertIn("70.0", text)


if __name__ == "__main__":
    unittest.main()
