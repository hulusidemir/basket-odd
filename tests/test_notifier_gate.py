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
                "signal_quality": {
                    "quality_score": 82,
                    "quality_label": "GÜÇLÜ",
                },
                "signal_gate": gate,
            },
            "period": 2,
        }

    async def test_shadow_signal_is_sent_with_score_and_wait_advice(self):
        notifier = self.notifier()
        result = await notifier.send_alert(
            **self.kwargs({"state": "SHADOW", "telegram_allowed": False})
        )
        self.assertEqual(result, {"chat": 1})
        text = notifier._send_to_all.await_args.args[0]
        self.assertIn("Sinyal skoru: 82/100", text)
        self.assertIn("Güven: GÜÇLÜ · Beklemek daha iyi olur", text)
        self.assertNotIn("PAS", text)
        self.assertNotIn("TEST", text)
        self.assertNotIn("ONAY", text)

    async def test_missing_gate_is_sent_with_cautious_advice(self):
        notifier = self.notifier()
        result = await notifier.send_alert(**self.kwargs({}))
        self.assertEqual(result, {"chat": 1})
        text = notifier._send_to_all.await_args.args[0]
        self.assertIn("Sinyal skoru: 82/100", text)
        self.assertIn("Temkinli değerlendirin", text)

    async def test_blocked_signal_is_sent_with_score_instead_of_pass(self):
        notifier = self.notifier()
        result = await notifier.send_alert(
            **self.kwargs({"state": "BLOCKED", "telegram_allowed": False})
        )
        self.assertEqual(result, {"chat": 1})
        text = notifier._send_to_all.await_args.args[0]
        self.assertIn("Sinyal skoru: 82/100", text)
        self.assertIn("Güven: GÜÇLÜ · Beklemek daha iyi olur", text)
        self.assertNotIn("PAS", text)
        self.assertNotIn("Güven skoru:</b>", text)

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
        self.assertIn("Güven: GÜÇLÜ · Oynanabilir", text)
        self.assertNotIn("ONAYLI", text)
        self.assertIn("70.0", text)

    async def test_startup_message_describes_score_based_format(self):
        notifier = self.notifier()

        await notifier.send_startup()

        text = notifier._send_to_all.await_args.args[0]
        self.assertIn("sinyal skoru, güven seviyesi ve oynanabilirlik yorumuyla", text)
        self.assertNotIn("PAS, TEST", text)


if __name__ == "__main__":
    unittest.main()
