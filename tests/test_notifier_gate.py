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
                    "stars": 4,
                },
                "signal_gate": gate,
            },
            "period": 2,
        }

    async def test_legacy_gate_does_not_add_playability_advice(self):
        notifier = self.notifier()
        result = await notifier.send_alert(
            **self.kwargs({"state": "SHADOW", "telegram_allowed": False})
        )
        self.assertEqual(result, {"chat": 1})
        text = notifier._send_to_all.await_args.args[0]
        self.assertIn("ALT · 82 ★★★★ · GÜÇLÜ", text)
        self.assertNotIn("Beklemek daha iyi olur", text)
        self.assertNotIn("<b>Kanıt:</b>", text)
        self.assertNotIn("PAS", text)
        self.assertNotIn("TEST", text)
        self.assertNotIn("ONAY", text)

    async def test_missing_gate_is_sent_with_score(self):
        notifier = self.notifier()
        result = await notifier.send_alert(**self.kwargs({}))
        self.assertEqual(result, {"chat": 1})
        text = notifier._send_to_all.await_args.args[0]
        self.assertIn("ALT · 82 ★★★★ · GÜÇLÜ", text)
        self.assertNotIn("Temkinli değerlendirin", text)

    async def test_blocked_legacy_state_is_not_shown(self):
        notifier = self.notifier()
        result = await notifier.send_alert(
            **self.kwargs({"state": "BLOCKED", "telegram_allowed": False})
        )
        self.assertEqual(result, {"chat": 1})
        text = notifier._send_to_all.await_args.args[0]
        self.assertIn("ALT · 82 ★★★★ · GÜÇLÜ", text)
        self.assertNotIn("Beklemek daha iyi olur", text)
        self.assertNotIn("PAS", text)
        self.assertNotIn("Güven skoru:</b>", text)

    async def test_trusted_legacy_state_does_not_change_message(self):
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
        self.assertIn("ALT · 82 ★★★★ · GÜÇLÜ", text)
        self.assertNotIn("Oynanabilir", text)
        self.assertNotIn("ONAYLI", text)
        self.assertNotIn("<b>Kanıt:</b>", text)

    async def test_startup_message_describes_simple_format(self):
        notifier = self.notifier()

        await notifier.send_startup()

        text = notifier._send_to_all.await_args.args[0]
        self.assertIn("sinyal skoru ve kısa gerekçesiyle", text)
        self.assertNotIn("PAS, TEST", text)


if __name__ == "__main__":
    unittest.main()
