import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from main import _run_live_loop


class LiveRuntimeTests(unittest.TestCase):
    def test_match_failure_uses_short_poll_but_session_failure_backs_off(self):
        cases = (
            ({"status": "partial", "session_reset_required": False}, 4),
            ({"status": "partial", "session_reset_required": True}, 25),
            ({"status": "continuing", "session_reset_required": False}, 4),
        )
        for health, expected in cases:
            with self.subTest(health=health):
                config = SimpleNamespace(LIVE_SCRAPE_TIMEOUT_SECONDS=60,
                                         LIVE_POLL_SECONDS=4,
                                         POLL_INTERVAL_MIN=25, POLL_INTERVAL_MAX=25)
                db = MagicMock()
                db.list_signal_list_entries.return_value = []
                scraper = SimpleNamespace(last_report=health,
                                          get_live_basketball_totals=AsyncMock(return_value=[]))
                notifier = SimpleNamespace(send_error=AsyncMock())
                with patch("main.asyncio.sleep", new=AsyncMock(side_effect=asyncio.CancelledError)) as sleep:
                    with self.assertRaises(asyncio.CancelledError):
                        asyncio.run(_run_live_loop(config, db, notifier, scraper, set()))
                sleep.assert_awaited_once_with(expected)

    def test_listing_exception_uses_backoff(self):
        config = SimpleNamespace(LIVE_SCRAPE_TIMEOUT_SECONDS=60,
                                 LIVE_POLL_SECONDS=4,
                                 POLL_INTERVAL_MIN=25, POLL_INTERVAL_MAX=25)
        db = MagicMock()
        db.list_signal_list_entries.return_value = []
        scraper = SimpleNamespace(get_live_basketball_totals=AsyncMock(side_effect=RuntimeError("listing unavailable")))
        notifier = SimpleNamespace(send_error=AsyncMock())
        with patch("main.asyncio.sleep", new=AsyncMock(side_effect=asyncio.CancelledError)) as sleep:
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(_run_live_loop(config, db, notifier, scraper, set()))
        sleep.assert_awaited_once_with(25)
