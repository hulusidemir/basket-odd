import asyncio
import unittest

from aiscore_scraper import (
    AiscoreScraper,
    _detail_status_from_top_text,
    _normalize_market_snapshot,
    _redact_proxy_url,
    _select_market_line,
    _status_from_play_by_play_hint,
)


class AiscoreScraperTests(unittest.TestCase):
    def test_mobile_url_is_forced(self):
        self.assertEqual(
            AiscoreScraper._mobile_url("https://www.aiscore.com/basketball/match-a/id/odds"),
            "https://m.aiscore.com/basketball/match-a/id/odds",
        )

    def test_play_by_play_hint_recovers_clock(self):
        result = _status_from_play_by_play_hint({"period": 2, "clock": "04:21"})
        self.assertEqual(result["status"], "Q2 04:21")

    def test_period_end_is_not_misclassified_as_full_time(self):
        for raw_status, expected in (
            ("Q1-Ended", "Q1-Ended"),
            ("2Q Ended", "Q2-Ended"),
            ("3rd Quarter Ended", "Q3-Ended"),
        ):
            with self.subTest(raw_status=raw_status):
                result = _detail_status_from_top_text(
                    f"Tabare 25 - 22 Borges {raw_status} Total Points"
                )
                self.assertEqual(result["status"], expected)
                self.assertFalse(result["is_finished"])
                self.assertTrue(result["period_ended"])

    def test_detail_status_distinguishes_live_clock_and_actual_final(self):
        live = _detail_status_from_top_text("Tabare 31 - 28 Borges Q2 04:21")
        self.assertEqual(live["status"], "Q2 04:21")
        self.assertFalse(live["is_finished"])

        for final_status in ("Full Time", "FT", "Finished", "Final", "Ended"):
            with self.subTest(final_status=final_status):
                result = _detail_status_from_top_text(
                    f"Tabare 85 - 87 Borges {final_status}"
                )
                self.assertTrue(result["is_finished"])
                self.assertEqual(result["status"], "Full Time")

    def test_first_paired_bookmaker_line_is_primary(self):
        snapshot = _normalize_market_snapshot({
            "opening_lines": [160, 180],
            "inplay_lines": [170, 190],
            "paired_bookmaker_count": 2,
        })
        self.assertEqual(_select_market_line({"opening": 160}, snapshot, "opening"), 160)
        self.assertEqual(_select_market_line({"inplay": 170}, snapshot, "inplay"), 170)

    def test_proxy_credentials_are_redacted(self):
        value = _redact_proxy_url("socks5://user:secret@127.0.0.1:9050")
        self.assertNotIn("user", value)
        self.assertNotIn("secret", value)
        self.assertIn("127.0.0.1:9050", value)

    def test_detail_navigation_retries_transient_connection_refusal(self):
        class Page:
            def __init__(self):
                self.calls = 0
                self.waits = []

            async def goto(self, url, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("Page.goto: NS_ERROR_CONNECTION_REFUSED")
                return "ok"

            async def wait_for_timeout(self, milliseconds):
                self.waits.append(milliseconds)

        scraper = AiscoreScraper("https://m.aiscore.com/basketball")
        page = Page()
        result = asyncio.run(scraper._goto_detail_with_retry(
            page,
            "https://m.aiscore.com/basketball/match-a/id/odds",
        ))

        self.assertEqual(result, "ok")
        self.assertEqual(page.calls, 2)
        self.assertEqual(page.waits, [350])

    def test_detail_navigation_exhaustion_returns_safe_error(self):
        class Page:
            def __init__(self):
                self.calls = 0

            async def goto(self, url, **kwargs):
                self.calls += 1
                raise RuntimeError(
                    "Page.goto: NS_ERROR_CONNECTION_REFUSED Call log with internal details"
                )

            async def wait_for_timeout(self, milliseconds):
                return None

        scraper = AiscoreScraper("https://m.aiscore.com/basketball")
        page = Page()
        with self.assertRaisesRegex(RuntimeError, "3 otomatik deneme") as raised:
            asyncio.run(scraper._goto_detail_with_retry(
                page,
                "https://m.aiscore.com/basketball/match-a/id/odds",
            ))

        self.assertEqual(page.calls, 3)
        self.assertNotIn("Call log", str(raised.exception))

if __name__ == "__main__":
    unittest.main()
