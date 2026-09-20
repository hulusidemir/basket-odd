import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from aiscore_scraper import (
    AiscoreScraper,
    _MatchSkip,
    _detail_status_from_top_text,
    _normalize_market_snapshot,
    _redact_proxy_url,
    _select_market_line,
    _status_from_play_by_play_hint,
)


class AiscoreScraperTests(unittest.TestCase):
    def test_free_slot_starts_next_match_before_slow_match_finishes(self):
        async def scenario():
            scraper = AiscoreScraper("url", concurrency=2)
            release = asyncio.Event()
            events = []

            async def extract(context, link):
                events.append(link)
                if link == "slow":
                    await release.wait()
                if link == "third":
                    release.set()
                return {"match_id": link}

            scraper._extract_single_with_timeout = extract
            rows = [row async for row in scraper._detail_results(None, ["slow", "fast", "third"])]
            self.assertEqual(len(rows), 3)
            self.assertEqual(events, ["slow", "fast", "third"])

        async def bounded():
            await asyncio.wait_for(scenario(), 1)

        asyncio.run(bounded())

    def test_persistent_session_reused_and_cancelled_session_closed(self):
        async def scenario():
            scraper = AiscoreScraper("url", persistent_session=True)
            session = MagicMock()
            session.__aenter__ = AsyncMock(return_value=session)
            session.__aexit__ = AsyncMock()
            with patch("aiscore_scraper.AsyncStealthySession", return_value=session) as factory:
                async with scraper._live_session():
                    pass
                async with scraper._live_session():
                    pass
                factory.assert_called_once()
                session.__aexit__.assert_not_awaited()
                with self.assertRaises(asyncio.CancelledError):
                    async with scraper._live_session():
                        raise asyncio.CancelledError()
                session.__aexit__.assert_awaited_once()
                self.assertIsNone(scraper._session)
                async with scraper._live_session():
                    pass
                self.assertEqual(factory.call_count, 2)
                await scraper.close()

        asyncio.run(scenario())

    def test_scrapling_session_enables_protection_and_proxy(self):
        scraper = AiscoreScraper("https://m.aiscore.com/basketball", concurrency=4)
        with patch.dict("os.environ", {"PLAYWRIGHT_PROXY": "socks5://localhost:9050"}):
            options = scraper._scrapling_session_options()

        self.assertEqual(options["proxy"], "socks5://localhost:9050")
        self.assertTrue(options["solve_cloudflare"])
        self.assertTrue(options["block_webrtc"])
        self.assertGreaterEqual(options["timeout"], 90000)
        self.assertEqual(options["max_pages"], 4)
        self.assertTrue(options["user_data_dir"].endswith("scrapling-profile"))

    def test_scraped_matches_are_emitted_before_the_next_batch(self):
        scraper = AiscoreScraper(
            "https://m.aiscore.com/basketball",
            concurrency=1,
        )
        scraper._last_listing_diagnostics = {"live_tab_count_known": False}
        scraper._wait_for_listing_ready = AsyncMock()
        scraper._collect_match_links = AsyncMock(return_value=["first", "second"])
        events = []

        async def extract(_context, link):
            events.append(f"extract:{link}")
            return {"match_id": link}

        async def emit(match):
            events.append(f"emit:{match['match_id']}")

        scraper._extract_single = extract
        page = MagicMock()
        page.set_default_timeout = MagicMock()

        class Session:
            context = MagicMock()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def fetch(self, url, **kwargs):
                await kwargs["page_action"](page)
                return MagicMock()

        with patch("aiscore_scraper.AsyncStealthySession", return_value=Session()):
            result = asyncio.run(scraper.get_live_basketball_totals(on_match=emit))

        self.assertEqual([row["match_id"] for row in result], ["first", "second"])
        self.assertEqual(
            events,
            ["extract:first", "emit:first", "extract:second", "emit:second"],
        )
        self.assertEqual(scraper.last_report["emitted_count"], 2)

    def test_cycle_cancellation_collects_pending_match_tasks(self):
        scraper = AiscoreScraper(
            "https://m.aiscore.com/basketball",
            concurrency=1,
        )
        scraper._last_listing_diagnostics = {"live_tab_count_known": False}
        scraper._wait_for_listing_ready = AsyncMock()
        scraper._collect_match_links = AsyncMock(return_value=["hung"])
        cancelled = False

        async def extract(_context, _link):
            nonlocal cancelled
            try:
                await asyncio.Event().wait()
            finally:
                cancelled = True

        scraper._extract_single_with_timeout = extract
        page = MagicMock()
        page.set_default_timeout = MagicMock()

        class Session:
            context = MagicMock()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def fetch(self, url, **kwargs):
                await kwargs["page_action"](page)
                return MagicMock()

        async def cancel_cycle():
            task = asyncio.create_task(scraper.get_live_basketball_totals())
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        with patch("aiscore_scraper.AsyncStealthySession", return_value=Session()):
            asyncio.run(cancel_cycle())

        self.assertTrue(cancelled)

    def test_unchanged_line_is_rejected_after_score_and_clock_advance(self):
        scraper = AiscoreScraper(
            "https://m.aiscore.com/basketball",
            stale_line_seconds=90,
            stale_score_delta=10,
            stale_game_minutes=2,
        )
        first = {
            "match_id": "m1", "match_name": "A - B", "tournament": "League",
            "bookmaker": "Book A", "inplay_total": 175.5,
            "score": "32 - 30", "status": "Q2 06:37",
            "_market_captured_monotonic": 100.0,
        }
        advanced = {
            **first,
            "score": "40 - 34", "status": "Q2 03:30",
            "_market_captured_monotonic": 195.0,
        }

        self.assertIsNone(scraper._stale_line_skip(first))
        result = scraper._stale_line_skip(advanced)
        self.assertIsInstance(result, _MatchSkip)
        self.assertEqual(result.reason, "stale_inplay_total")

    def test_changed_line_resets_stale_observation_guard(self):
        scraper = AiscoreScraper("https://m.aiscore.com/basketball")
        first = {
            "match_id": "m1", "match_name": "A - B", "tournament": "League",
            "bookmaker": "Book A", "inplay_total": 175.5,
            "score": "32 - 30", "status": "Q2 06:37",
            "_market_captured_monotonic": 100.0,
        }
        changed = {
            **first,
            "inplay_total": 171.5, "score": "65 - 75", "status": "Q4 05:58",
            "_market_captured_monotonic": 300.0,
        }
        self.assertIsNone(scraper._stale_line_skip(first))
        self.assertIsNone(scraper._stale_line_skip(changed))

    def test_concurrency_backs_off_and_recovers_gradually(self):
        scraper = AiscoreScraper(
            "https://m.aiscore.com/basketball",
            concurrency=4,
        )
        self.assertEqual(scraper._effective_concurrency, 2)

        scraper._reduce_concurrency()
        self.assertEqual(scraper._effective_concurrency, 1)

        scraper._record_healthy_concurrency_cycle()
        scraper._record_healthy_concurrency_cycle()
        self.assertEqual(scraper._effective_concurrency, 1)
        scraper._record_healthy_concurrency_cycle()
        self.assertEqual(scraper._effective_concurrency, 2)

    def test_hung_match_is_bounded_and_reported_as_degraded_skip(self):
        scraper = AiscoreScraper("https://m.aiscore.com/basketball")
        scraper.match_timeout_seconds = 0.01
        cancelled = False

        async def extract(_context, _link):
            nonlocal cancelled
            try:
                await asyncio.Event().wait()
            finally:
                cancelled = True

        scraper._extract_single = extract
        result = asyncio.run(
            scraper._extract_single_with_timeout(MagicMock(), "hung")
        )

        self.assertIsInstance(result, _MatchSkip)
        self.assertEqual(result.reason, "match_timeout")
        self.assertTrue(result.degraded)
        self.assertTrue(cancelled)

    def test_navigation_retry_delay_does_not_use_browser_timer(self):
        page = MagicMock()
        page.goto = AsyncMock(side_effect=[RuntimeError("TimeoutError"), "ok"])
        page.wait_for_timeout = AsyncMock(side_effect=AssertionError("must not be used"))
        scraper = AiscoreScraper("https://m.aiscore.com/basketball")

        with patch("aiscore_scraper.asyncio.sleep", new=AsyncMock()) as sleep:
            result = asyncio.run(scraper._goto_detail_with_retry(page, "url"))

        self.assertEqual(result, "ok")
        sleep.assert_awaited_once_with(0.35)
        page.wait_for_timeout.assert_not_awaited()

    def test_challenge_timeout_is_reported_as_access_failure(self):
        scraper = AiscoreScraper("https://m.aiscore.com/basketball", page_timeout_ms=30000)
        page = MagicMock()
        page.wait_for_function = AsyncMock(side_effect=TimeoutError())
        page.title = AsyncMock(return_value="Just a moment...")
        with self.assertRaisesRegex(RuntimeError, "access verification"):
            asyncio.run(scraper._wait_for_listing_ready(page))
        self.assertEqual(page.wait_for_function.call_args.kwargs["timeout"], 30000)

    def test_empty_listing_requires_explicit_source_confirmation(self):
        cases = (
            ({"live_tab_reported_count": 4}, "no verified links", 3),
            ({}, "not explicitly verified", 3),
            ({"authoritative_empty": True, "live_tab_reported_count": 0}, None, 1),
        )
        for diagnostics, error, attempts in cases:
            with self.subTest(diagnostics=diagnostics):
                scraper = AiscoreScraper("https://m.aiscore.com/basketball")
                scraper._last_listing_diagnostics = diagnostics
                page = MagicMock()
                page.goto = AsyncMock()
                page.close = AsyncMock()
                page.is_closed.return_value = False
                page.title = AsyncMock(return_value="Basketball")
                page.evaluate = AsyncMock(return_value=100)
                context = MagicMock()
                scraper._wait_for_listing_ready = AsyncMock()
                scraper._collect_match_links = AsyncMock(return_value=[])

                class Session:
                    def __init__(self):
                        self.context = context
                        self.fetch_count = 0

                    async def __aenter__(self):
                        return self

                    async def __aexit__(self, *args):
                        return False

                    async def fetch(self, url, **kwargs):
                        self.fetch_count += 1
                        await kwargs["page_action"](page)
                        return MagicMock()

                session = Session()
                with patch("aiscore_scraper.AsyncStealthySession", return_value=session):
                    if error:
                        with self.assertRaisesRegex(RuntimeError, error):
                            asyncio.run(scraper.get_live_basketball_totals())
                        self.assertEqual(scraper.last_report["status"], "error")
                    else:
                        self.assertEqual(asyncio.run(scraper.get_live_basketball_totals()), [])
                        self.assertEqual(scraper.last_report["status"], "empty")
                self.assertEqual(session.fetch_count, attempts)
                self.assertEqual(scraper.last_report["unverified_count"], diagnostics.get("live_tab_reported_count", 0))

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
        })
        self.assertEqual(_select_market_line(snapshot, "opening"), 160)
        self.assertEqual(_select_market_line(snapshot, "inplay"), 170)

    def test_missing_market_lines_are_not_filled_from_unrelated_fields(self):
        snapshot = _normalize_market_snapshot({
            "opening_lines": [None, "invalid", 50],
            "inplay_lines": [],
            "prematch_lines": [161, 165, 165],
        })
        self.assertIsNone(_select_market_line(snapshot, "opening"))
        self.assertIsNone(_select_market_line(snapshot, "inplay"))
        self.assertIsNone(_select_market_line(snapshot, "prematch"))

    def test_prematch_uses_selected_bookmaker_not_median(self):
        snapshot = _normalize_market_snapshot({
            "opening_lines": [160, 180, 180],
            "inplay_lines": [175, 190, 190],
            "prematch_lines": [162, 185, 185],
        })
        self.assertEqual(_select_market_line(snapshot, "prematch"), 162)

    def test_missing_prematch_does_not_borrow_next_bookmaker(self):
        snapshot = _normalize_market_snapshot({
            "opening_lines": [160, 180], "inplay_lines": [175, 190],
            "prematch_lines": [None, 185],
        })
        self.assertIsNone(_select_market_line(snapshot, "prematch"))

    def test_invalid_paired_row_is_skipped_as_a_whole(self):
        snapshot = _normalize_market_snapshot({
            "opening_lines": [160, 180], "inplay_lines": [None, 190],
            "prematch_lines": [162, 185],
        })
        self.assertEqual(_select_market_line(snapshot, "opening"), 180)
        self.assertEqual(_select_market_line(snapshot, "inplay"), 190)
        self.assertEqual(_select_market_line(snapshot, "prematch"), 185)

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
        self.assertEqual(page.waits, [])

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
