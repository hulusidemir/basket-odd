import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

from aiscore_scraper import (
    AiscoreScraper,
    _MatchSkip,
    _normalize_market_snapshot,
    _redact_proxy_url,
    _safe_env_int,
    _select_market_line,
)


class _LinkPage:
    def __init__(self, href_batches, *, live_count=3, live_found=True):
        self.url = "https://www.aiscore.com/basketball"
        self.href_batches = list(href_batches)
        self.live_count = live_count
        self.live_found = live_found

    async def wait_for_function(self, *_args, **_kwargs):
        return None

    async def wait_for_timeout(self, _milliseconds):
        return None

    async def evaluate(self, script):
        if "const topArea" in script:
            return {
                "url": self.url,
                "title": "Basketball",
                "tabCandidates": [],
                "matchLinkCount": self.live_count,
                "topHtml": "",
            }
        if "let tab = null" in script:
            return {
                "found": self.live_found,
                "tabText": f"Live ({self.live_count})",
                "count": self.live_count,
                "countKnown": self.live_found,
                "clicked": self.live_found,
            }
        if "const liveHrefs" in script:
            return self.href_batches.pop(0) if self.href_batches else []
        if script == "window.scrollBy(0, 600)":
            return None
        raise AssertionError(f"Unexpected evaluate call: {script[:80]}")


class _CyclePage:
    def __init__(self, goto_errors=None):
        self.url = "https://www.aiscore.com/basketball"
        self.closed = False
        self.goto_errors = list(goto_errors or [])
        self.goto_calls = []

    def set_default_timeout(self, _timeout):
        return None

    async def goto(self, url, **kwargs):
        self.goto_calls.append((url, kwargs))
        if self.goto_errors:
            error = self.goto_errors.pop(0)
            if error is not None:
                raise error

    async def wait_for_timeout(self, _milliseconds):
        return None

    async def title(self):
        return "Basketball"

    async def evaluate(self, _script):
        return 1000

    async def screenshot(self, **_kwargs):
        return None

    async def close(self):
        self.closed = True

    def is_closed(self):
        return self.closed


class _Context:
    def __init__(self, page=None):
        self.page = page
        self.closed = False

    async def new_page(self):
        return self.page

    async def close(self):
        self.closed = True


class _Browser:
    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


class _PlaywrightContextManager:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *_args):
        return None


class _DetailPage:
    def __init__(self, parsed):
        self.parsed = parsed
        self.evaluate_count = 0

    async def goto(self, *_args, **_kwargs):
        return None

    async def wait_for_function(self, *_args, **_kwargs):
        return None

    async def wait_for_timeout(self, _milliseconds):
        return None

    async def evaluate(self, _script):
        self.evaluate_count += 1
        if self.evaluate_count == 1:
            return False
        return self.parsed


class AiscoreScraperTests(unittest.TestCase):
    def test_first_readable_bookmaker_line_is_used_without_median_selection(self):
        parsed = {"inplay": 142.5}
        snapshot = {
            "inplay_lines": [142.5, 146.5, 148.5],
            "inplay_median": 146.5,
        }

        self.assertEqual(_select_market_line(parsed, snapshot, "inplay"), 142.5)

    def test_explicit_inplay_row_wins_over_wrong_positional_consensus(self):
        parsed = {"inplay": 142.5}
        snapshot = {"inplay_median": 157.5}

        self.assertEqual(_select_market_line(parsed, snapshot, "inplay"), 142.5)

    def test_proxy_credentials_are_redacted(self):
        redacted = _redact_proxy_url("http://alice:secret@example.test:8080")
        self.assertEqual(redacted, "http://***:***@example.test:8080")
        self.assertNotIn("alice", redacted)
        self.assertNotIn("secret", redacted)

        schemeless = _redact_proxy_url("alice:secret@example.test:8080")
        self.assertEqual(schemeless, "***:***@example.test:8080")
        self.assertNotIn("alice", schemeless)
        self.assertNotIn("secret", schemeless)

    def test_safe_concurrency_env_handles_invalid_and_out_of_range_values(self):
        with patch.dict(os.environ, {"TEST_CONCURRENCY": "broken"}):
            self.assertEqual(
                _safe_env_int("TEST_CONCURRENCY", 2, minimum=1, maximum=8),
                2,
            )
        with patch.dict(os.environ, {"TEST_CONCURRENCY": "99"}):
            self.assertEqual(
                _safe_env_int("TEST_CONCURRENCY", 2, minimum=1, maximum=8),
                8,
            )

    def test_market_consensus_preserves_duplicate_bookmaker_votes(self):
        snapshot = _normalize_market_snapshot(
            {
                "opening_lines": [160.5] * 8 + [170.5],
                "prematch_lines": [161.5, 161.5, 166.5],
                "inplay_lines": [175.5, 175.5, 181.5],
                "bookmaker_count": 9,
            }
        )
        self.assertEqual(len(snapshot["opening_lines"]), 9)
        self.assertEqual(snapshot["opening_median"], 160.5)
        self.assertEqual(snapshot["prematch_median"], 161.5)
        self.assertEqual(snapshot["inplay_median"], 175.5)
        self.assertEqual(snapshot["bookmaker_count"], 9)
        self.assertEqual(snapshot["paired_bookmaker_count"], 0)

    def test_explicit_paired_bookmaker_count_is_preserved(self):
        snapshot = _normalize_market_snapshot(
            {
                "opening_lines": [160.5, 161.5],
                "inplay_lines": [170.5, 171.5],
                "bookmaker_count": 2,
                "paired_bookmaker_count": 2,
            }
        )

        self.assertEqual(snapshot["paired_bookmaker_count"], 2)

    def test_ordered_live_only_link_collection_does_not_sort_or_fallback(self):
        page = _LinkPage(
            [
                [
                    "/basketball/match-b/2",
                    "/basketball/match-a/1",
                    "/basketball/match-b/2",
                ],
                ["/basketball/match-c/3", "/basketball/match-a/1"],
            ],
            live_count=3,
        )
        scraper = AiscoreScraper(page.url)
        links = asyncio.run(scraper._collect_match_links(page))
        self.assertEqual(
            links,
            [
                "https://www.aiscore.com/basketball/match-b/2",
                "https://www.aiscore.com/basketball/match-a/1",
                "https://www.aiscore.com/basketball/match-c/3",
            ],
        )

    def test_reported_count_exposes_unverified_live_links(self):
        page = _LinkPage(
            [["/basketball/match-a/1", "/basketball/match-b/2"], [], []],
            live_count=3,
        )
        scraper = AiscoreScraper(page.url)

        links = asyncio.run(scraper._collect_match_links(page))

        self.assertEqual(len(links), 2)
        self.assertEqual(
            scraper._last_listing_diagnostics["unverified_live_link_count"],
            1,
        )

    def test_reported_live_matches_without_verified_links_is_an_error(self):
        page = _LinkPage([[], [], []], live_count=2)
        scraper = AiscoreScraper(page.url)
        with self.assertRaisesRegex(RuntimeError, "verified zero count"):
            asyncio.run(scraper._collect_match_links(page))
        self.assertEqual(
            scraper._last_listing_diagnostics["verified_live_link_count"],
            0,
        )

    def test_explicit_live_zero_is_the_only_clean_empty_listing(self):
        page = _LinkPage([[], [], []], live_count=0)
        scraper = AiscoreScraper(page.url)

        links = asyncio.run(scraper._collect_match_links(page))

        self.assertEqual(links, [])
        self.assertTrue(scraper._last_listing_diagnostics["authoritative_empty"])

    def test_missing_live_tab_is_not_treated_as_clean_empty(self):
        page = _LinkPage([[]], live_count=0, live_found=False)
        scraper = AiscoreScraper(page.url)

        with self.assertRaisesRegex(RuntimeError, "Live tab could not be verified"):
            asyncio.run(scraper._collect_match_links(page))

    def _run_cycle(
        self,
        *,
        links,
        extracted,
        goto_errors=None,
        listing_diagnostics=None,
    ):
        page = _CyclePage(goto_errors=goto_errors)
        listing_context = _Context(page)
        detail_context = _Context()
        browser = _Browser()
        scraper = AiscoreScraper(
            page.url,
            page_timeout_ms=12345,
            concurrency=2,
        )
        scraper._create_browser_context = AsyncMock(
            return_value=(browser, listing_context)
        )
        scraper._new_desktop_context = AsyncMock(return_value=detail_context)
        scraper._wait_for_listing_ready = AsyncMock()
        scraper._collect_match_links = AsyncMock(return_value=links)
        scraper._last_listing_diagnostics = dict(listing_diagnostics or {})
        scraper._extract_single = AsyncMock(side_effect=extracted)
        with patch(
            "aiscore_scraper.async_playwright",
            return_value=_PlaywrightContextManager(),
        ):
            result = asyncio.run(scraper.get_live_basketball_totals())
        return scraper, result, page

    def test_listing_navigation_retries_with_explicit_timeout(self):
        scraper, result, page = self._run_cycle(
            links=["https://www.aiscore.com/basketball/match-a/1"],
            extracted=[{"match_id": "1"}],
            goto_errors=[TimeoutError("slow"), None],
        )
        self.assertEqual(result, [{"match_id": "1"}])
        self.assertEqual(len(page.goto_calls), 2)
        self.assertEqual(page.goto_calls[0][1]["timeout"], 12345)
        self.assertEqual(scraper.last_report["listing_attempts"], 2)
        self.assertEqual(len(scraper.last_report["listing_navigation_errors"]), 1)

    def test_partial_cycle_is_reported_without_dropping_valid_rows(self):
        scraper, result, _page = self._run_cycle(
            links=[
                "https://www.aiscore.com/basketball/match-a/1",
                "https://www.aiscore.com/basketball/match-b/2",
            ],
            extracted=[{"match_id": "1"}, None],
        )
        self.assertEqual(result, [{"match_id": "1"}])
        self.assertEqual(scraper.last_report["status"], "partial")
        self.assertEqual(scraper.last_report["discovered_count"], 2)
        self.assertEqual(scraper.last_report["parsed_count"], 1)
        self.assertEqual(scraper.last_report["coverage_pct"], 50.0)

    def test_expected_match_skip_does_not_degrade_cycle_health(self):
        scraper, result, _page = self._run_cycle(
            links=[
                "https://www.aiscore.com/basketball/match-a/1",
                "https://www.aiscore.com/basketball/match-b/2",
            ],
            extracted=[{"match_id": "1"}, _MatchSkip("late_q4")],
        )

        self.assertEqual(result, [{"match_id": "1"}])
        self.assertEqual(scraper.last_report["status"], "ok")
        self.assertEqual(scraper.last_report["parsed_count"], 1)
        self.assertEqual(scraper.last_report["skipped_count"], 1)
        self.assertEqual(scraper.last_report["failed_count"], 0)
        self.assertEqual(scraper.last_report["coverage_pct"], 100.0)
        self.assertEqual(scraper.last_report["parse_coverage_pct"], 50.0)

    def test_unverified_reported_link_makes_complete_parses_partial(self):
        scraper, result, _page = self._run_cycle(
            links=[
                "https://www.aiscore.com/basketball/match-a/1",
                "https://www.aiscore.com/basketball/match-b/2",
            ],
            extracted=[{"match_id": "1"}, {"match_id": "2"}],
            listing_diagnostics={
                "live_tab_count_known": True,
                "live_tab_reported_count": 3,
                "verified_live_link_count": 2,
            },
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(scraper.last_report["status"], "partial")
        self.assertEqual(scraper.last_report["unverified_count"], 1)
        self.assertEqual(scraper.last_report["coverage_pct"], 66.7)

    def test_discovered_but_zero_parsed_raises_and_records_error(self):
        page = _CyclePage()
        listing_context = _Context(page)
        detail_context = _Context()
        browser = _Browser()
        scraper = AiscoreScraper(page.url, concurrency=2)
        scraper._create_browser_context = AsyncMock(
            return_value=(browser, listing_context)
        )
        scraper._new_desktop_context = AsyncMock(return_value=detail_context)
        scraper._wait_for_listing_ready = AsyncMock()
        scraper._collect_match_links = AsyncMock(
            return_value=[
                "https://www.aiscore.com/basketball/match-a/1",
                "https://www.aiscore.com/basketball/match-b/2",
            ]
        )
        scraper._extract_single = AsyncMock(side_effect=[None, None])
        with (
            patch(
                "aiscore_scraper.async_playwright",
                return_value=_PlaywrightContextManager(),
            ),
            self.assertRaisesRegex(RuntimeError, "parsed none"),
        ):
            asyncio.run(scraper.get_live_basketball_totals())
        self.assertEqual(scraper.last_report["status"], "error")
        self.assertEqual(scraper.last_report["parsed_count"], 0)
        self.assertEqual(scraper.last_report["failed_count"], 2)

    def test_primary_lines_use_first_readable_bookmaker_and_skip_overview(self):
        parsed = {
            "opening": 170.5,
            "prematch": 171.5,
            "inplay": 181.5,
            "matchName": "Home - Away",
            "tournament": "FIBA",
            "status": "Q2 05:00",
            "score": "40 - 35",
            "isFinished": False,
            "isQ4": False,
            "remainingMinutes": 5.0,
            "hasLockedRows": False,
            "quarterScores": {
                "home": [20],
                "away": [18],
                "source": "fixture",
                "quality": 90,
            },
            "oddsSnapshot": {
                "opening_lines": [160.5] * 8 + [170.5],
                "prematch_lines": [161.5, 161.5, 171.5],
                "inplay_lines": [175.5, 175.5, 181.5],
                "bookmaker_count": 9,
            },
        }
        page = _DetailPage(parsed)
        scraper = AiscoreScraper(
            "https://www.aiscore.com/basketball",
            skip_h2h=True,
        )
        scraper._fetch_overview_data = AsyncMock(return_value={})
        result = asyncio.run(
            scraper._extract_match(
                page,
                "https://www.aiscore.com/basketball/match-home-away/abc123",
            )
        )
        self.assertEqual(result["opening_total"], 160.5)
        self.assertEqual(result["prematch_total"], 161.5)
        self.assertEqual(result["inplay_total"], 175.5)
        scraper._fetch_overview_data.assert_not_awaited()

    def test_missing_totals_market_is_an_expected_skip(self):
        parsed = {
            "opening": None,
            "prematch": None,
            "inplay": None,
            "matchName": "Home - Away",
            "tournament": "FIBA",
            "status": "Q2 05:00",
            "score": "40 - 35",
            "isFinished": False,
            "isQ4": False,
            "remainingMinutes": 5.0,
            "hasLockedRows": False,
            "quarterScores": {},
            "oddsSnapshot": {},
        }
        page = _DetailPage(parsed)
        scraper = AiscoreScraper(
            "https://www.aiscore.com/basketball",
            skip_h2h=True,
        )

        result = asyncio.run(
            scraper._extract_match(
                page,
                "https://www.aiscore.com/basketball/match-home-away/abc123",
            )
        )

        self.assertIsInstance(result, _MatchSkip)
        self.assertEqual(result.reason, "totals_missing")
        self.assertFalse(result.degraded)
        self.assertTrue(result.retryable)


if __name__ == "__main__":
    unittest.main()
