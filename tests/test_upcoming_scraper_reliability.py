import os
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

from upcoming_scraper import UpcomingScraper


class _RetryPage:
    def __init__(self):
        self.goto_calls = []
        self.waits = []

    async def goto(self, url, **kwargs):
        self.goto_calls.append((url, kwargs))
        if len(self.goto_calls) == 1:
            raise TimeoutError("first attempt timed out")

    async def wait_for_timeout(self, value):
        self.waits.append(value)


class _Response:
    def __init__(self, status):
        self.status = status


class _HttpRetryPage(_RetryPage):
    async def goto(self, url, **kwargs):
        self.goto_calls.append((url, kwargs))
        return _Response(403 if len(self.goto_calls) == 1 else 200)


class _DetailPage:
    def __init__(self):
        self.closed = False

    def set_default_timeout(self, _value):
        return None

    def is_closed(self):
        return self.closed

    async def close(self):
        self.closed = True


class _DetailContext:
    async def new_page(self):
        return _DetailPage()


class UpcomingScraperSettingsTests(unittest.TestCase):
    def test_invalid_env_settings_fall_back_without_crashing(self):
        with patch.dict(
            os.environ,
            {
                "UPCOMING_DAYS_AHEAD": "bad",
                "UPCOMING_CONCURRENCY": "bad",
                "UPCOMING_MATCH_TIMEOUT_SECONDS": "bad",
            },
            clear=False,
        ):
            scraper = UpcomingScraper(page_timeout_ms=35000)

        self.assertEqual(scraper.days_ahead, 0)
        self.assertEqual(scraper.concurrency, 2)
        self.assertEqual(scraper.match_timeout_seconds, 70)

    def test_runtime_settings_are_clamped(self):
        scraper = UpcomingScraper(
            page_timeout_ms=999999,
            days_ahead=99,
            concurrency=99,
            match_timeout_seconds=999,
        )

        self.assertEqual(scraper.page_timeout_ms, 120000)
        self.assertEqual(scraper.days_ahead, 14)
        self.assertEqual(scraper.concurrency, 8)
        self.assertEqual(scraper.match_timeout_seconds, 180)

    def test_zero_max_matches_keeps_legacy_unlimited_semantics(self):
        self.assertIsNone(UpcomingScraper(max_matches=0).max_matches)

    def test_outer_budget_covers_all_match_batches_and_listing_retry(self):
        scraper = UpcomingScraper(
            page_timeout_ms=5000,
            max_matches=4,
            concurrency=2,
            match_timeout_seconds=30,
        )

        # Two 30-second detail batches plus two listing sources, each with two
        # nav attempts across two generations and bounded readiness overhead.
        self.assertGreaterEqual(scraper.estimated_outer_timeout_seconds(), 160)

    def test_kickoff_provenance_rejects_unlisted_time_only_detail(self):
        scraper = UpcomingScraper()

        self.assertFalse(
            scraper._kickoff_has_trusted_provenance(
                "18:30",
                kickoff_source="detail_header_time",
                listing_source="",
            )
        )
        self.assertTrue(
            scraper._kickoff_has_trusted_provenance(
                "18:30",
                kickoff_source="today_matches",
                listing_source="today_matches",
            )
        )

    def test_listing_kickoff_wins_over_conflicting_detail_date(self):
        scraper = UpcomingScraper()

        kickoff, source = scraper._resolve_kickoff(
            {"kickoff": "2026-07-14 09:00", "kickoff_source": "detail_header"},
            {"kickoff": "2026-07-13 20:30", "kickoff_source": "scheduled_nuxt"},
        )

        self.assertEqual(kickoff, "2026-07-13 20:30")
        self.assertEqual(source, "scheduled_nuxt")

    def test_kickoff_filter_uses_rolling_24_hour_window(self):
        scraper = UpcomingScraper(timezone_id="Europe/Istanbul")
        start = datetime(2026, 8, 6, 12, 0, tzinfo=ZoneInfo("Europe/Istanbul"))
        scraper._window_bounds = lambda: (
            start,
            datetime(2026, 8, 7, 12, 0, tzinfo=ZoneInfo("Europe/Istanbul")),
        )

        self.assertFalse(scraper._kickoff_in_allowed_window("2026-08-06 11:59"))
        self.assertTrue(scraper._kickoff_in_allowed_window("2026-08-06 12:00"))
        self.assertTrue(scraper._kickoff_in_allowed_window("2026-08-07 12:00"))
        self.assertFalse(scraper._kickoff_in_allowed_window("2026-08-07 12:01"))

    def test_today_fallback_alone_is_not_complete_24_hour_coverage(self):
        scraper = UpcomingScraper()
        scraper._listing_source_reports = {
            "scheduled": {"status": "partial", "count": 3},
            "today_matches": {"status": "ok", "count": 3},
            "future": {"status": "skipped", "count": 0},
        }

        self.assertFalse(scraper._listing_is_complete())


class UpcomingScraperAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_navigation_retries_with_explicit_timeout(self):
        scraper = UpcomingScraper(page_timeout_ms=12345)
        page = _RetryPage()

        loaded = await scraper._goto_with_retry(
            page,
            "https://example.test/match",
            label="test page",
        )

        self.assertTrue(loaded)
        self.assertEqual(len(page.goto_calls), 2)
        self.assertTrue(
            all(call[1]["timeout"] == 12345 for call in page.goto_calls)
        )
        self.assertEqual(page.waits, [400])

    async def test_navigation_retries_http_error_responses(self):
        scraper = UpcomingScraper(page_timeout_ms=12345)
        page = _HttpRetryPage()

        loaded = await scraper._goto_with_retry(
            page,
            "https://example.test/match",
            label="test page",
        )

        self.assertTrue(loaded)
        self.assertEqual(len(page.goto_calls), 2)
        self.assertEqual(page.waits, [400])

    async def test_report_counts_discovery_before_cap_and_marks_partial(self):
        scraper = UpcomingScraper(max_matches=2, concurrency=2)
        links = [
            "https://www.aiscore.com/basketball/match-a-b/id-a",
            "https://www.aiscore.com/basketball/match-c-d/id-b",
            "https://www.aiscore.com/basketball/match-e-f/id-c",
        ]

        async def collect(_context):
            scraper._listing_source_reports = {
                "scheduled": {"status": "ok", "count": 3, "error": None},
                "today_matches": {"status": "skipped", "count": 0, "error": None},
                "future": {"status": "skipped", "count": 0, "error": None},
            }
            return links

        async def extract(_context, link):
            if link.endswith("id-b"):
                return None
            return {"match_id": scraper._extract_match_id(link), "match_name": "A - B"}

        scraper._collect_upcoming_links = collect
        scraper._extract_one_with_timeout = extract

        rows = await scraper._fetch_with_context(object())

        self.assertEqual(len(rows), 1)
        self.assertEqual(scraper.last_report["discovered"], 3)
        self.assertEqual(scraper.last_report["attempted"], 2)
        self.assertEqual(scraper.last_report["parsed"], 1)
        self.assertEqual(scraper.last_report["failed"], 1)
        self.assertEqual(scraper.last_report["coverage"], 0.5)
        self.assertTrue(scraper.last_report["truncated"])
        self.assertEqual(scraper.last_report["status"], "partial")
        self.assertFalse(scraper.last_report["reconcile_safe"])

    async def test_complete_report_is_reconcile_safe(self):
        scraper = UpcomingScraper(max_matches=None, concurrency=2)
        links = [
            "https://www.aiscore.com/basketball/match-a-b/id-a",
            "https://www.aiscore.com/basketball/match-c-d/id-b",
        ]

        async def collect(_context):
            scraper._listing_source_reports = {
                "scheduled": {"status": "ok", "count": 2, "error": None},
                "today_matches": {"status": "skipped", "count": 0, "error": None},
                "future": {"status": "skipped", "count": 0, "error": None},
            }
            return links

        async def extract(_context, link):
            return {"match_id": scraper._extract_match_id(link), "match_name": "A - B"}

        scraper._collect_upcoming_links = collect
        scraper._extract_one_with_timeout = extract

        rows = await scraper._fetch_with_context(object())

        self.assertEqual(len(rows), 2)
        self.assertEqual(scraper.last_report["status"], "complete")
        self.assertEqual(scraper.last_report["coverage"], 1.0)
        self.assertTrue(scraper.last_report["reconcile_safe"])

    async def test_parsed_fallback_row_still_marks_generation_partial(self):
        scraper = UpcomingScraper(max_matches=None)
        link = "https://m.aiscore.com/basketball/match-a-b/id-a"

        async def collect(_context):
            scraper._listing_source_reports = {
                "scheduled": {"status": "ok", "count": 1, "error": None},
                "today_matches": {"status": "skipped", "count": 0, "error": None},
                "future": {"status": "skipped", "count": 0, "error": None},
            }
            return [link]

        async def extract(_context, _link):
            return {
                "match_id": "id-a",
                "match_name": "A - B",
                "data_status": "partial",
            }

        scraper._collect_upcoming_links = collect
        scraper._extract_one_with_timeout = extract

        await scraper._fetch_with_context(object())

        self.assertEqual(scraper.last_report["parsed"], 1)
        self.assertEqual(scraper.last_report["partial_rows"], 1)
        self.assertEqual(scraper.last_report["status"], "partial")
        self.assertFalse(scraper.last_report["reconcile_safe"])

    async def test_incomplete_listing_with_some_links_is_retried_and_merged(self):
        scraper = UpcomingScraper(max_matches=None, days_ahead=1)
        link_a = "https://www.aiscore.com/basketball/match-a-b/id-a"
        link_b = "https://www.aiscore.com/basketball/match-c-d/id-b"
        calls = 0

        async def collect(_context):
            nonlocal calls
            calls += 1
            if calls == 1:
                scraper._listing_source_reports = {
                    "scheduled": {"status": "partial", "count": 1, "error": "tomorrow timeout"},
                    "today_matches": {"status": "skipped", "count": 0, "error": None},
                    "future": {"status": "skipped", "count": 0, "error": None},
                }
                return [link_a]
            scraper._listing_source_reports = {
                "scheduled": {"status": "ok", "count": 1, "error": None},
                "today_matches": {"status": "skipped", "count": 0, "error": None},
                "future": {"status": "skipped", "count": 0, "error": None},
            }
            return [link_b]

        async def extract(_context, link):
            return {"match_id": scraper._extract_match_id(link), "match_name": "A - B"}

        scraper._collect_upcoming_links = collect
        scraper._extract_one_with_timeout = extract

        rows = await scraper._fetch_with_context(object())

        self.assertEqual(calls, 2)
        self.assertEqual({row["match_id"] for row in rows}, {"id-a", "id-b"})
        self.assertEqual(len(scraper.last_report["listing_attempts"]), 2)
        self.assertTrue(scraper.last_report["listing_complete"])
        self.assertEqual(scraper.last_report["status"], "complete")

    async def test_missing_total_market_marks_detail_row_partial(self):
        scraper = UpcomingScraper(max_matches=None)
        scraper._kickoff_in_allowed_window = lambda _value: True
        link = "https://www.aiscore.com/basketball/match-a-b/id-a"
        scraper._listing_rows_by_id["id-a"] = {
            "match_id": "id-a",
            "match_name": "A - B",
            "home_team": "A",
            "away_team": "B",
            "kickoff": "18:30",
            "listing_source": "today_matches",
            "kickoff_source": "today_matches",
        }
        scraper._listing_source_by_id["id-a"] = "today_matches"
        scraper._read_odds_page = AsyncMock(
            return_value={
                "match_name": "A - B",
                "opening": None,
                "prematch": None,
                "is_live": False,
                "is_finished": False,
            }
        )
        row = await scraper._extract_one(_DetailContext(), link)

        self.assertEqual(row["data_status"], "partial")
        self.assertIn("total_market_unavailable", row["data_warnings"])
        self.assertEqual(row["match_name"], "A - B")


if __name__ == "__main__":
    unittest.main()
