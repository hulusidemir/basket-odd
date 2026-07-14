import os
import unittest
from unittest.mock import patch

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

    def test_nuxt_total_requires_explicit_total_market_evidence(self):
        scraper = UpcomingScraper()

        self.assertIsNone(
            scraper._verified_total_from_markets(
                [{"market": "Winning Margin", "values": [178.5]}]
            )
        )
        self.assertIsNone(
            scraper._verified_total_from_markets(
                [{"market": "2", "values": [178.5]}]
            )
        )
        self.assertEqual(
            scraper._verified_total_from_markets(
                [
                    {"market": "Moneyline", "values": [180]},
                    {"market": "Total Points O/U", "values": [166.5, 168.5]},
                ]
            ),
            168.5,
        )

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
        link = "https://www.aiscore.com/basketball/match-a-b/id-a"

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
                    "scheduled": {"status": "ok", "count": 1, "error": None},
                    "today_matches": {"status": "skipped", "count": 0, "error": None},
                    "future": {"status": "failed", "count": 0, "error": "timeout"},
                }
                return [link_a]
            scraper._listing_source_reports = {
                "scheduled": {"status": "failed", "count": 0, "error": "timeout"},
                "today_matches": {"status": "ok", "count": 1, "error": None},
                "future": {"status": "ok", "count": 1, "error": None},
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


if __name__ == "__main__":
    unittest.main()
