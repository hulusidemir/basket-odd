import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from flask import Flask

import upcoming_app


class _FakeScraper:
    max_matches = 2

    def __init__(self, report, matches):
        self.last_report = report
        self._matches = matches

    async def fetch(self):
        return list(self._matches)

    def estimated_outer_timeout_seconds(self, _planned_matches=None):
        return 60


class _FakeDatabase:
    instances = []

    def __init__(self, _path):
        self.save_call = None
        self.__class__.instances.append(self)

    def init(self):
        return None

    def save_upcoming_matches_and_signals(self, matches, **kwargs):
        self.save_call = (matches, kwargs)
        return {
            "matches": matches,
            "saved_matches": len(matches),
            "saved_signals": 0,
            "reconciled": bool(kwargs.get("reconcile")),
            "removed_missing": 0,
            "removed_expired": 0,
        }

    def list_upcoming_matches(self, limit=500):
        return [
            {
                "match_id": "id-a",
                "match_name": "A - B",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        ][:limit]


def _complete_report():
    return {
        "status": "complete",
        "listing_complete": True,
        "reconcile_safe": True,
        "truncated": False,
        "failed": 0,
        "coverage": 1.0,
        "discovered_match_ids": ["id-a"],
    }


class UpcomingAppReliabilityTests(unittest.TestCase):
    def setUp(self):
        _FakeDatabase.instances.clear()
        with upcoming_app._fetch_lock:
            upcoming_app._fetch_state.update(
                {
                    "job_id": 42,
                    "running": True,
                    "started_at": upcoming_app._now_iso(),
                    "finished_at": None,
                    "matches": [],
                    "count": 0,
                    "saved_matches": 0,
                    "saved_signals": 0,
                    "report": None,
                    "error": None,
                }
            )

    def test_reconcile_requires_complete_nonempty_full_coverage_report(self):
        self.assertTrue(upcoming_app._reconcile_allowed(_complete_report()))

        for changed in (
            {"status": "partial"},
            {"listing_complete": False},
            {"reconcile_safe": False},
            {"truncated": True},
            {"failed": 1},
            {"coverage": 0.99},
            {"discovered_match_ids": []},
        ):
            report = {**_complete_report(), **changed}
            self.assertFalse(upcoming_app._reconcile_allowed(report), changed)

    def test_partial_job_passes_seen_ids_but_disables_reconciliation(self):
        report = {
            **_complete_report(),
            "status": "partial",
            "reconcile_safe": False,
            "failed": 1,
            "coverage": 0.5,
            "discovered_match_ids": ["id-a", "id-b"],
        }
        scraper = _FakeScraper(report, [{"match_id": "id-a", "match_name": "A - B"}])

        with patch.object(upcoming_app, "Database", _FakeDatabase):
            upcoming_app._run_fetch_job(42, scraper, 60, "ignored.db")

        _, kwargs = _FakeDatabase.instances[0].save_call
        self.assertEqual(kwargs["seen_match_ids"], ["id-a", "id-b"])
        self.assertFalse(kwargs["reconcile"])
        state = upcoming_app._public_state()
        self.assertEqual(state["report"]["status"], "partial")
        self.assertFalse(state["report"]["reconciled"])

    def test_complete_job_enables_reconciliation(self):
        scraper = _FakeScraper(
            _complete_report(),
            [{"match_id": "id-a", "match_name": "A - B"}],
        )

        with patch.object(upcoming_app, "Database", _FakeDatabase):
            upcoming_app._run_fetch_job(42, scraper, 60, "ignored.db")

        _, kwargs = _FakeDatabase.instances[0].save_call
        self.assertTrue(kwargs["reconcile"])

    def test_freshness_marks_mixed_old_rows_stale(self):
        now = datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc)
        rows = [
            {"fetched_at": (now - timedelta(seconds=30)).isoformat()},
            {"fetched_at": (now - timedelta(seconds=3600)).isoformat()},
        ]

        freshness = upcoming_app._freshness_summary(rows, now=now)

        self.assertEqual(freshness["data_age_seconds"], 30)
        self.assertEqual(freshness["oldest_data_age_seconds"], 3600)
        self.assertEqual(freshness["stale_row_count"], 1)
        self.assertTrue(freshness["stale"])

    def test_invalid_app_env_uses_bounded_default(self):
        with patch.dict("os.environ", {"BAD_UPCOMING_INT": "not-a-number"}):
            value = upcoming_app._env_int(
                "BAD_UPCOMING_INT", 17, minimum=10, maximum=20
            )
        self.assertEqual(value, 17)

    def test_build_scraper_uses_config_concurrency_and_zero_means_unlimited(self):
        class ConfigStub:
            AISCORE_URL = "https://example.test/basketball"
            PAGE_TIMEOUT_MS = 30000
            UPCOMING_DAYS_AHEAD = 0
            AISCORE_TIMEZONE = "UTC"
            UPCOMING_CONCURRENCY = 5

        with patch.dict(
            "os.environ",
            {"UPCOMING_MAX_MATCHES": "0", "UPCOMING_MATCH_TIMEOUT_SECONDS": "bad"},
        ):
            scraper = upcoming_app._build_scraper(ConfigStub())

        self.assertIsNone(scraper.max_matches)
        self.assertEqual(scraper.concurrency, 5)
        self.assertEqual(scraper.match_timeout_seconds, 60)

    def test_list_api_exposes_report_and_freshness(self):
        class ConfigStub:
            DB_PATH = "ignored.db"

        with upcoming_app._fetch_lock:
            upcoming_app._fetch_state.update(
                {
                    "running": False,
                    "report": _complete_report(),
                    "error": "previous transient error",
                }
            )
        app = Flask(__name__)
        app.register_blueprint(upcoming_app.upcoming_bp)

        with (
            patch.object(upcoming_app, "Config", return_value=ConfigStub()),
            patch.object(upcoming_app, "Database", _FakeDatabase),
        ):
            response = app.test_client().get("/upcoming/api/list")

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["report"]["status"], "complete")
        self.assertIn("fetched_at", payload)
        self.assertIn("data_age_seconds", payload)
        self.assertEqual(payload["last_fetch_error"], "previous transient error")


if __name__ == "__main__":
    unittest.main()
