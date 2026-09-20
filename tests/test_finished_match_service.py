import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from db import Database
from aiscore_final_scraper import _finished_browser_lock, _finished_browser_session
from finished_match_service import (
    AiscoreFinishedMatchChecker,
    FinishedCheckBusy,
    _active_finished_scan_lock,
    _empty_result_summary,
    _settle_deleted_match_from_final_score,
    run_active_match_finished_scan,
    run_deleted_match_result_cycle,
    run_single_deleted_match_result_check,
)


class FinishedMatchServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp_dir.name) / "test.db"))
        self.db.init()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_finished_checker_uses_separate_protected_scrapling_profile(self):
        checker = AiscoreFinishedMatchChecker(page_timeout_ms=20_000)
        with patch.dict(
            "os.environ",
            {"PLAYWRIGHT_PROXY": "socks5://localhost:9050"},
        ):
            options = checker._scrapling_session_options()

        self.assertEqual(options["proxy"], "socks5://localhost:9050")
        self.assertTrue(options["solve_cloudflare"])
        self.assertTrue(options["block_webrtc"])
        self.assertGreaterEqual(options["timeout"], 90_000)
        self.assertTrue(options["user_data_dir"].endswith("scrapling-finished-profile"))

    def test_check_matches_reuses_one_scrapling_session(self):
        checker = AiscoreFinishedMatchChecker(page_timeout_ms=20_000)
        match = {
            "match_id": "match-1",
            "match_name": "A - B",
            "url": "https://m.aiscore.com/basketball/match-a-b/match-1",
        }
        parsed = {
            "match_id": "match-1",
            "status": "Q4 01:00",
            "score": "",
            "is_finished": False,
        }

        class Session:
            context = MagicMock()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

        session = Session()
        checker._check_single_with_retry = AsyncMock(return_value=dict(parsed))
        with patch(
            "aiscore_final_scraper.AsyncStealthySession",
            return_value=session,
        ) as session_factory:
            results = asyncio.run(checker.check_matches([match]))

        self.assertEqual(results, [parsed])
        session_factory.assert_called_once()
        checker._check_single_with_retry.assert_awaited_once_with(session, match)
        self.assertEqual(checker.last_report["checked_count"], 1)

    def test_hung_cloudflare_attempt_times_out_and_next_match_is_checked(self):
        checker = AiscoreFinishedMatchChecker(page_timeout_ms=100, retry_attempts=0)
        cancelled = []

        async def fetch(session, match):
            if match["match_id"] == "stuck":
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.append(True)
            return {"match_id": match["match_id"], "is_finished": False}

        checker._check_single = fetch
        session = MagicMock()
        session.playwright = None
        with (
            patch("aiscore_final_scraper.AsyncStealthySession", return_value=session),
            patch("aiscore_final_scraper._MATCH_ATTEMPT_TIMEOUT", 0.01),
        ):
            results = asyncio.run(checker.check_matches([
                {"match_id": "stuck"}, {"match_id": "next"},
            ]))

        self.assertEqual(cancelled, [True])
        self.assertEqual(results, [{"match_id": "next", "is_finished": False}])
        self.assertEqual(checker.last_report["failure_counts"], {"timeout": 1})
        session.__aexit__.assert_awaited_once()
        self.assertFalse(_finished_browser_lock.locked())

    def test_scan_deadline_preserves_final_results_and_releases_active_lock(self):
        for match_id in ("finished", "stuck"):
            self.db.save_alert(match_id, "A - B", 160, 170, "ALT", 10,
                               status="Q4 01:00", score="70 - 70",
                               url=f"https://example.test/{match_id}")
        session = MagicMock()
        session.playwright = None

        async def fetch(checker, browser, match):
            if match["match_id"] == "stuck":
                # Earlier finals are already archived while the next page hangs.
                self.assertFalse(self.db.active_alerts_for_match("finished"))
                self.assertEqual(self.db.get_deleted_alerts_for_match("finished")[0]["result"], "Başarılı")
                await asyncio.Event().wait()
            return {"match_id": match["match_id"], "is_finished": True,
                    "status": "Full Time", "score": "80 - 75"}

        def archive(match_id):
            return self.db.archive_match_with_display_snapshots(match_id, {
                row["id"]: {"id": row["id"], "score": row["score"]}
                for row in self.db.active_alerts_for_match(match_id)
            })

        with (
            patch("aiscore_final_scraper.AsyncStealthySession", return_value=session),
            patch.object(AiscoreFinishedMatchChecker, "_check_single", fetch),
            patch("aiscore_final_scraper._SCAN_TIMEOUT", 0.03),
            patch.object(self.db, "get_active_matches_with_urls", return_value=[
                {"match_id": "finished"}, {"match_id": "stuck"},
            ]),
        ):
            summary = asyncio.run(run_active_match_finished_scan(self.db, _Config(), archive))

        self.assertEqual(summary["moved_count"], 1)
        self.assertEqual(summary["updated_count"], 1)
        self.assertEqual(summary["failure_counts"], {"scan_timeout": 1})
        self.assertEqual(len(self.db.active_alerts_for_match("stuck")), 1)
        self.assertEqual(self.db.get_deleted_alerts_for_match("finished")[0]["result"], "Başarılı")
        self.assertFalse(_active_finished_scan_lock.locked())
        self.assertFalse(_finished_browser_lock.locked())

    def test_all_final_checks_share_browser_lock(self):
        session = MagicMock()
        session.playwright = None

        async def overlap():
            async with _finished_browser_session({}):
                with self.assertRaises(FinishedCheckBusy):
                    async with _finished_browser_session({}):
                        self.fail("A second browser must not start")

        with patch("aiscore_final_scraper.AsyncStealthySession", return_value=session) as factory:
            asyncio.run(overlap())
        factory.assert_called_once()
        self.assertFalse(_finished_browser_lock.locked())

    def test_partial_start_and_stuck_close_stop_driver_and_allow_next_scan(self):
        async def hang(*args):
            await asyncio.Event().wait()

        session = MagicMock()
        session.__aenter__.side_effect = hang
        session.__aexit__.side_effect = hang
        session.playwright.stop = AsyncMock()

        async def open_browser():
            async with _finished_browser_session({}):
                self.fail("Startup should time out")

        with (
            patch("aiscore_final_scraper.AsyncStealthySession", return_value=session),
            patch("aiscore_final_scraper._BROWSER_START_TIMEOUT", 0.01),
            patch("aiscore_final_scraper._BROWSER_CLOSE_TIMEOUT", 0.01),
        ):
            with self.assertRaises(TimeoutError):
                asyncio.run(open_browser())
        session.playwright.stop.assert_awaited_once()
        self.assertFalse(_finished_browser_lock.locked())

        session.__aenter__.side_effect = None
        session.__aexit__.side_effect = None

        async def retry():
            async with _finished_browser_session({}):
                return True

        with patch("aiscore_final_scraper.AsyncStealthySession", return_value=session):
            self.assertTrue(asyncio.run(retry()))

    def test_cancelled_scan_closes_browser_and_releases_locks(self):
        session = MagicMock()
        session.playwright = None
        entered = asyncio.Event()

        async def fetch(*args):
            entered.set()
            await asyncio.Event().wait()

        async def cancel_scan():
            task = asyncio.create_task(run_active_match_finished_scan(self.db, _Config()))
            await entered.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        with (
            patch("aiscore_final_scraper.AsyncStealthySession", return_value=session),
            patch.object(self.db, "get_active_matches_with_urls", return_value=[{"match_id": "stuck"}]),
            patch.object(AiscoreFinishedMatchChecker, "_check_single", fetch),
        ):
            asyncio.run(cancel_scan())
        session.__aexit__.assert_awaited_once()
        self.assertFalse(_active_finished_scan_lock.locked())
        self.assertFalse(_finished_browser_lock.locked())

    def test_deleted_result_cycle_settles_from_stored_final_score(self):
        alert_id = self.db.save_alert(
            "match-1",
            "Home - Away",
            160,
            170,
            "ALT",
            10,
            status="Full Time",
            score="70 - 80",
            url="https://example.test/match-1",
        )
        self.db.delete_match_data("match-1")

        summary = asyncio.run(run_deleted_match_result_cycle(self.db, _Config()))

        self.assertEqual(summary["tracked_count"], 1)
        self.assertEqual(summary["checked_count"], 1)
        self.assertEqual(summary["finished_match_count"], 1)
        self.assertEqual(summary["updated_count"], 1)
        row = self.db.get_deleted_alert_by_id(alert_id)
        self.assertEqual(row["result"], "Başarılı")
        self.assertEqual(row["status"], "Full Time")
        self.assertEqual(row["score"], "70 - 80")
        self.assertEqual(summary["coverage_percent"], 100.0)

    def test_checker_retries_a_transient_failure(self):
        checker = AiscoreFinishedMatchChecker(
            page_timeout_ms=100,
            retry_attempts=1,
        )
        match = {
            "match_id": "match-retry",
            "match_name": "A - B",
            "url": "https://example.test/retry",
        }
        success = {
            "match_id": "match-retry",
            "status": "Q4 01:00",
            "score": "",
            "is_finished": False,
        }
        checker._check_single = AsyncMock(side_effect=[
            {"match_id": "match-retry", "_check_error": "timeout"},
            success,
        ])

        with patch("aiscore_final_scraper.asyncio.sleep", new=AsyncMock()):
            result = asyncio.run(checker._check_single_with_retry(None, match))

        self.assertEqual(result["match_id"], "match-retry")
        self.assertEqual(result["_check_attempts"], 2)
        self.assertEqual(checker._check_single.await_count, 2)

    def test_access_block_page_is_not_treated_as_a_match(self):
        checker = AiscoreFinishedMatchChecker(
            page_timeout_ms=100,
            retry_attempts=0,
        )
        page = MagicMock()
        page.set_extra_http_headers = AsyncMock()
        page.wait_for_timeout = AsyncMock()
        page.evaluate = AsyncMock(return_value={
            "title": "...",
            "pageText": "aiscore.com has been blocked by the decision",
            "status": "",
            "score": "",
            "isFinished": False,
        })

        class Session:
            async def fetch(self, url, **kwargs):
                await kwargs["page_action"](page)

        result = asyncio.run(checker._check_single(Session(), {
            "match_id": "blocked-match",
            "match_name": "A - B",
            "url": "https://m.aiscore.com/basketball/match-a-b/blocked-match",
        }))

        self.assertEqual(result["_check_error"], "blocked")

    def test_active_scan_reports_partial_coverage_without_archiving_failed_match(self):
        for match_id in ("match-ok", "match-failed"):
            self.db.save_alert(
                match_id,
                f"{match_id} Home - Away",
                160,
                170,
                "ALT",
                10,
                status="Q4 01:00",
                score="70 - 70",
                url=f"https://example.test/{match_id}",
            )

        checker = AiscoreFinishedMatchChecker(page_timeout_ms=100)
        checker.check_matches = AsyncMock(return_value=[{
            "match_id": "match-ok",
            "match_name": "Home - Away",
            "status": "Q4 01:00",
            "score": "",
            "is_finished": False,
        }])
        checker.last_report = {
            "attempted_count": 2,
            "checked_count": 1,
            "check_failed_count": 1,
            "retry_count": 1,
            "failure_counts": {"timeout": 1},
            "failures": [{
                "match_id": "match-failed",
                "error_code": "timeout",
                "attempts": 2,
            }],
        }

        with patch(
            "finished_match_service.AiscoreFinishedMatchChecker",
            return_value=checker,
        ):
            summary = asyncio.run(run_active_match_finished_scan(self.db, _Config()))

        self.assertEqual(summary["checked_count"], 1)
        self.assertEqual(summary["check_failed_count"], 1)
        self.assertEqual(summary["retry_count"], 1)
        self.assertEqual(summary["coverage_percent"], 50.0)
        self.assertEqual(summary["failure_counts"], {"timeout": 1})
        self.assertIn("ulaşılamadı ve aktif bırakıldı", summary["message"])
        active_ids = {
            row["match_id"] for row in self.db.get_active_matches_with_urls()
        }
        self.assertIn("match-failed", active_ids)

    def test_overlapping_active_scan_returns_busy_instead_of_starting_browser(self):
        self.assertTrue(_active_finished_scan_lock.acquire(blocking=False))
        try:
            summary = asyncio.run(run_active_match_finished_scan(self.db, _Config()))
        finally:
            _active_finished_scan_lock.release()

        self.assertTrue(summary["busy"])
        self.assertIn("zaten çalışıyor", summary["message"])

    def test_deleted_result_cycle_does_not_settle_non_final_stored_score(self):
        alert_id = self.db.save_alert(
            "match-1",
            "Home - Away",
            160,
            170,
            "ALT",
            10,
            status="Q3 04:00",
            score="70 - 80",
            url="https://example.test/match-1",
        )
        self.db.delete_match_data("match-1")

        with patch(
            "finished_match_service.AiscoreFinishedMatchChecker.check_matches",
            new=AsyncMock(return_value=[]),
        ):
            summary = asyncio.run(run_deleted_match_result_cycle(self.db, _Config()))

        self.assertEqual(summary["tracked_count"], 1)
        self.assertEqual(summary["checked_count"], 0)
        self.assertEqual(summary["updated_count"], 0)
        self.assertIn("ulaşılamadı", summary["message"])
        row = self.db.get_deleted_alert_by_id(alert_id)
        self.assertEqual(row["result"], "")

    def test_active_finished_scan_snapshots_match_before_soft_delete(self):
        alert_id = self.db.save_alert(
            "match-1",
            "Home - Away",
            160,
            170,
            "ALT",
            10,
            status="Q4 00:10",
            score="70 - 80",
            url="https://example.test/match-1",
        )

        def before_delete(match_id):
            active = self.db.active_alerts_for_match(match_id)
            self.assertEqual([row["id"] for row in active], [alert_id])
            return self.db.archive_match_with_display_snapshots(match_id, {
                alert_id: {"id": alert_id, "snapshot_marker": "live-dashboard"}
            })

        with patch(
            "finished_match_service.AiscoreFinishedMatchChecker.check_matches",
            new=AsyncMock(return_value=[{
                "match_id": "match-1",
                "match_name": "Home - Away",
                "status": "Full Time",
                "score": "70 - 80",
                "is_finished": True,
            }]),
        ):
            summary = asyncio.run(run_active_match_finished_scan(
                self.db,
                _Config(),
                before_delete=before_delete,
            ))

        self.assertEqual(summary["moved_count"], 1)
        row = self.db.get_deleted_alert_by_id(alert_id)
        self.assertEqual(
            row["display_snapshot"],
            '{"id": 1, "snapshot_marker": "live-dashboard"}',
        )
        self.assertEqual(row["result"], "Başarılı")
        self.assertEqual(summary["updated_count"], 1)

    def test_active_finished_scan_accepts_atomic_archive_callback(self):
        alert_id = self.db.save_alert(
            "match-atomic",
            "Home - Away",
            160,
            170,
            "ÜST",
            10,
            status="Q4 00:10",
            score="80 - 80",
            url="https://example.test/match-atomic",
        )

        def archive(match_id):
            return self.db.archive_match_with_display_snapshots(
                match_id,
                {
                    alert_id: {
                        "id": alert_id,
                        "match_id": match_id,
                        "direction": "ÜST",
                    }
                },
            )

        with patch(
            "finished_match_service.AiscoreFinishedMatchChecker.check_matches",
            new=AsyncMock(return_value=[{
                "match_id": "match-atomic",
                "match_name": "Home - Away",
                "status": "Full Time",
                "score": "90 - 90",
                "is_finished": True,
            }]),
        ):
            summary = asyncio.run(run_active_match_finished_scan(
                self.db,
                _Config(),
                before_delete=archive,
            ))

        self.assertEqual(summary["moved_count"], 1)
        self.assertEqual(summary["updated_count"], 1)
        self.assertEqual(summary["successful_count"], 1)
        archived = self.db.get_deleted_alert_by_id(alert_id)
        self.assertEqual(archived["result"], "Başarılı")
        self.assertEqual(archived["status"], "Q4 00:10")
        self.assertEqual(archived["score"], "80 - 80")
        self.assertEqual(archived["final_status"], "Full Time")
        self.assertEqual(archived["final_score"], "90 - 90")

    def test_settlement_uses_stored_raw_signal_direction(self):
        alert_id = self.db.save_alert(
            "match-1",
            "Home - Away",
            160,
            170,
            "ALT",
            10,
            status="Full Time",
            score="90 - 90",
            url="https://example.test/match-1",
        )
        self.db.archive_match_with_display_snapshots("match-1", {
            alert_id: {
                "id": alert_id,
                "direction": "ÜST",
            }
        })
        self.db.delete_match_data("match-1")

        summary = asyncio.run(run_deleted_match_result_cycle(self.db, _Config()))

        self.assertEqual(summary["updated_count"], 1)
        row = self.db.get_deleted_alert_by_id(alert_id)
        self.assertEqual(row["direction"], "ALT")
        self.assertEqual(json.loads(row["display_snapshot"])["direction"], "ÜST")
        self.assertEqual(row["result"], "Başarısız")

    def test_one_archive_failure_does_not_stop_later_finished_matches(self):
        first_id = self.db.save_alert(
            "match-fail", "A - B", 160, 170, "ALT", 10,
            status="Q4 00:10", score="70 - 70", url="https://example.test/fail",
        )
        second_id = self.db.save_alert(
            "match-ok", "C - D", 160, 170, "ALT", 10,
            status="Q4 00:10", score="70 - 70", url="https://example.test/ok",
        )

        def archive(match_id):
            if match_id == "match-fail":
                raise RuntimeError("snapshot conflict")
            return self.db.archive_match_with_display_snapshots(
                match_id,
                {second_id: {"id": second_id, "match_id": match_id, "direction": "ALT"}},
            )

        results = [
            {"match_id": "match-fail", "match_name": "A - B", "status": "Full Time", "score": "80 - 75", "is_finished": True},
            {"match_id": "match-ok", "match_name": "C - D", "status": "Full Time", "score": "80 - 75", "is_finished": True},
        ]
        with (
            patch(
                "finished_match_service.AiscoreFinishedMatchChecker.check_matches",
                new=AsyncMock(return_value=results),
            ),
            patch("finished_match_service.logger.exception"),
        ):
            summary = asyncio.run(run_active_match_finished_scan(
                self.db,
                _Config(),
                before_delete=archive,
            ))

        self.assertEqual(summary["archive_failed_count"], 1)
        self.assertEqual(summary["moved_count"], 1)
        self.assertIsNotNone(self.db.get_alert(first_id))
        self.assertIsNotNone(self.db.get_deleted_alert_by_id(second_id))

    def test_callback_without_archive_count_does_not_delete_active_signal(self):
        alert_id = self.db.save_alert(
            "match-incomplete", "A - B", 160, 170, "ALT", 10,
            status="Q4 00:10", score="70 - 70", url="https://example.test/incomplete",
        )
        with (
            patch(
                "finished_match_service.AiscoreFinishedMatchChecker.check_matches",
                new=AsyncMock(return_value=[{
                    "match_id": "match-incomplete", "status": "Full Time",
                    "score": "80 - 75", "is_finished": True,
                }]),
            ),
            patch("finished_match_service.logger.exception"),
        ):
            summary = asyncio.run(run_active_match_finished_scan(
                self.db, _Config(), before_delete=lambda match_id: None,
            ))

        self.assertEqual(summary["archive_failed_count"], 1)
        self.assertEqual(summary["moved_count"], 0)
        self.assertIsNotNone(self.db.get_alert(alert_id))

    def test_missing_snapshot_callback_cannot_archive_a_signal(self):
        alert_id = self.db.save_alert("no-snapshot", "A - B", 160, 170, "ALT", 10,
                                      url="https://example.test/no-snapshot")
        with (
            patch.object(AiscoreFinishedMatchChecker, "check_matches", new=AsyncMock(return_value=[{
                "match_id": "no-snapshot", "status": "Full Time",
                "score": "80 - 75", "is_finished": True,
            }])),
            patch("finished_match_service.logger.exception"),
        ):
            summary = asyncio.run(run_active_match_finished_scan(self.db, _Config()))
        self.assertEqual(summary["archive_failed_count"], 1)
        self.assertIsNotNone(self.db.get_alert(alert_id))

    def test_settle_returns_false_when_no_alert_can_be_updated(self):
        self.db.save_alert(
            "match-1",
            "Home - Away",
            160,
            170,
            "INVALID",
            10,
            status="Full Time",
            score="90 - 90",
            url="https://example.test/match-1",
        )
        self.db.delete_match_data("match-1")
        summary = _empty_result_summary(tracked_count=1)

        settled = _settle_deleted_match_from_final_score(
            self.db,
            summary,
            "match-1",
            "90 - 90",
            "Full Time",
        )

        self.assertFalse(settled)
        self.assertEqual(summary["finished_match_count"], 0)
        self.assertEqual(summary["updated_count"], 0)

    def test_single_recheck_fetches_again_and_corrects_resolved_result(self):
        alert_id = self.db.save_alert(
            "match-correct",
            "Home - Away",
            160,
            170,
            "ALT",
            10,
            status="Full Time",
            score="90 - 90",
            url="https://example.test/match-correct",
        )
        self.db.delete_match_data("match-correct")
        self.db.update_deleted_alert_final_result(
            alert_id, result="Başarısız", final_score="90 - 90", final_status="Full Time",
        )

        with patch(
            "finished_match_service.AiscoreFinishedMatchChecker.check_matches",
            new=AsyncMock(return_value=[{
                "match_id": "match-correct",
                "match_name": "Home - Away",
                "status": "Full Time",
                "score": "70 - 75",
                "is_finished": True,
            }]),
        ) as check_matches:
            summary = asyncio.run(run_single_deleted_match_result_check(
                self.db,
                _Config(),
                alert_id,
            ))

        check_matches.assert_awaited_once()
        self.assertEqual(summary["updated_count"], 1)
        row = self.db.get_deleted_alert_by_id(alert_id)
        self.assertEqual(row["result"], "Başarılı")
        self.assertEqual(row["result_source"], "automatic_final_score")
        self.assertEqual(row["final_score"], "70 - 75")

    def test_automatic_settlement_does_not_overwrite_settled_result_without_recheck(self):
        alert_id = self.db.save_alert(
            "match-settled",
            "Home - Away",
            160,
            170,
            "ALT",
            10,
            status="Q4 00:10",
            score="80 - 80",
            url="https://example.test/match-settled",
        )
        self.db.delete_match_data("match-settled")
        self.db.update_deleted_alert_final_result(
            alert_id, result="Başarısız", final_score="90 - 90", final_status="Full Time",
        )

        updated = self.db.update_deleted_alert_final_result(
            alert_id,
            result="Başarılı",
            final_score="70 - 75",
            final_status="Full Time",
        )

        self.assertFalse(updated)
        row = self.db.get_deleted_alert_by_id(alert_id)
        self.assertEqual(row["result"], "Başarısız")
        self.assertEqual(row["result_source"], "automatic_final_score")


class _Config:
    PAGE_TIMEOUT_MS = 100


if __name__ == "__main__":
    unittest.main()
