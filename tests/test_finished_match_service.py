import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from db import Database
from finished_match_service import (
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
            self.db.save_active_alert_display_snapshots({
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

    def test_settlement_direction_prefers_frozen_display_snapshot(self):
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
            ai_analysis=json.dumps({"final_direction": "ALT"}),
        )
        self.db.save_active_alert_display_snapshots({
            alert_id: {
                "id": alert_id,
                "direction": "ÜST",
                "final_direction": "ÜST",
            }
        })
        self.db.delete_match_data("match-1")

        summary = asyncio.run(run_deleted_match_result_cycle(self.db, _Config()))

        self.assertEqual(summary["updated_count"], 1)
        row = self.db.get_deleted_alert_by_id(alert_id)
        self.assertEqual(row["direction"], "ALT")
        self.assertEqual(json.loads(row["display_snapshot"])["direction"], "ÜST")
        self.assertEqual(row["result"], "Başarılı")

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
        self.db.update_deleted_alert_result(alert_id, "Başarısız")

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

    def test_automatic_settlement_does_not_overwrite_manual_result(self):
        alert_id = self.db.save_alert(
            "match-manual",
            "Home - Away",
            160,
            170,
            "ALT",
            10,
            status="Q4 00:10",
            score="80 - 80",
            url="https://example.test/match-manual",
        )
        self.db.delete_match_data("match-manual")
        self.db.update_deleted_alert_result(alert_id, "Başarısız")

        updated = self.db.update_deleted_alert_final_result(
            alert_id,
            result="Başarılı",
            final_score="70 - 75",
            final_status="Full Time",
        )

        self.assertFalse(updated)
        row = self.db.get_deleted_alert_by_id(alert_id)
        self.assertEqual(row["result"], "Başarısız")
        self.assertEqual(row["result_source"], "manual")


class _Config:
    PAGE_TIMEOUT_MS = 100


if __name__ == "__main__":
    unittest.main()
