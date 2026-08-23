import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from db import Database


class DisplaySnapshotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dashboard_temp_dir = tempfile.TemporaryDirectory()
        cls.previous_db_path = os.environ.get("DB_PATH")
        os.environ["DB_PATH"] = str(Path(cls.dashboard_temp_dir.name) / "dashboard.db")

        import config
        import dashboard

        importlib.reload(config)
        cls.dashboard = importlib.reload(dashboard)

    @classmethod
    def tearDownClass(cls):
        if cls.previous_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = cls.previous_db_path
        cls.dashboard_temp_dir.cleanup()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.temp_dir.name) / "test.db"))
        self.db.init()
        self.alert_id = self.db.save_alert(
            "match-1", "Home - Away", 160, 172, "ALT", 12,
            score="75 - 70", status="Q3 04:00",
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_display_snapshot_survives_deletion_unchanged(self):
        snapshot = {
            "id": self.alert_id,
            "direction": "ALT",
            "fair_line": 166.5,
        }
        self.assertEqual(self.db.save_active_alert_display_snapshots({self.alert_id: snapshot}), 1)
        self.db.delete_match_data("match-1")
        row = self.db.get_deleted_alert_by_id(self.alert_id)
        self.assertIn('"fair_line": 166.5', row["display_snapshot"])

    def test_snapshot_guard_prevents_unsnapshotted_soft_delete(self):
        with self.assertRaisesRegex(RuntimeError, "missing its live dashboard snapshot"):
            self.db.delete_match_data(
                "match-1",
                require_display_snapshot=True,
            )

        self.assertIsNotNone(self.db.get_alert(self.alert_id))
        self.db.save_active_alert_display_snapshots({
            self.alert_id: {"id": self.alert_id}
        })
        self.assertEqual(
            self.db.delete_match_data(
                "match-1",
                require_display_snapshot=True,
            ),
            1,
        )
        self.assertIsNotNone(self.db.get_deleted_alert_by_id(self.alert_id))

    def test_snapshot_guard_prevents_unsnapshotted_clear_all(self):
        with self.assertRaisesRegex(RuntimeError, "missing its live dashboard snapshot"):
            self.db.clear_all(require_display_snapshot=True)

        self.assertIsNotNone(self.db.get_alert(self.alert_id))
        self.db.save_active_alert_display_snapshots({
            self.alert_id: {"id": self.alert_id}
        })
        self.assertEqual(
            self.db.clear_all(require_display_snapshot=True),
            1,
        )
        self.assertIsNotNone(self.db.get_deleted_alert_by_id(self.alert_id))

    def test_atomic_archive_persists_snapshot_and_soft_deletes_together(self):
        snapshot = {
            "id": self.alert_id,
            "match_id": "match-1",
            "direction": "ALT",
            "fair_line": 166.5,
        }

        affected = self.db.archive_match_with_display_snapshots(
            "match-1",
            {self.alert_id: snapshot},
        )

        self.assertEqual(affected, 1)
        self.assertIsNone(self.db.get_alert(self.alert_id))
        archived = self.db.get_deleted_alert_by_id(self.alert_id)
        self.assertEqual(json.loads(archived["display_snapshot"]), snapshot)

    def test_atomic_archive_aborts_when_active_set_changed(self):
        second_id = self.db.save_alert(
            "match-1", "Home - Away", 160, 174, "ALT", 14,
            score="76 - 72", status="Q3 03:00", signal_count=2,
        )

        with self.assertRaisesRegex(RuntimeError, "active alerts changed"):
            self.db.archive_match_with_display_snapshots(
                "match-1",
                {self.alert_id: {"id": self.alert_id, "match_id": "match-1"}},
            )

        self.assertIsNotNone(self.db.get_alert(self.alert_id))
        self.assertIsNotNone(self.db.get_alert(second_id))

    def test_hard_delete_only_accepts_archived_alerts(self):
        self.assertFalse(self.db.delete_alert(self.alert_id))
        self.assertIsNotNone(self.db.get_alert(self.alert_id))

        self.db.save_active_alert_display_snapshots({
            self.alert_id: {"id": self.alert_id}
        })
        self.db.delete_match_data("match-1", require_display_snapshot=True)
        self.assertTrue(self.db.delete_alert(self.alert_id))
        self.assertIsNone(self.db.get_deleted_alert_by_id(self.alert_id))
        self.assertFalse(self.db.is_match_deleted("match-1"))

    def test_non_final_recheck_never_clears_resolved_result(self):
        self.db.save_active_alert_display_snapshots({
            self.alert_id: {"id": self.alert_id, "status": "Q3 04:00"}
        })
        self.db.delete_match_data("match-1", require_display_snapshot=True)
        self.db.update_deleted_alert_result(self.alert_id, "Başarılı")

        self.assertEqual(self.db.mark_deleted_match_in_progress("match-1"), 0)
        archived = self.db.get_deleted_alert_by_id(self.alert_id)
        self.assertEqual(archived["result"], "Başarılı")
        self.assertEqual(archived["result_source"], "manual")

    def test_purge_deleted_does_not_remove_reused_active_match_id(self):
        self.db.save_active_alert_display_snapshots({
            self.alert_id: {"id": self.alert_id}
        })
        self.db.delete_match_data("match-1", require_display_snapshot=True)
        with self.db._conn() as conn:
            conn.execute(
                "UPDATE match_actions SET deleted_at = NULL WHERE match_id = ?",
                ("match-1",),
            )
        active_id = self.db.save_alert(
            "match-1", "Home - Away Again", 162, 174, "ÜST", 12,
            score="76 - 72", status="Q3 03:00",
        )

        self.assertEqual(
            self.db.purge_deleted_matches(),
            {"deleted_count": 1, "protected_count": 0},
        )
        self.assertIsNotNone(self.db.get_alert(active_id))
        with self.db._conn() as conn:
            action = conn.execute(
                "SELECT match_id FROM match_actions WHERE match_id = ?",
                ("match-1",),
            ).fetchone()
        self.assertIsNotNone(action)

    def test_live_snapshot_has_version_and_capture_metadata(self):
        with patch.object(self.dashboard, "db", self.db):
            saved = self.dashboard._save_dashboard_snapshots([
                {
                    "id": self.alert_id,
                    "direction": "ALT",
                    "fair_line": 166.5,
                }
            ])

        self.assertEqual(saved, 1)
        row = self.db.get_alert(self.alert_id)
        snapshot = json.loads(row["display_snapshot"])
        self.assertEqual(snapshot["snapshot_meta"]["schema_version"], 1)
        self.assertEqual(snapshot["snapshot_meta"]["source"], "live_dashboard")
        self.assertTrue(snapshot["snapshot_meta"]["captured_at"])

    def test_internal_telegram_delivery_fields_never_enter_live_dto_or_snapshot(self):
        row = self.db.get_alert(self.alert_id)
        row.update({
            "telegram_status": "sent",
            "telegram_retry_count": 1,
            "telegram_last_error": "private",
            "telegram_message_ids": '{"recipient-key": 123}',
            "ai_analysis": json.dumps({
                "projected_total": 168,
                "pure_projected_total": 168,
                "fair_line": 166,
                "fair_edge": -6,
                "projection_quality": 90,
            }),
        })

        enriched = self.dashboard.enrich_alerts_with_analysis([row])[0]
        snapshot = self.dashboard._dashboard_snapshot_payloads([enriched])[self.alert_id]

        for key in (
            "telegram_status",
            "telegram_retry_count",
            "telegram_last_error",
            "telegram_message_ids",
        ):
            self.assertNotIn(key, enriched)
            self.assertNotIn(key, snapshot)

    def test_current_signal_score_is_frozen_at_signal_time(self):
        row = self.db.get_alert(self.alert_id)
        row["ai_analysis"] = json.dumps({
            "projected_total": 168,
            "pure_projected_total": 168,
            "fair_line": 166,
            "projection_model_version": self.dashboard.PROJECTION_MODEL_VERSION,
            "fair_model_version": self.dashboard.FAIR_MODEL_VERSION,
            "signal_quality": {
                "quality_score": 77,
                "quality_label": "GÜÇLÜ",
                "signal_score_version": self.dashboard.SIGNAL_SCORE_VERSION,
                "stars": 4,
            },
        })

        enriched = self.dashboard.enrich_alerts_with_analysis([row])[0]

        self.assertEqual(enriched["signal_quality_score"], 77)
        self.assertEqual(enriched["signal_quality_label"], "GÜÇLÜ")

    def test_deleted_enrichment_removes_legacy_bucket_stars(self):
        row = {
            "id": 10,
            "match_id": "match-10",
            "direction": "ALT",
            "live": 165,
            "status": "FT",
            "score": "80 - 70",
            "result": "",
            "display_snapshot": json.dumps({
                "id": 10,
                "direction": "ALT",
                "live": 165,
                "fair_line": 158,
                "projected": 164,
                "bucket_stars": [{"id": "legacy-star"}],
            }),
            "ai_analysis": json.dumps({
                "fair_line": 158,
                "projected_total": 164,
                "final_direction": "ALT",
            }),
        }

        enriched = self.dashboard._enrich_deleted_alert(row, full=False)

        self.assertNotIn("bucket_stars", enriched)
        self.assertEqual(enriched["result"], "")

    def test_deleted_enrichment_never_exposes_snapshot_bucket_stars(self):
        stars = [{"id": "from-live-dashboard", "label": "Canlı Snapshot"}]
        row = {
            "id": 11,
            "match_id": "match-11",
            "result": "Başarılı",
            "display_snapshot": json.dumps({
                "id": 11,
                "direction": "ÜST",
                "bucket_stars": stars,
            }),
            "ai_analysis": "{}",
        }

        enriched = self.dashboard._enrich_deleted_alert(row, full=False)

        self.assertNotIn("bucket_stars", enriched)

    def test_deleted_enrichment_keeps_frozen_market_evidence_in_list_and_details(self):
        frozen_evidence = {
            "version": self.dashboard.MARKET_EVIDENCE_VERSION,
            "code": "opposes_signal",
            "label": "SİNYALİN TERSİ DESTEKLENİYOR",
            "symbol": "⇄",
            "primary_reason": "Frozen evidence reason",
            "metrics": {
                "stats_fair_total": 171.5,
                "live_total": 165.0,
                "signal_edge": -6.5,
            },
        }
        row = {
            "id": 14,
            "match_id": "match-14",
            "result": "Başarısız",
            "display_snapshot": json.dumps({
                "id": 14,
                "match_id": "match-14",
                "direction": "ALT",
                "market_evidence": frozen_evidence,
            }, ensure_ascii=False),
            "ai_analysis": json.dumps({
                "market_evidence": {
                    "version": self.dashboard.MARKET_EVIDENCE_VERSION,
                    "code": "supports_signal",
                    "label": "RAW_ANALYSIS_MUST_NOT_OVERRIDE_SNAPSHOT",
                    "symbol": "✓",
                },
            }, ensure_ascii=False),
        }

        lightweight = self.dashboard._enrich_deleted_alert(row, full=False)
        full = self.dashboard._enrich_deleted_alert(row, full=True)

        self.assertEqual(lightweight["market_evidence"], frozen_evidence)
        self.assertEqual(full["market_evidence"], frozen_evidence)
        self.assertEqual(lightweight["market_evidence_rank"], 2)
        self.assertEqual(full["market_evidence_rank"], 2)

    def test_live_analysis_rebuild_preserves_signal_time_market_evidence(self):
        frozen_evidence = {
            "version": self.dashboard.MARKET_EVIDENCE_VERSION,
            "code": "supports_signal",
            "symbol": "✓",
        }
        with patch.object(
            self.dashboard,
            "build_signal_analysis",
            return_value={
                "fair_line": 170.0,
                "direction": "ÜST",
                "final_direction": "ÜST",
                "market_evidence": {"code": "insufficient", "symbol": "?"},
            },
        ):
            rebuilt = self.dashboard._rebuild_live_analysis_from_alert(
                {"opening": 160, "live": 170, "direction": "ALT"},
                {"direction": "ALT", "final_direction": "ALT", "market_evidence": frozen_evidence},
            )

        self.assertEqual(rebuilt["fair_line"], 170.0)
        self.assertEqual(rebuilt["market_evidence"], frozen_evidence)
        self.assertEqual(rebuilt["direction"], "ALT")
        self.assertEqual(rebuilt["final_direction"], "ALT")

    def test_live_analysis_rebuild_does_not_invent_legacy_market_evidence(self):
        with patch.object(
            self.dashboard,
            "build_signal_analysis",
            return_value={
                "fair_line": 170.0,
                "direction": "ÜST",
                "final_direction": "ÜST",
                "market_evidence": {"code": "insufficient", "symbol": "?"},
            },
        ):
            rebuilt = self.dashboard._rebuild_live_analysis_from_alert(
                {"opening": 160, "live": 170, "direction": "ALT"},
                {},
            )

        self.assertNotIn("market_evidence", rebuilt)
        self.assertEqual(rebuilt["direction"], "ALT")
        self.assertEqual(rebuilt["final_direction"], "ALT")

    def _seed_refreshable_alert_with_frozen_evidence(self):
        frozen_evidence = {
            "version": self.dashboard.MARKET_EVIDENCE_VERSION,
            "code": "supports_signal",
            "symbol": "✓",
            "supported_direction": "ÜST",
        }
        analysis = {
            "direction": "ÜST",
            "final_direction": "ÜST",
            "projected_total": 166.0,
            "fair_line": 165.0,
            "projection_model_version": self.dashboard.PROJECTION_MODEL_VERSION,
            "fair_model_version": self.dashboard.FAIR_MODEL_VERSION,
            "market_evidence": frozen_evidence,
        }
        with self.db._conn() as conn:
            conn.execute(
                "UPDATE alerts SET url = ?, ai_analysis = ? WHERE id = ?",
                (
                    "https://www.aiscore.com/basketball/match-home-away/test-id",
                    json.dumps(analysis, ensure_ascii=False),
                    self.alert_id,
                ),
            )
        return frozen_evidence

    def _conflicting_refresh_analysis(self):
        return {
            "direction": "ALT",
            "final_direction": "ALT",
            "projected_total": 168.0,
            "fair_line": 167.0,
            "projection_model_version": self.dashboard.PROJECTION_MODEL_VERSION,
            "fair_model_version": self.dashboard.FAIR_MODEL_VERSION,
            "market_evidence": {"code": "insufficient", "symbol": "?"},
        }

    def test_h2h_refresh_preserves_frozen_evidence_and_direction(self):
        frozen_evidence = self._seed_refreshable_alert_with_frozen_evidence()

        with (
            patch.object(self.dashboard, "db", self.db),
            patch.object(
                self.dashboard,
                "_fetch_alert_h2h_body",
                new=AsyncMock(return_value="usable h2h body"),
            ),
            patch.object(
                self.dashboard,
                "build_signal_analysis",
                return_value=self._conflicting_refresh_analysis(),
            ),
        ):
            response = self.dashboard.app.test_client().post(
                f"/api/alerts/{self.alert_id}/h2h/refresh"
            )

        self.assertEqual(response.status_code, 200)
        persisted = json.loads(self.db.get_alert(self.alert_id)["ai_analysis"])
        self.assertEqual(persisted["market_evidence"], frozen_evidence)
        self.assertEqual(persisted["direction"], "ÜST")
        self.assertEqual(persisted["final_direction"], "ÜST")

    def test_quarter_refresh_preserves_frozen_evidence_and_direction(self):
        frozen_evidence = self._seed_refreshable_alert_with_frozen_evidence()
        overview = {
            "status": "Q4 05:00",
            "score": "80 - 78",
            "quarterScores": {
                "home": [20, 20, 20, 20],
                "away": [19, 20, 19, 20],
            },
        }

        with (
            patch.object(self.dashboard, "db", self.db),
            patch.object(
                self.dashboard,
                "_fetch_alert_overview_snapshot",
                new=AsyncMock(return_value=overview),
            ),
            patch.object(
                self.dashboard,
                "build_signal_analysis",
                return_value=self._conflicting_refresh_analysis(),
            ),
        ):
            response = self.dashboard.app.test_client().post(
                f"/api/alerts/{self.alert_id}/quarter-scores/refresh"
            )

        self.assertEqual(response.status_code, 200)
        persisted = json.loads(self.db.get_alert(self.alert_id)["ai_analysis"])
        self.assertEqual(persisted["market_evidence"], frozen_evidence)
        self.assertEqual(persisted["direction"], "ÜST")
        self.assertEqual(persisted["final_direction"], "ÜST")
        self.assertEqual(persisted["quarter_scores"], overview["quarterScores"])

    def test_live_enrichment_keeps_stored_direction_after_backtest_refresh(self):
        frozen_evidence = {
            "version": self.dashboard.MARKET_EVIDENCE_VERSION,
            "code": "supports_signal",
            "symbol": "✓",
        }
        alert = {
            "id": 0,
            "match_id": "match-direction",
            "match_name": "Home - Away",
            "direction": "ALT",
            "opening": 160.0,
            "live": 170.0,
            "diff": 10.0,
            "status": "Q2 05:00",
            "score": "30 - 30",
            "tournament": "FIBA Europe Cup",
            "signal_count": 1,
            "ai_analysis": json.dumps({
                "direction": "ALT",
                "final_direction": "ALT",
                "projected_total": 166.0,
                "fair_line": 165.0,
                "projection_model_version": self.dashboard.PROJECTION_MODEL_VERSION,
                "fair_model_version": self.dashboard.FAIR_MODEL_VERSION,
                "market_evidence": frozen_evidence,
            }, ensure_ascii=False),
        }

        def flip_direction(_alert, analysis, _profile, _threshold):
            return {**analysis, "direction": "ÜST", "final_direction": "ÜST"}

        with patch.object(
            self.dashboard,
            "enrich_analysis_with_backtest",
            side_effect=flip_direction,
        ):
            enriched = self.dashboard.enrich_alerts_with_analysis(
                [alert],
                backtest_profile={},
            )[0]

        self.assertEqual(enriched["direction"], "ALT")
        self.assertEqual(enriched["final_direction"], "ALT")
        self.assertEqual(enriched["analysis"]["direction"], "ALT")
        self.assertEqual(enriched["analysis"]["final_direction"], "ALT")
        self.assertEqual(enriched["market_evidence"], frozen_evidence)

    def test_live_enrichment_prefers_frozen_analysis_direction_over_legacy_alert_direction(self):
        frozen_evidence = {
            "version": self.dashboard.MARKET_EVIDENCE_VERSION,
            "code": "supports_signal",
            "symbol": "✓",
            "supported_direction": "ÜST",
        }
        alert = {
            "id": 0,
            "match_id": "legacy-direction-mismatch",
            "match_name": "Home - Away",
            "direction": "ALT",
            "opening": 170.0,
            "live": 160.0,
            "diff": -10.0,
            "status": "Q2 05:00",
            "score": "30 - 30",
            "tournament": "FIBA Europe Cup",
            "signal_count": 1,
            "ai_analysis": json.dumps({
                "direction": "ÜST",
                "final_direction": "ÜST",
                "projected_total": 166.0,
                "fair_line": 165.0,
                "projection_model_version": self.dashboard.PROJECTION_MODEL_VERSION,
                "fair_model_version": self.dashboard.FAIR_MODEL_VERSION,
                "market_evidence": frozen_evidence,
            }, ensure_ascii=False),
        }

        def flip_to_raw_direction(_alert, analysis, _profile, _threshold):
            return {**analysis, "direction": "ALT", "final_direction": "ALT"}

        with patch.object(
            self.dashboard,
            "enrich_analysis_with_backtest",
            side_effect=flip_to_raw_direction,
        ):
            enriched = self.dashboard.enrich_alerts_with_analysis(
                [alert],
                backtest_profile={},
            )[0]

        self.assertEqual(enriched["direction"], "ÜST")
        self.assertEqual(enriched["final_direction"], "ÜST")
        self.assertEqual(enriched["analysis"]["direction"], "ÜST")
        self.assertEqual(enriched["analysis"]["final_direction"], "ÜST")
        self.assertEqual(enriched["market_evidence"], frozen_evidence)

    def test_deleted_enrichment_keeps_snapshot_model_values_and_overlays_settlement(self):
        row = {
            "id": 12,
            "match_id": "match-12",
            "direction": "ALT",
            "status": "Full Time",
            "score": "81 - 80",
            "final_status": "Full Time",
            "final_score": "81 - 80",
            "result": "Başarısız",
            "note": "settled note",
            "display_snapshot": json.dumps({
                "id": 12,
                "match_id": "match-12",
                "direction": "ÜST",
                "status": "Q3 04:00",
                "score": "55 - 52",
                "fair_line": 170,
                "projected": 174,
                "projected_gap": 8,
                "opening_delta": -12,
                "signal_quality_score": 82,
                "signal_gate": {
                    "state": "SHADOW",
                    "telegram_allowed": False,
                    "evidence": {"resolved_unique": 12},
                },
                "bucket_stars": [{"id": "frozen-star"}],
            }),
            "ai_analysis": json.dumps({
                "final_direction": "ALT",
                "fair_line": 140,
                "projected_total": 141,
                "projected_gap": -20,
                "opening_delta": 99,
                "signal_gate": {
                    "state": "TRUSTED",
                    "telegram_allowed": True,
                },
                "signal_quality": {
                    "quality_score": 5,
                    "quality_label": "RAW_ANALYSIS_MUST_NOT_OVERRIDE_SNAPSHOT",
                },
            }),
        }

        enriched = self.dashboard._enrich_deleted_alert(row, full=True)

        self.assertEqual(enriched["direction"], "ÜST")
        self.assertEqual(enriched["fair_line"], 170)
        self.assertEqual(enriched["projected"], 174)
        self.assertEqual(enriched["projected_gap"], 8)
        self.assertEqual(enriched["opening_delta"], -12)
        self.assertNotIn("signal_quality_score", enriched)
        self.assertEqual(enriched["signal_gate"]["state"], "SHADOW")
        self.assertFalse(enriched["signal_gate"]["telegram_allowed"])
        self.assertEqual(enriched["signal_gate"]["evidence"]["resolved_unique"], 12)
        self.assertNotIn("bucket_stars", enriched)
        self.assertEqual(enriched["status"], "Q3 04:00")
        self.assertEqual(enriched["score"], "55 - 52")
        self.assertEqual(enriched["final_status"], "Full Time")
        self.assertEqual(enriched["final_score"], "81 - 80")
        self.assertEqual(enriched["result"], "Başarısız")
        self.assertEqual(enriched["note"], "settled note")

    def test_deleted_list_payload_is_lightweight_but_keeps_frozen_values(self):
        row = {
            "id": 13,
            "match_id": "match-13",
            "status": "Full Time",
            "score": "82 - 79",
            "result": "Başarılı",
            "display_snapshot": json.dumps({
                "id": 13,
                "match_id": "match-13",
                "match_name": "Home - Away",
                "direction": "ALT",
                "status": "Q2 05:00",
                "score": "40 - 35",
                "fair_line": 166.5,
                "projected": 164.0,
                "selection_reason": "Frozen reason",
                "signal_gate": {"state": "BLOCKED", "reason_codes": ["CANDIDATE_RULE_NOT_MET"]},
                "signal_quality": {"quality_score": 40},
                "bucket_stars": [{"id": "frozen-star"}],
                "analysis": {"large": "detail-only"},
                "team_context": {"large": "detail-only"},
                "snapshot_meta": {"schema_version": 1},
            }),
        }

        lightweight = self.dashboard._enrich_deleted_alert(row, full=False)
        full = self.dashboard._enrich_deleted_alert(row, full=True)

        self.assertEqual(lightweight["fair_line"], 166.5)
        self.assertEqual(lightweight["projected"], 164.0)
        self.assertNotIn("bucket_stars", lightweight)
        self.assertNotIn("bucket_stars", full)
        self.assertEqual(lightweight["status"], "Q2 05:00")
        self.assertEqual(lightweight["score"], "40 - 35")
        self.assertNotIn("analysis", lightweight)
        self.assertNotIn("team_context", lightweight)
        self.assertNotIn("snapshot_meta", lightweight)
        self.assertEqual(full["analysis"], {"large": "detail-only"})
        self.assertEqual(full["snapshot_meta"], {"schema_version": 1})

    def test_deleted_template_displays_frozen_signal_score_and_stars(self):
        template_path = Path(self.dashboard.app.template_folder) / "deleted_matches.html"
        template = template_path.read_text(encoding="utf-8")
        function_start = template.index("function signalQualityHtml(alert)")
        function_end = template.index("function qualityValue(value", function_start)
        quality_renderer = template[function_start:function_end]

        self.assertIn("quality.quality_score ?? alert?.signal_quality_score", quality_renderer)
        self.assertIn("`${Math.round(score)}`", quality_renderer)
        self.assertIn("quality.stars ?? alert?.signal_stars", quality_renderer)
        self.assertIn("'★'.repeat(stars)", quality_renderer)
        self.assertNotIn("'☆'.repeat", quality_renderer)
        self.assertNotIn("${escapeHtml(gate.text)}</button>", quality_renderer)

    def test_deleted_template_displays_frozen_market_evidence(self):
        template_path = Path(self.dashboard.app.template_folder) / "deleted_matches.html"
        template = template_path.read_text(encoding="utf-8")
        renderer_start = template.index("function marketEvidenceHtml(alert)")
        renderer_end = template.index("function qualityValue(value", renderer_start)
        renderer = template[renderer_start:renderer_end]
        modal_start = template.index("function openMarketEvidenceModal(alertId)")
        modal_end = template.index("function closeSignalQualityModal()", modal_start)
        modal = template[modal_start:modal_end]

        self.assertIn('<th data-sort="market_evidence_rank">Kanıt</th>', template)
        self.assertIn("alert?.market_evidence", renderer)
        self.assertIn("const hasEvidence = Boolean(String(evidence.code || '').trim())", renderer)
        self.assertIn("hasEvidence ? '?' : '–'", renderer)
        self.assertIn("Kanıt özelliğinden önce oluşturuldu", renderer)
        self.assertIn("openMarketEvidenceModal", renderer)
        self.assertIn("<td>${marketEvidenceHtml(alert)}</td>", template)
        self.assertIn("'market_evidence_rank'].includes(field)", template)
        self.assertIn("İstatistik Kanıtı -", modal)
        self.assertIn("Bu sinyal kanıt özelliğinden önce oluşturuldu", modal)
        self.assertIn("evidence.primary_reason", modal)
        self.assertIn("Kalan sayı · model / piyasa", modal)
        self.assertIn("Gerçekleşmiş skor sapması", modal)
        self.assertIn("Serbest atış oranı", modal)

    def test_deleted_template_has_compact_ft_score_total_and_projection_order(self):
        template_path = Path(self.dashboard.app.template_folder) / "deleted_matches.html"
        template = template_path.read_text(encoding="utf-8")

        self.assertIn('<th class="ft-col" data-sort="final_status">FT</th>', template)
        self.assertIn('class="ft-col mono">${fullTimeLabel(alert)}</td>', template)
        self.assertIn("${scoreWithTotalHtml(alert.score)}", template)
        self.assertIn("${scoreWithTotalHtml(alert.final_score)}", template)
        self.assertLess(
            template.index('<th data-sort="fair_line">Adil Barem</th>'),
            template.index('<th data-sort="projected">Tempo Proj.</th>'),
        )

    def test_team_history_uses_final_score_for_match_result_total(self):
        serialized = self.dashboard._serialize_team_history_entry({
            "id": 21,
            "match_id": "match-21",
            "score": "55 - 52",
            "final_status": "Full Time",
            "final_score": "81 - 80",
        })
        template_path = Path(self.dashboard.app.template_folder) / "dashboard.html"
        template = template_path.read_text(encoding="utf-8")

        self.assertEqual(serialized["score"], "55 - 52")
        self.assertEqual(serialized["final_status"], "Full Time")
        self.assertEqual(serialized["final_score"], "81 - 80")
        self.assertIn(
            "const score = String(match.final_score || (hasLegacyFinalScore ? match.score : '') || '').trim();",
            template,
        )

    def test_star_filters_and_rules_are_absent_from_dashboard_templates(self):
        for filename in ("dashboard.html", "deleted_matches.html"):
            template = (Path(self.dashboard.app.template_folder) / filename).read_text(encoding="utf-8")
            lowered = template.lower()
            self.assertNotIn("starred", lowered)
            self.assertNotIn("bucketstars", lowered)
            self.assertNotIn("yıldızlı", lowered)

    def test_dashboard_market_evidence_uses_symbol_and_compact_explanation(self):
        template_path = Path(self.dashboard.app.template_folder) / "dashboard.html"
        template = template_path.read_text(encoding="utf-8")

        self.assertIn("const hasEvidence = Boolean(String(evidence.code || '').trim())", template)
        self.assertIn("const symbol = String(evidence.symbol || (hasEvidence ? '?' : '–'))", template)
        self.assertIn("Kanıt özelliğinden önce oluşturuldu", template)
        self.assertIn('<div class="row"><span>İstatistiksel adil barem</span>', template)
        self.assertIn('<div class="row"><span>Kalan sayı · model / piyasa</span>', template)
        self.assertIn('<div class="row"><span>Gerçekleşmiş skor sapması</span>', template)
        self.assertIn('<div class="row"><span>Tahmini tempo / 40 dk</span>', template)
        self.assertIn('<div class="row"><span>Serbest atış oranı</span>', template)
        self.assertNotIn("scoreData.risk_note", template)
        self.assertNotIn('Skorun Dağılımı', template)
        self.assertNotIn("'☆'.repeat", template)
        self.assertNotIn("const componentHelpTexts = {", template)

    def test_dashboard_displays_signal_score_next_to_market_evidence(self):
        template_path = Path(self.dashboard.app.template_folder) / "dashboard.html"
        template = template_path.read_text(encoding="utf-8")
        function_start = template.index("function signalQualityHtml(alert)")
        function_end = template.index("function cardGateReasonHtml(alert)", function_start)
        renderer = template[function_start:function_end]

        self.assertIn("alert?.signal_quality_score ?? quality.quality_score", renderer)
        self.assertIn('class="sig-score-pill ${scoreTone}"', renderer)
        self.assertIn("openSignalScoreModal", renderer)
        self.assertIn("</button>${scorePill}</span>", renderer)

        modal_start = template.index("function openSignalScoreModal(id)")
        modal_end = template.index("function openSignalQualityModal(id)", modal_start)
        modal = template[modal_start:modal_end]
        self.assertIn("Sinyal Skoru -", modal)
        self.assertIn('<div class="sig-section-title">Neden?</div>', modal)
        self.assertIn("quality.reason || alert.signal_quality_reason", modal)
        self.assertIn("quality.risk_note || alert.signal_quality_risk_note", modal)

    def test_market_evidence_outcome_report_deduplicates_match_and_direction(self):
        version = self.dashboard.MARKET_EVIDENCE_VERSION
        rows = [
            {
                "id": 1,
                "match_id": "m1",
                "direction": "ALT",
                "signal_count": 1,
                "result": "Başarılı",
                "market_evidence": {"version": version, "code": "supports_signal"},
            },
            {
                "id": 2,
                "match_id": "m1",
                "direction": "ALT",
                "signal_count": 2,
                "result": "Başarısız",
                "market_evidence": {"version": version, "code": "opposes_signal"},
            },
            {
                "id": 3,
                "match_id": "m1",
                "direction": "ÜST",
                "signal_count": 3,
                "result": "Başarısız",
                "market_evidence": {"version": version, "code": "opposes_signal"},
            },
            {
                "id": 4,
                "match_id": "m2",
                "direction": "ALT",
                "signal_count": 1,
                "result": "Başarılı",
                "market_evidence": {"version": version, "code": "mixed"},
            },
            {
                "id": 5,
                "match_id": "legacy",
                "direction": "ALT",
                "signal_count": 1,
                "result": "Başarılı",
                "market_evidence": {
                    "version": "market_evidence_4x10_v1",
                    "code": "supports_market",
                },
            },
        ]

        report = self.dashboard.build_market_evidence_outcome_report(rows)
        buckets = {bucket["code"]: bucket for bucket in report["buckets"]}

        self.assertEqual(report["total_unique"], 3)
        self.assertEqual(report["resolved"], 3)
        self.assertEqual(report["decisive_resolved"], 2)
        self.assertEqual(report["verdict_correct"], 2)
        self.assertEqual(report["verdict_accuracy"], 100.0)
        self.assertEqual(buckets["supports_signal"]["total"], 1)
        self.assertEqual(buckets["opposes_signal"]["total"], 1)
        self.assertEqual(buckets["mixed"]["total"], 1)

    def test_market_evidence_outcome_report_rejects_unknown_direction(self):
        version = self.dashboard.MARKET_EVIDENCE_VERSION
        report = self.dashboard.build_market_evidence_outcome_report([
            {
                "id": 1,
                "match_id": "m1",
                "direction": "",
                "signal_count": 1,
                "result": "Başarılı",
                "market_evidence": {"version": version, "code": "supports_signal"},
            },
            {
                "id": 2,
                "match_id": "m2",
                "direction": "SIDE",
                "signal_count": 1,
                "result": "Başarılı",
                "market_evidence": {"version": version, "code": "supports_signal"},
            },
        ])

        self.assertEqual(report["total_unique"], 0)
        self.assertEqual(report["resolved"], 0)

    def test_market_evidence_outcome_report_falls_back_from_invalid_final_direction(self):
        version = self.dashboard.MARKET_EVIDENCE_VERSION
        report = self.dashboard.build_market_evidence_outcome_report([
            {
                "id": 1,
                "match_id": "m1",
                "final_direction": "SIDE",
                "direction": "ALT",
                "signal_count": 1,
                "result": "Başarılı",
                "market_evidence": {"version": version, "code": "supports_signal"},
            },
        ])

        self.assertEqual(report["total_unique"], 1)
        self.assertEqual(report["resolved"], 1)

    def test_deleted_template_has_first_signal_per_match_and_direction_view(self):
        template_path = Path(self.dashboard.app.template_folder) / "deleted_matches.html"
        template = template_path.read_text(encoding="utf-8")

        self.assertIn('id="uniqueMatchesBtn"', template)
        self.assertIn("function firstSignalPerMatchDirection(rows)", template)
        self.assertIn(
            "JSON.stringify([matchKey(alert), normalizeDirection(alert.direction)])",
            template,
        )
        self.assertIn(
            "uniqueMatchesOnly ? firstSignalPerMatchDirection(deletedAlerts)",
            template,
        )
        self.assertIn("const signalCount = Number(alert?.signal_count)", template)
        self.assertIn("renderSignalBreakdown(rows)", template)
        self.assertIn("const uniqueScoped = firstSignalPerMatchDirection(rows)", template)
        self.assertIn("return baseStats(scoped, uniqueScoped)", template)

    def test_deleted_details_route_uses_direct_id_lookup(self):
        self.db.save_active_alert_display_snapshots({
            self.alert_id: {
                "id": self.alert_id,
                "match_id": "match-1",
                "match_name": "Home - Away",
                "direction": "ALT",
            }
        })
        self.db.delete_match_data("match-1", require_display_snapshot=True)

        with (
            patch.object(self.dashboard, "db", self.db),
            patch.object(self.db, "recent_deleted_alerts", side_effect=AssertionError("full archive scan")),
        ):
            client = self.dashboard.app.test_client()
            response = client.get(f"/api/deleted-matches/{self.alert_id}/details")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["id"], self.alert_id)

    def test_deleted_insights_are_descriptive_not_play_recommendations(self):
        signals = []
        for index in range(10):
            signals.append({
                "id": index + 1,
                "match_id": f"alt-{index}",
                "match_name": f"Alt Home {index} - Alt Away {index}",
                "direction": "ALT",
                "diff": 2.5,
                "status": "Q2",
                "result": "Başarılı",
            })
            signals.append({
                "id": index + 101,
                "match_id": f"ust-{index}",
                "match_name": f"Ust Home {index} - Ust Away {index}",
                "direction": "ÜST",
                "diff": 0.5,
                "status": "Q3",
                "result": "Başarısız",
            })

        report = self.dashboard.build_deleted_matches_insights(signals)
        serialized = json.dumps(report, ensure_ascii=False)

        self.assertEqual(report["rules"], {})
        self.assertEqual(report["simulation"], {})
        self.assertNotIn('"verdict": "OYNA"', serialized)
        self.assertNotIn("güvenle gir", serialized)
        self.assertIn("oynanabilirlik kanıtı değildir", serialized)


if __name__ == "__main__":
    unittest.main()
