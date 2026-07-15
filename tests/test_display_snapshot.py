import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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

    def test_current_confidence_score_is_frozen_at_signal_time(self):
        row = self.db.get_alert(self.alert_id)
        row["ai_analysis"] = json.dumps({
            "projected_total": 168,
            "pure_projected_total": 168,
            "fair_line": 166,
            "signal_quality": {
                "quality_score": 77,
                "quality_label": "GÜÇLÜ",
                "confidence_score_version": self.dashboard.CONFIDENCE_SCORE_VERSION,
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
        self.assertEqual(enriched["signal_quality_score"], 82)
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

    def test_deleted_template_displays_frozen_confidence_score_instead_of_gate_label(self):
        template_path = Path(self.dashboard.app.template_folder) / "deleted_matches.html"
        template = template_path.read_text(encoding="utf-8")
        function_start = template.index("function signalQualityHtml(alert)")
        function_end = template.index("function qualityValue(value", function_start)
        quality_renderer = template[function_start:function_end]

        self.assertIn("quality.quality_score ?? alert?.signal_quality_score", quality_renderer)
        self.assertIn("${Math.round(score)}/100", quality_renderer)
        self.assertNotIn("${escapeHtml(gate.text)}</button>", quality_renderer)

    def test_deleted_template_has_compact_ft_score_total_and_projection_order(self):
        template_path = Path(self.dashboard.app.template_folder) / "deleted_matches.html"
        template = template_path.read_text(encoding="utf-8")

        self.assertIn('<th class="ft-col" data-sort="final_status">FT</th>', template)
        self.assertIn('class="ft-col mono">${fullTimeLabel(alert)}</td>', template)
        self.assertIn("${scoreWithTotalHtml(alert.score)}", template)
        self.assertIn("${scoreWithTotalHtml(alert.final_score)}", template)
        self.assertLess(
            template.index('<th data-sort="fair_line">Adil Barem</th>'),
            template.index('<th data-sort="projected">Proj.</th>'),
        )

    def test_star_filters_and_rules_are_absent_from_dashboard_templates(self):
        for filename in ("dashboard.html", "deleted_matches.html"):
            template = (Path(self.dashboard.app.template_folder) / filename).read_text(encoding="utf-8")
            lowered = template.lower()
            self.assertNotIn("starred", lowered)
            self.assertNotIn("bucketstars", lowered)
            self.assertNotIn("yıldızlı", lowered)

    def test_deleted_template_has_first_signal_per_match_view(self):
        template_path = Path(self.dashboard.app.template_folder) / "deleted_matches.html"
        template = template_path.read_text(encoding="utf-8")

        self.assertIn('id="uniqueMatchesBtn"', template)
        self.assertIn("function firstSignalPerMatch(rows)", template)
        self.assertIn("uniqueMatchesOnly ? firstSignalPerMatch(deletedAlerts)", template)
        self.assertIn("const signalCount = Number(alert?.signal_count)", template)
        self.assertIn("renderSignalBreakdown(rows)", template)

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
