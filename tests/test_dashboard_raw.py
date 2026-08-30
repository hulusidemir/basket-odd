import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class DashboardRawTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        import dashboard
        cls.dashboard = dashboard
        from db import Database
        cls.dashboard.db = Database(str(Path(cls.tmp.name) / "test.db"))
        cls.dashboard.db.init()

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_live_dto_strips_legacy_analysis_and_keeps_raw_change(self):
        row = self.dashboard._raw_alert({
            "opening": 160,
            "live": 171,
            "direction": "ALT",
            "ai_analysis": '{"fair_line": 166}',
            "display_snapshot": '{"projected_total": 180}',
        })
        self.assertEqual(row["barem_change"], 11)
        self.assertNotIn("ai_analysis", row)
        self.assertNotIn("display_snapshot", row)

    def test_live_dto_adds_current_pace_projection(self):
        row = self.dashboard._raw_alert({
            "match_name": "Home - Away",
            "tournament": "EuroLeague",
            "status": "Q2 05:00",
            "score": "40 - 35",
            "opening": 160,
            "live": 171,
            "direction": "ALT",
        })

        self.assertEqual(row["pace_projection"], 200.0)
        self.assertEqual(row["pace_elapsed_minutes"], 15)
        self.assertIsInstance(row["signal_quality"]["score"], int)
        self.assertIn(row["signal_quality"]["tone"], {"strong", "good", "uncertain", "weak"})

    def test_deleted_dto_uses_frozen_snapshot_without_live_recalculation(self):
        stored = {
            "id": 91,
            "match_id": "frozen-match",
            "score": "99 - 99",
            "status": "Q4 00:01",
            "result": "Başarılı",
            "final_score": "101 - 100",
            "final_status": "Full Time",
            "deleted_at": "2026-08-30 01:00:00",
            "display_snapshot": json.dumps({
                "id": 91,
                "match_id": "frozen-match",
                "score": "40 - 35",
                "status": "Q2 05:00",
                "barem_change": 11,
                "pace_projection": 200.0,
                "signal_quality": {"score": 72, "label": "İyi", "tone": "good"},
                "list_markers": [{"type": "black", "value": "Home"}],
            }),
        }

        with patch.object(
            self.dashboard,
            "current_pace_projection",
            side_effect=AssertionError("deleted rows must never recalculate"),
        ), patch.object(
            self.dashboard,
            "calculate_signal_quality",
            side_effect=AssertionError("deleted SKS must never recalculate"),
        ):
            row = self.dashboard._frozen_deleted_alert(stored)

        self.assertEqual(row["score"], "40 - 35")
        self.assertEqual(row["status"], "Q2 05:00")
        self.assertEqual(row["barem_change"], 11)
        self.assertEqual(row["pace_projection"], 200.0)
        self.assertEqual(row["signal_quality"]["score"], 72)
        self.assertEqual(row["final_score"], "101 - 100")
        self.assertEqual(row["result"], "Başarılı")

    def test_legacy_deleted_dto_leaves_missing_computed_fields_empty(self):
        row = self.dashboard._frozen_deleted_alert({
            "id": 92,
            "match_id": "legacy-match",
            "opening": 160,
            "live": 171,
            "score": "40 - 35",
            "status": "Q2 05:00",
            "display_snapshot": "",
        })

        self.assertIsNone(row["barem_change"])
        self.assertIsNone(row["pace_projection"])
        self.assertIsNone(row["signal_quality"])
        self.assertEqual(row["list_markers"], [])

    def test_archive_pipeline_freezes_projection_for_deleted_page(self):
        alert_id = self.dashboard.db.save_alert(
            "projection-freeze-match",
            "Home - Away",
            160,
            171,
            "ALT",
            11,
            tournament="EuroLeague",
            status="Q2 05:00",
            score="40 - 35",
            signal_count=1,
        )

        self.assertEqual(self.dashboard._archive_active_match("projection-freeze-match"), 1)
        stored = self.dashboard.db.get_deleted_alert_by_id(alert_id)
        snapshot = json.loads(stored["display_snapshot"])
        self.assertEqual(snapshot["snapshot_meta"]["schema_version"], 4)
        self.assertEqual(snapshot["pace_projection"], 200.0)
        self.assertIn("signal_quality", snapshot)

        with patch.object(
            self.dashboard,
            "current_pace_projection",
            side_effect=AssertionError("deleted rows must never recalculate"),
        ):
            deleted = self.dashboard._frozen_deleted_alert(stored)
        self.assertEqual(deleted["pace_projection"], 200.0)

    def test_templates_do_not_contain_removed_model_features(self):
        root = Path(self.dashboard.app.template_folder)
        combined = "\n".join(
            (root / name).read_text(encoding="utf-8")
            for name in ("dashboard.html", "deleted_matches.html", "upcoming_matches.html")
        )
        for removed in ("fair_line", "market_evidence", "projected_total", "h2h_avg_total"):
            self.assertNotIn(removed, combined)

    def test_all_pages_share_shell_navigation_and_table_layout(self):
        root = Path(self.dashboard.app.template_folder)
        templates = {
            name: (root / name).read_text(encoding="utf-8")
            for name in ("dashboard.html", "deleted_matches.html", "upcoming_matches.html", "bankroll.html")
        }
        for content in templates.values():
            self.assertIn("dashboard.css", content)
            self.assertIn('class="app-shell"', content)
            self.assertIn('class="topbar"', content)
            self.assertIn('class="nav"', content)
            self.assertIn('class="hero"', content)
        for name in ("dashboard.html", "deleted_matches.html", "upcoming_matches.html"):
            self.assertIn('class="data-table', templates[name])
        self.assertNotIn("min-width:1240px", templates["deleted_matches.html"])
        stylesheet = (Path(self.dashboard.app.static_folder) / "dashboard.css").read_text(encoding="utf-8")
        self.assertIn("width: calc(100% - 16px);", stylesheet)
        self.assertIn("max-width: none;", stylesheet)

    def test_dashboard_keeps_ios_app_and_mobile_web_links(self):
        template = (Path(self.dashboard.app.template_folder) / "dashboard.html").read_text(encoding="utf-8")
        self.assertIn("function isAppleMobileDevice()", template)
        self.assertIn("function iosAppMatchUrl(value)", template)
        self.assertIn("https://j.aiscore.com/links/", template)
        self.assertIn("function mobileMatchUrl(value)", template)
        self.assertIn("function desktopMatchUrl(value)", template)
        self.assertIn("url.hostname = 'www.aiscore.com'", template)
        self.assertIn('class="match-mobile-link"', template)

    def test_dashboard_has_match_filter_signal_badge_and_row_action_states(self):
        template = (Path(self.dashboard.app.template_folder) / "dashboard.html").read_text(encoding="utf-8")
        stylesheet = (Path(self.dashboard.app.static_folder) / "dashboard.css").read_text(encoding="utf-8")

        self.assertIn("let matchFilter = '';", template)
        self.assertIn("function toggleMatchFilter(button)", template)
        self.assertIn("function signalTime(value)", template)
        self.assertIn("timeZone:'Europe/Istanbul'", template)
        self.assertIn('class="signal-sequence"', template)
        self.assertIn('class="signal-time"', template)
        self.assertIn("signalCount > 1", template)
        self.assertIn('<thead><tr><th>Sinyal</th><th>Maç</th>', template)
        self.assertIn('data-label="Sinyal"><button type="button" class="signal-modal-trigger"', template)
        self.assertIn('<span class="direction-pill ${directionClass}"', template)
        self.assertNotIn('<th class="num">Fark</th>', template)
        self.assertIn('class="change-text ${changeClass}"', template)
        self.assertIn("--icon-control-size: 32px", stylesheet)
        self.assertIn('<circle cx="12" cy="12" r="2.5"></circle>', template)
        self.assertNotIn('2.8 5.7 6.2.9', template)
        for state_class in ("row-bet-placed", "row-followed", "row-ignored"):
            self.assertIn(state_class, template)
            self.assertIn(f".{state_class}", stylesheet)

    def test_dashboard_has_signal_statistics_modal_and_blacklist_controls(self):
        template = (Path(self.dashboard.app.template_folder) / "dashboard.html").read_text(encoding="utf-8")
        stylesheet = (Path(self.dashboard.app.static_folder) / "dashboard.css").read_text(encoding="utf-8")

        self.assertIn('id="signalModal"', template)
        self.assertIn("function openSignalModal(event, alertId)", template)
        self.assertIn("function addModalBlacklist(button)", template)
        self.assertIn("/team-history", template)
        self.assertIn("function finalScoreTotal(value)", template)
        self.assertIn("function historyVerdict(match, finalTotal)", template)
        self.assertIn("Barem hareketi", template)
        self.assertIn("Final skor", template)
        self.assertIn("history-match-link", template)
        self.assertIn("Çeyrek skorlarını aç", template)
        self.assertIn("modal-list-button", stylesheet)
        self.assertIn(".history-verdict.failed", stylesheet)
        self.assertNotIn('id="listForm"', template)
        self.assertNotIn('class="panel list-panel"', template)

    def test_dashboard_has_sks_column_and_plain_language_quality_modal(self):
        template = (Path(self.dashboard.app.template_folder) / "dashboard.html").read_text(encoding="utf-8")
        stylesheet = (Path(self.dashboard.app.static_folder) / "dashboard.css").read_text(encoding="utf-8")

        self.assertIn('id="qualitySortBtn">SKS', template)
        self.assertIn('</button></th><th>İşlemler</th>', template)
        self.assertIn('id="qualityModal"', template)
        self.assertIn("function qualityButton(alert)", template)
        self.assertIn("function openQualityModal(event, alertId)", template)
        self.assertIn('data-label="SKS">${qualityButton(a)}', template)
        self.assertIn("quality-score-trigger", stylesheet)
        self.assertIn("quality-factor-list", stylesheet)
        self.assertIn('id="qualitySortBtn"', template)
        self.assertIn("function sortedByQuality(rows)", template)
        self.assertIn("function toggleQualitySort()", template)

    def test_dashboard_can_recalculate_all_live_sks_values(self):
        template = (Path(self.dashboard.app.template_folder) / "dashboard.html").read_text(encoding="utf-8")
        client = self.dashboard.app.test_client()

        response = client.post("/api/alerts/recalculate-quality", json={})

        self.assertEqual(response.status_code, 200)
        self.assertTrue(all("signal_quality" in row for row in response.get_json()))
        self.assertIn('id="qualityRefreshBtn"', template)
        self.assertIn("function recalculateQuality()", template)
        self.assertIn("/api/alerts/recalculate-quality", template)

    def test_dashboard_opens_modal_only_from_signal_button(self):
        template = (Path(self.dashboard.app.template_folder) / "dashboard.html").read_text(encoding="utf-8")
        stylesheet = (Path(self.dashboard.app.static_folder) / "dashboard.css").read_text(encoding="utf-8")

        self.assertNotIn('data-alert-id="${Number(a.id)}" onclick=', template)
        self.assertIn('class="signal-modal-trigger" onclick="openSignalModal(event,', template)
        self.assertIn("!event.target.closest('.signal-modal-trigger')", template)
        self.assertIn(".signal-table tbody tr { cursor: default; }", stylesheet)
        self.assertIn(".signal-modal-trigger {", stylesheet)

    def test_archive_has_result_and_unique_signal_filters(self):
        template = (Path(self.dashboard.app.template_folder) / "deleted_matches.html").read_text(encoding="utf-8")

        self.assertIn('data-result="Başarılı"', template)
        self.assertIn('data-result="Başarısız"', template)
        self.assertIn('id="uniqueBtn"', template)
        self.assertIn("function uniqueSignalKey(row)", template)
        self.assertIn("function uniqueSignals(rows)", template)
        self.assertIn('class="report-item report-filter" data-result="Başarılı"', template)
        self.assertIn('class="report-item report-filter" data-result="Başarısız"', template)
        self.assertIn("function chooseResultFilter(value)", template)

    def test_archive_has_client_side_pagination(self):
        template = (Path(self.dashboard.app.template_folder) / "deleted_matches.html").read_text(encoding="utf-8")

        self.assertIn("const pageSize=25", template)
        self.assertIn('id="pagination"', template)
        self.assertIn('id="previousPage"', template)
        self.assertIn('id="nextPage"', template)
        self.assertIn("filteredRows.slice(start,start+pageSize)", template)
        self.assertIn("currentPage=1;syncResultFilters();render()", template)

    def test_archive_displays_and_sorts_only_frozen_sks(self):
        template = (Path(self.dashboard.app.template_folder) / "deleted_matches.html").read_text(encoding="utf-8")

        self.assertIn('id="qualitySortBtn"', template)
        self.assertIn("function sortedByQuality(rows)", template)
        self.assertIn("function toggleQualitySort()", template)
        self.assertIn("function qualityButton(alert)", template)
        self.assertIn('id="qualityModal"', template)
        self.assertIn('data-label="SKS">${qualityButton(a)}', template)
        self.assertNotIn("/api/alerts/recalculate-quality", template)

    def test_archive_uses_trash_icons_without_permanent_delete_text(self):
        template = (Path(self.dashboard.app.template_folder) / "deleted_matches.html").read_text(encoding="utf-8")

        self.assertIn("archive-delete-button", template)
        self.assertIn('aria-label="Kaydı sil"', template)
        self.assertNotIn("Kalıcı sil", template)
        self.assertNotIn("kalıcı sil", template)

    def test_page_headers_do_not_show_slogan_copy(self):
        root = Path(self.dashboard.app.template_folder)
        combined = "\n".join(
            (root / name).read_text(encoding="utf-8")
            for name in ("dashboard.html", "deleted_matches.html", "upcoming_matches.html", "bankroll.html")
        )

        self.assertNotIn('class="brand-subtitle"', combined)
        self.assertNotIn("Aynı bahis şirketindeki", combined)
        self.assertNotIn("Yaklaşan karşılaşmaların programı", combined)
        self.assertNotIn("Arşivlenen ham barem sinyalleri", combined)

    def test_archive_result_is_read_only(self):
        template = (Path(self.dashboard.app.template_folder) / "deleted_matches.html").read_text(encoding="utf-8")

        self.assertIn('class="result-pill result-readonly ${resultClass}"', template)
        self.assertNotIn("function setResult(", template)
        self.assertNotIn("/api/deleted-matches/${id}/result", template)

        client = self.dashboard.app.test_client()
        response = client.post(
            "/api/deleted-matches/999/result",
            json={"result": "Başarılı"},
        )
        self.assertEqual(response.status_code, 404)

    def test_result_report_counts_unique_match_direction_pairs(self):
        rows = [
            {"id": 2, "match_id": "m1", "direction": "ALT", "signal_count": 2, "result": "Başarılı"},
            {"id": 1, "match_id": "m1", "direction": "ALT", "signal_count": 1, "result": "Başarılı"},
            {"id": 3, "match_id": "m1", "direction": "ÜST", "signal_count": 3, "result": "Başarısız"},
            {"id": 4, "match_id": "m1", "direction": "ÜST", "signal_count": 4, "result": "Başarısız"},
            {"id": 5, "match_id": "m2", "direction": "ALT", "signal_count": 1, "result": "Başarılı"},
            {"id": 6, "match_id": "m3", "direction": "ALT", "signal_count": 1, "result": ""},
        ]

        report = self.dashboard._basic_result_report(rows)

        self.assertEqual(report["total"], 6)
        self.assertEqual(report["unique_total"], 4)
        self.assertEqual(report["unique_resolved"], 3)
        self.assertEqual(report["unique_successful"], 2)
        self.assertEqual(report["unique_failed"], 1)
        chosen = {
            (row["match_id"], row["direction"]): row["id"]
            for row in self.dashboard._unique_signal_rows(rows)
        }
        self.assertEqual(chosen[("m1", "ALT")], 1)

    def test_team_history_keeps_both_directions_but_hides_same_direction_repeats(self):
        history_rows = [
            {"id": 2, "match_id": "old-match", "match_name": "Home - Rival", "direction": "ALT", "signal_count": 2, "result": "Başarılı"},
            {"id": 1, "match_id": "old-match", "match_name": "Home - Rival", "direction": "ALT", "signal_count": 1, "result": "Başarılı"},
            {"id": 3, "match_id": "old-match", "match_name": "Home - Rival", "direction": "ÜST", "signal_count": 3, "result": "Başarısız"},
        ]

        with patch.object(self.dashboard, "_deleted_rows", return_value=history_rows):
            history = self.dashboard._team_history("Home - Current", "current-match")

        home_matches = history["teams"][0]["matches"]
        self.assertEqual([(row["id"], row["direction"]) for row in home_matches], [(1, "ALT"), (3, "ÜST")])

    def test_modal_sources_expose_teams_and_blacklist_markers(self):
        alert_id = self.dashboard.db.save_alert(
            "modal-stat-match", "İstanbul Home - Ankara Away", 155, 166,
            "ALT", 11, tournament="Modal League", signal_count=1,
        )
        client = self.dashboard.app.test_client()

        history_response = client.get(f"/api/alerts/{alert_id}/team-history")
        self.assertEqual(history_response.status_code, 200)
        self.assertEqual(
            [team["name"] for team in history_response.get_json()["teams"]],
            ["İstanbul Home", "Ankara Away"],
        )

        list_response = client.post("/api/signal-lists", json={
            "list_type": "black", "scope": "team", "value": "İstanbul Home",
        })
        self.assertEqual(list_response.status_code, 200)
        alerts_response = client.get("/api/alerts")
        modal_alert = next(
            row for row in alerts_response.get_json()
            if row["match_id"] == "modal-stat-match"
        )
        self.assertTrue(any(
            marker["type"] == "black" and marker["value"] == "İstanbul Home"
            for marker in modal_alert["list_markers"]
        ))

    def test_action_endpoint_switches_every_signal_for_the_match(self):
        first_id = self.dashboard.db.save_alert(
            "dashboard-action-match", "Home - Away", 160, 171, "ALT", 11,
            signal_count=1,
        )
        second_id = self.dashboard.db.save_alert(
            "dashboard-action-match", "Home - Away", 160, 173, "ALT", 13,
            signal_count=2,
        )
        client = self.dashboard.app.test_client()

        bet_response = client.post(f"/api/alerts/{first_id}/bet", json={})
        self.assertEqual(bet_response.status_code, 200)
        self.assertEqual(bet_response.get_json()["affected"], 2)
        self.assertTrue(all(
            row["bet_placed"] == 1
            for row in self.dashboard.db.active_alerts_for_match("dashboard-action-match")
        ))

        follow_response = client.post(f"/api/alerts/{second_id}/follow", json={})
        self.assertEqual(follow_response.status_code, 200)
        rows = self.dashboard.db.active_alerts_for_match("dashboard-action-match")
        self.assertTrue(all(row["followed"] == 1 for row in rows))
        self.assertTrue(all(row["bet_placed"] == 0 and row["ignored"] == 0 for row in rows))


if __name__ == "__main__":
    unittest.main()
