"""Check modal presentation against stored alert and snapshot fields."""

import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def render_helpers(alert, team=None):
    setup = """
const esc = value => String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
const fmt = value => hasMetric(value) ? Number(value).toFixed(1) : '—';
const desktopMatchUrl = () => '';
"""
    script = setup + (ROOT / "static/signal_display.js").read_text(encoding="utf-8") + "\n"
    script += f"const alert = {json.dumps(alert, ensure_ascii=False)};\n"
    script += f"const team = {json.dumps(team, ensure_ascii=False)};\n"
    script += "console.log(JSON.stringify({summary: signalSummary(alert), reasons: qualityReasons(alert), reference: signalReferenceFacts(alert), lines: signalLineFacts(alert), format: signalFormat(alert), subtitle: signalSectionSubtitle(alert), history: team ? historyTeamCard(team) : ''}));"
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def test_modal_uses_stored_quality_fair_projection_and_factors():
    alert = {
        "direction": "ALT", "quality_score": 72, "quality_label": "YÜKSEK",
        "quality_factors": json.dumps({"fair_edge": 24, "pace_support": 18, "market_move": -4,
                                       "game_state": 12, "data_quality": 10, "repeat_penalty": -5}),
        "score": "45 - 40", "status": "Q2 04:00", "live": 170,
        "fair_total": 158.5, "pace_projection": 162.0,
        "ppm_comparison": {"current_ppm": 3.4, "required_ppm": 4.3, "required_change_pct": 26.5},
        "opening": 184, "barem_change": -14,
        "reference_used": "prematch", "reference_total": 180,
        "decision_change": -10, "effective_threshold": 0,
    }
    rendered = render_helpers(alert)
    assert "72" in rendered["summary"] and "YÜKSEK" in rendered["summary"]
    assert "158.5" in rendered["summary"] and "162.0" in rendered["summary"]
    assert "4,30" in rendered["summary"] and "%26,5" in rendered["summary"]
    assert "Barem hareketi: 180,0 → 170,0 (−10,0) · -4 puan" in rendered["reasons"]
    assert "Tekrar sinyali etkisi · -5 puan" in rendered["reasons"]
    assert "Adil barem farkı: −11,5 sayı · +24 puan" in rendered["reasons"]
    assert "Mevcut → gereken PPM farkı +26,5% · +18 puan" in rendered["reasons"]
    assert rendered["reasons"].count("<li>") <= 5
    assert "Uygulanan eşik" not in rendered["reference"]
    assert "Son Maç Önü Baremi" in rendered["reference"]
    assert "Maç önü referansına göre fark" in rendered["reference"]
    assert "İlk Açılış Baremi" in rendered["lines"]
    alert["effective_threshold"] = 12.6
    assert "Uygulanan eşik" in render_helpers(alert)["reference"]


def test_legacy_null_quality_and_small_team_sample():
    rendered = render_helpers(
        {"quality_score": None, "quality_label": None, "quality_factors": None},
        {"name": "Home", "role": "Ev", "summary": {"total": 1, "successful": 1,
         "failed": 0, "success_rate": 100}, "matches": []},
    )
    assert "quality-empty" in rendered["summary"]
    assert "Sinyal Neden Geldi?" in rendered["reasons"]
    assert "1/1 başarılı" in rendered["history"]
    assert "Örneklem yetersiz" in rendered["history"]
    assert "100%" not in rendered["history"]
    assert rendered["format"] == "—"
    assert "Canlıya —" in rendered["summary"]


def test_fair_projection_reference_and_signal_order_presentation():
    alert = {"direction": "ÜST", "live": 127.5, "fair_total": 133.8,
             "pace_projection": 131.2, "opening": 145.5, "barem_change": -18,
             "reference_used": "opening", "reference_total": 145.5,
             "decision_change": -18, "signal_count": 1,
             "quality_factors": {"fair_edge": 0, "market_move": 20, "data_quality": 10}}
    rendered = render_helpers(alert)
    assert "Canlıya +6,3" in rendered["summary"]
    assert "Canlıya +3,7" in rendered["summary"]
    assert rendered["summary"].count('class="positive-text"') == 2
    assert "Adil barem farkı: +6,3 sayı · 0 puan" in rendered["reasons"]
    assert "Barem hareketi: 145,5 → 127,5 (−18,0) · +20 puan" in rendered["reasons"]
    assert rendered["lines"].count("145.5") == 1
    assert "Sinyal referansı</span><strong>İlk Açılış Baremi" in rendered["reference"]
    assert "İlk sinyal" in rendered["lines"]
    alert["signal_count"] = 3
    assert "3. sinyal" in render_helpers(alert)["lines"]
    alert["direction"] = "ALT"
    assert render_helpers(alert)["summary"].count('class="negative-text"') == 2


def test_duration_label_uses_existing_live_duration_field():
    from dashboard import _raw_alert

    source = {"match_name": "Home - Away", "tournament": "Club Friendship",
              "status": "Q2 04:00", "score": "45 - 40", "opening": 145.5,
              "live": 127.5, "direction": "ALT"}
    forty = _raw_alert(source, confirmed_12_minutes=False)
    forty_eight = _raw_alert(source, confirmed_12_minutes=True)
    assert forty["pace_game_minutes"] == 40
    assert forty_eight["pace_game_minutes"] == 48
    assert render_helpers(forty)["format"] == "4×10 / 40 dk"
    rendered = render_helpers(forty_eight)
    assert rendered["format"] == "4×12 / 48 dk"
    assert "Club Friendship · 4×12 / 48 dk" in rendered["subtitle"]
    assert "Canlı saatten doğrulandı" not in rendered["subtitle"]


def test_live_list_management_is_collapsed_and_shared_modal_sections():
    live = (ROOT / "templates/dashboard.html").read_text(encoding="utf-8")
    archived = (ROOT / "templates/deleted_matches.html").read_text(encoding="utf-8")
    assert '<details class="modal-lists-section" id="modalListDetails"' in live
    assert "${listsOpen ? 'open' : ''}" in live
    assert 'id="modalListRows"' in live and 'onclick="toggleSignalList(this)"' in live
    for template in (live, archived):
        assert "${signalSummary(alert)}" in template
        assert "${qualityReasons(alert)}" in template
        assert "${signalLineFacts(alert)}" in template
    assert "${signalSectionSubtitle(alert)}" in live
