"""
dashboard.py — Anomaly detection dashboard.
Flask-powered web interface.

Usage:
    python dashboard.py
"""

import asyncio
import csv
import io
import json
import os
import re
from datetime import datetime, timedelta
from flask import Flask, Response, jsonify, render_template, request
from db import Database
from config import Config
from finished_match_service import (
    run_active_match_finished_scan,
    run_deleted_match_result_cycle,
    run_single_deleted_match_result_check,
)
from signal_analysis import build_backtest_profile, build_signal_analysis, enrich_analysis_with_backtest

config = Config()
db = Database(config.DB_PATH)
db.init()

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["TEMPLATES_AUTO_RELOAD"] = True

BET_BUILDER_ALERT_WINDOW_MINUTES = 240


def _parse_analysis(raw) -> dict:
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _strip_score_labels(value):
    legacy_prefix = "ai"
    score_keys = {
        f"{legacy_prefix}_score",
        f"{legacy_prefix}_tier",
        f"{legacy_prefix}_confidence",
        "signal_" + "grade",
        "dashboard_" + "action",
        "selection_" + "policy",
    }
    if isinstance(value, dict):
        return {
            key: _strip_score_labels(item)
            for key, item in value.items()
            if key not in score_keys
        }
    if isinstance(value, list):
        return [_strip_score_labels(item) for item in value]
    if isinstance(value, str):
        legacy_label = "A" + "I"
        cleaned = re.sub(rf"\b{legacy_label}\s+\d+(?:[.,]\d+)?\s*,?\s*", "", value)
        replacements = {
            f"Nihai {legacy_label} yön": "Nihai yön",
            f"{legacy_label} karar": "Yeni karar",
            f"{legacy_label} yön": "Yeni yön",
            f"{legacy_label} filtresinden": "Filtrelerden",
            f"{legacy_label} filtre": "Sinyal filtre",
            f"{legacy_label} ": "",
        }
        for old, new in replacements.items():
            cleaned = cleaned.replace(old, new)
        return re.sub(r"\s{2,}", " ", cleaned).strip()
    return value


def _sanitize_recent_form_analysis(analysis: dict) -> dict:
    if not isinstance(analysis, dict):
        return analysis or {}
    analysis = _strip_score_labels(analysis)
    home = analysis.get("home_last6") if isinstance(analysis.get("home_last6"), dict) else {}
    away = analysis.get("away_last6") if isinstance(analysis.get("away_last6"), dict) else {}
    home_scores = home.get("scores") if isinstance(home.get("scores"), list) else []
    away_scores = away.get("scores") if isinstance(away.get("scores"), list) else []
    same_match_rows = (
        bool(home_scores and away_scores)
        and home.get("source") == "match_rows"
        and away.get("source") == "match_rows"
        and home_scores == away_scores
    )
    h2h_as_form = home.get("source") == "h2h_stats" or away.get("source") == "h2h_stats"
    if not same_match_rows and not h2h_as_form:
        return analysis

    cleaned = {**analysis, "home_last6": {}, "away_last6": {}, "team_recent_total": None}
    warnings = list(cleaned.get("warnings") or [])
    note = "SF verisi H2H ile karışmış göründüğü için gösterilmedi; yeni çekimde takım son maç bölümü ayrı okunacak."
    if note not in warnings:
        cleaned["warnings"] = [note, *warnings]
    if isinstance(cleaned.get("team_context"), dict):
        cleaned["team_context"] = {
            **cleaned["team_context"],
            "home_last6_profile": {},
            "away_last6_profile": {},
            "home_profile": {},
            "away_profile": {},
            "expected_total": None,
        }
    if isinstance(cleaned.get("fair_components"), dict):
        cleaned["fair_components"] = {**cleaned["fair_components"], "team_recent": None}
    return cleaned


def _trim_backtest_payload(backtest: dict | None) -> dict:
    if not isinstance(backtest, dict):
        return {}
    scores = {}
    for direction, item in (backtest.get("scores") or {}).items():
        if not isinstance(item, dict):
            continue
        scores[direction] = {
            "rate": item.get("rate"),
            "samples": item.get("samples"),
        }
    return {
        "sample_size": backtest.get("sample_size", 0),
        "chosen_rate": backtest.get("chosen_rate"),
        "chosen_samples": backtest.get("chosen_samples", 0),
        "scores": scores,
    }


def enrich_alerts_with_analysis(alerts: list[dict], backtest_profile: dict | None = None) -> list[dict]:
    for alert in alerts:
        analysis = _parse_analysis(alert.get("ai_analysis"))
        alert.pop("ai_analysis", None)
        if not analysis:
            analysis = build_signal_analysis(
                {
                    **alert,
                    "opening_total": alert.get("opening"),
                    "inplay_total": alert.get("live"),
                    "prematch_total": alert.get("prematch"),
                },
                {},
                config.THRESHOLD,
                backtest_profile=backtest_profile,
            )
        elif backtest_profile is not None:
            analysis = enrich_analysis_with_backtest(alert, analysis, backtest_profile, config.THRESHOLD)
        analysis = _sanitize_recent_form_analysis(analysis)
        if analysis.get("projected_total") is None:
            analysis = {**analysis}
            analysis["fair_line"] = None
            analysis["fair_edge"] = None
            analysis["recommendation"] = "Adil barem hesaplanamadı: maç süresi/projeksiyon güvenilir okunamadı."
            warnings = analysis.get("warnings") if isinstance(analysis.get("warnings"), list) else []
            missing_note = "Adil barem hesaplanamadı: maç süresi/projeksiyon güvenilir okunamadı."
            if missing_note not in warnings:
                analysis["warnings"] = [*warnings, missing_note]
        if analysis.get("h2h_quality_score") is None:
            inferred_quality = 35 if analysis.get("h2h_total") is None else 80
            inferred_notes = list(analysis.get("h2h_quality_notes") or [])
            if analysis.get("h2h_total") is None:
                inferred_notes.append("Eski analiz kaydı: H2H ortalaması yok.")
            elif analysis.get("team_recent_total") is None:
                inferred_notes.append("Eski analiz kaydı: H2H var, takım son-form bileşeni yok.")
            analysis = {
                **analysis,
                "h2h_quality_score": inferred_quality,
                "h2h_quality_notes": inferred_notes,
            }
            warnings = analysis.get("warnings") if isinstance(analysis.get("warnings"), list) else []
            analysis["warnings"] = [*warnings, *[note for note in inferred_notes if note not in warnings]]
        if isinstance(analysis.get("backtest"), dict):
            analysis = {**analysis, "backtest": _trim_backtest_payload(analysis.get("backtest"))}
        alert["analysis"] = analysis
        alert["fair_line"] = analysis.get("fair_line")
        alert["fair_edge"] = analysis.get("fair_edge")
        alert["projected"] = analysis.get("projected_total")
        alert["market_total"] = analysis.get("market_total")
        alert["team_recent_total"] = analysis.get("team_recent_total")
        alert["home_last6"] = analysis.get("home_last6") or {}
        alert["away_last6"] = analysis.get("away_last6") or {}
        alert["h2h_total"] = analysis.get("h2h_total")
        alert["history_total"] = analysis.get("history_total")
        alert["recommendation"] = analysis.get("recommendation") or ""
        alert["legacy_direction"] = analysis.get("legacy_direction") or alert.get("direction")
        alert["final_direction"] = analysis.get("final_direction") or analysis.get("direction") or alert.get("direction")
        alert["signal_scores"] = analysis.get("signal_scores") or {}
        alert["signal_votes"] = analysis.get("signal_votes") if isinstance(analysis.get("signal_votes"), list) else []
        alert["backtest"] = _trim_backtest_payload(analysis.get("backtest"))
        alert["telegram_eligible"] = bool(analysis.get("telegram_eligible"))
        alert["selection_reason"] = analysis.get("selection_reason") or ""
        alert["projection_quality"] = analysis.get("projection_quality")
        alert["warnings"] = analysis.get("warnings") if isinstance(analysis.get("warnings"), list) else []
    return alerts



def _is_live_basketball_status(status: str) -> bool:
    status_clean = (status or "").strip().upper()
    if not status_clean:
        return False

    if re.match(r"^HT$", status_clean):
        return True

    if re.search(r"(?:Q[1-4]|[1-4]Q)(?:[-:\s]+\d{1,2}:\d{2})?$", status_clean):
        return True

    if re.search(r"^[1-4]\s*[-:\s]+\d{1,2}:\d{2}$", status_clean):
        return True

    return False


def _is_recent_alert(alerted_at: str, window_minutes: int = BET_BUILDER_ALERT_WINDOW_MINUTES) -> bool:
    if not alerted_at:
        return False

    try:
        alert_time = datetime.strptime(alerted_at, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return False

    return alert_time >= (datetime.utcnow() - timedelta(minutes=window_minutes))


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def build_bet_builder(max_count: int) -> dict:
    backtest_profile = build_backtest_profile(db.recent_deleted_alerts(limit=None))
    alerts = [
        alert
        for alert in enrich_alerts_with_analysis(db.recent_alerts(limit=500), backtest_profile)
        if _is_live_basketball_status(alert.get("status", ""))
        and _is_recent_alert(alert.get("alerted_at", ""))
    ]
    saved_match_ids = db.get_saved_bet_match_ids(limit=1000)
    latest_by_match = {}
    for alert in alerts:
        match_id = alert.get("match_id")
        if not match_id or match_id in latest_by_match:
            continue
        latest_by_match[match_id] = alert
    finished_match_ids = set(
        db.latest_finished_by_match_ids(list(latest_by_match.keys())).keys()
    )

    candidates = []
    excluded_ignored = 0
    excluded_bet = 0
    excluded_follow = 0
    excluded_saved = 0
    excluded_finished = 0

    for alert in latest_by_match.values():
        match_id = str(alert.get("match_id") or "").strip()
        if match_id in finished_match_ids:
            excluded_finished += 1
            continue
        if bool(alert.get("ignored", 0)):
            excluded_ignored += 1
            continue
        if bool(alert.get("bet_placed", 0)):
            excluded_bet += 1
            continue
        if bool(alert.get("followed", 0)):
            excluded_follow += 1
            continue
        if match_id in saved_match_ids:
            excluded_saved += 1
            continue

        direction = str(alert.get("direction") or "").strip().upper().replace("UST", "ÜST")
        if direction not in {"ALT", "ÜST"}:
            continue
        if not bool(alert.get("telegram_eligible")):
            continue

        opening = _safe_float(alert.get("opening"))
        live = _safe_float(alert.get("live"))
        diff = _safe_float(alert.get("diff"))
        fair_line = alert.get("fair_line")
        fair_edge = _safe_float(alert.get("fair_edge")) if alert.get("fair_edge") is not None else None
        projected = alert.get("projected")
        history_total = alert.get("history_total")
        opening_gap = round(live - opening, 1)
        signal_priority = abs(diff)
        fair_priority = abs(fair_edge) if fair_edge is not None else 0
        signal_priority += fair_priority / 5
        candidates.append({
            "match_id": alert.get("match_id"),
            "match_name": alert.get("match_name", ""),
            "tournament": alert.get("tournament", ""),
            "url": alert.get("url", ""),
            "direction": direction,
            "signal_tier": "Gelen Sinyal",
            "signal_code": f"{direction}-SİNYAL",
            "opening": round(opening, 1),
            "live": round(live, 1),
            "projected": round(float(projected), 1) if projected is not None else None,
            "fair_line": round(float(fair_line), 1) if fair_line is not None else None,
            "fair_edge": round(fair_edge, 1) if fair_edge is not None else None,
            "history_total": round(float(history_total), 1) if history_total is not None else None,
            "recommendation": alert.get("recommendation", ""),
            "status": alert.get("status", ""),
            "score": alert.get("score", ""),
            "opening_gap": opening_gap,
            "diff": round(diff, 1),
            "signal_priority": signal_priority,
            "bet_placed": int(alert.get("bet_placed") or 0),
            "followed": int(alert.get("followed") or 0),
            "ignored": int(alert.get("ignored") or 0),
        })

    candidates.sort(
        key=lambda item: (item["signal_priority"], item["diff"]),
        reverse=True,
    )

    eligible_candidates = candidates
    leg_count = min(max(max_count, 1), len(eligible_candidates))
    can_build = leg_count >= 1
    slip = eligible_candidates[:leg_count] if can_build else []

    if not can_build:
        message = (
            f"Kupon oluşturulmadı. En az 1 uygun sinyal gerekiyor; şu an "
            f"{len(eligible_candidates)} sinyal uygun göründü."
        )
    else:
        message = (
            f"Kupon hazır. Kalan {len(eligible_candidates)} sinyal içinden "
            f"en büyük barem farkına sahip {leg_count} seçim alındı."
        )

    excluded_total = (
        excluded_finished + excluded_ignored + excluded_bet + excluded_follow + excluded_saved
    )
    if excluded_total > 0:
        message += (
            f" {excluded_total} maç daha önce işaretlendiği/kaydedildiği için otomatik dışlandı "
            f"(Biten: {excluded_finished}, Pas geçilen: {excluded_ignored}, Bahis: {excluded_bet}, "
            f"Takip: {excluded_follow}, Eski kupon: {excluded_saved})."
        )

    return {
        "created": can_build,
        "requested_max_count": max(max_count, 1),
        "selected_count": leg_count if can_build else 0,
        "eligible_count": len(eligible_candidates),
        "total_candidates": len(candidates),
        "excluded_count": excluded_total,
        "message": message,
        "slip": slip,
    }


def _normalize_saved_bet_result(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"başarılı", "basarili", "success"}:
        return "Başarılı"
    if normalized in {"başarısız", "basarisiz", "fail", "failed"}:
        return "Başarısız"
    return ""


def normalize_bet_builder_payload(raw_payload: dict | None) -> dict | None:
    if not isinstance(raw_payload, dict):
        return None

    def safe_int(value, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    raw_slip = raw_payload.get("slip")
    if not isinstance(raw_slip, list):
        raw_slip = []

    numeric_float_fields = {
        "opening", "live", "projected", "fair_line", "fair_edge",
        "history_total", "opening_gap", "diff", "signal_priority",
    }
    numeric_int_fields = set()
    keep_fields = {
        "match_id",
        "match_name",
        "tournament",
        "url",
        "direction",
        "signal_tier",
        "signal_code",
        "status",
        "score",
        "recommendation",
    }
    bool_fields = {"bet_placed", "followed", "ignored"}

    slip: list[dict] = []
    for raw_item in raw_slip:
        if not isinstance(raw_item, dict):
            continue
        item: dict = {}
        for key in keep_fields:
            item[key] = str(raw_item.get(key) or "")
        for key in numeric_float_fields:
            try:
                item[key] = round(float(raw_item.get(key) or 0), 1)
            except (TypeError, ValueError):
                item[key] = 0.0
        for key in numeric_int_fields:
            try:
                item[key] = int(raw_item.get(key) or 0)
            except (TypeError, ValueError):
                item[key] = 0
        for key in bool_fields:
            item[key] = 1 if bool(raw_item.get(key)) else 0
        item["result"] = _normalize_saved_bet_result(raw_item.get("result") or "")
        slip.append(item)

    requested_max_count = safe_int(raw_payload.get("requested_max_count") or len(slip) or 1, 1)
    selected_count = safe_int(raw_payload.get("selected_count") or len(slip), len(slip))
    eligible_count = safe_int(raw_payload.get("eligible_count") or len(slip), len(slip))
    total_candidates = safe_int(raw_payload.get("total_candidates") or eligible_count, eligible_count)

    return {
        "created": bool(raw_payload.get("created")) and len(slip) > 0,
        "requested_max_count": max(1, min(requested_max_count, 8)),
        "selected_count": max(0, selected_count),
        "eligible_count": max(0, eligible_count),
        "total_candidates": max(0, total_candidates),
        "excluded_count": max(0, safe_int(raw_payload.get("excluded_count"), 0)),
        "message": str(raw_payload.get("message") or ""),
        "slip": slip,
    }


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/deleted-matches")
def deleted_matches():
    return render_template("deleted_matches.html")


@app.route("/api/alerts")
def api_alerts():
    backtest_profile = build_backtest_profile(db.recent_deleted_alerts(limit=None))
    return jsonify(enrich_alerts_with_analysis(db.recent_alerts(limit=500), backtest_profile))


def _lightweight_enrich_deleted_alerts(alerts: list[dict], backtest_profile: dict | None = None) -> list[dict]:
    for alert in alerts:
        analysis = _parse_analysis(alert.get("ai_analysis"))
        alert.pop("ai_analysis", None)
        if not analysis:
            analysis = {}
        elif backtest_profile is not None:
            analysis = enrich_analysis_with_backtest(alert, analysis, backtest_profile, config.THRESHOLD)
        analysis = _sanitize_recent_form_analysis(analysis)
        if isinstance(analysis.get("backtest"), dict):
            analysis = {**analysis, "backtest": _trim_backtest_payload(analysis.get("backtest"))}
        alert["analysis"] = analysis
        alert["fair_line"] = analysis.get("fair_line")
        alert["fair_edge"] = analysis.get("fair_edge")
        alert["projected"] = analysis.get("projected_total")
        alert["market_total"] = analysis.get("market_total")
        alert["team_recent_total"] = analysis.get("team_recent_total")
        alert["home_last6"] = analysis.get("home_last6") or {}
        alert["away_last6"] = analysis.get("away_last6") or {}
        alert["h2h_total"] = analysis.get("h2h_total")
        alert["history_total"] = analysis.get("history_total")
        alert["recommendation"] = analysis.get("recommendation") or ""
        alert["legacy_direction"] = analysis.get("legacy_direction") or alert.get("direction")
        alert["final_direction"] = analysis.get("final_direction") or analysis.get("direction") or alert.get("direction")
        alert["signal_scores"] = analysis.get("signal_scores") or {}
        alert["signal_votes"] = analysis.get("signal_votes") if isinstance(analysis.get("signal_votes"), list) else []
        alert["backtest"] = _trim_backtest_payload(analysis.get("backtest"))
        alert["telegram_eligible"] = bool(analysis.get("telegram_eligible"))
        alert["selection_reason"] = analysis.get("selection_reason") or ""
        alert["projection_quality"] = analysis.get("projection_quality")
        alert["warnings"] = analysis.get("warnings") if isinstance(analysis.get("warnings"), list) else []
    return alerts


@app.route("/api/deleted-matches")
def api_deleted_matches():
    limit = request.args.get("limit", default=1000, type=int) or 1000
    limit = max(1, min(limit, 5000))
    backtest_profile = build_backtest_profile(db.recent_deleted_alerts(limit=None))
    return jsonify(_lightweight_enrich_deleted_alerts(db.recent_deleted_alerts(limit=limit), backtest_profile))


@app.route("/api/deleted-matches/export.csv")
def api_export_finished_deleted_matches_csv():
    deleted_rows = db.recent_deleted_alerts(limit=None)
    backtest_profile = build_backtest_profile(deleted_rows)
    rows = [
        row for row in _lightweight_enrich_deleted_alerts(deleted_rows, backtest_profile)
        if str(row.get("result") or "").strip()
    ]

    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output)
    writer.writerow([
        "Maç", "Sinyal Anı", "Sinyal Türü", "Skor",
        "Açılış", "Canlı", "Proj.", "Adil Barem", "Sonuç", "Not",
    ])

    for row in rows:
        direction = str(row.get("direction") or "").strip()
        match_name = re.sub(r"\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}\s*", "", str(row.get("match_name") or ""))
        match_name = re.sub(r"\s*betting odds\s*", "", match_name, flags=re.IGNORECASE).strip()
        projected = row.get("projected")
        projected_cell = f"{float(projected):.1f}" if projected is not None else ""
        fair_line = row.get("fair_line")
        fair_line_cell = f"{float(fair_line):.1f}" if fair_line is not None else "Hesaplanamıyor"
        writer.writerow([
            match_name,
            row.get("alert_moment") or "",
            direction,
            row.get("score") or "",
            row.get("opening") if row.get("opening") is not None else "",
            row.get("live") if row.get("live") is not None else "",
            projected_cell,
            fair_line_cell,
            row.get("result") or "",
            row.get("note") or "",
        ])

    filename = f"silinen-biten-maclar-{datetime.now().strftime('%Y%m%d-%H%M')}.csv"
    return Response(
        output.getvalue(),
        content_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.route("/api/deleted-matches/clear", methods=["POST"])
def api_clear_deleted_matches():
    deleted_count = db.purge_deleted_matches()
    return jsonify({"cleared": True, "deleted_count": deleted_count})


@app.route("/api/deleted-matches/<int:alert_id>", methods=["DELETE"])
def api_purge_deleted_alert(alert_id: int):
    removed = db.delete_alert(alert_id)
    if not removed:
        return jsonify({"error": "not found"}), 404
    return jsonify({"id": alert_id, "deleted": True})


@app.route("/api/alerts/check-finished", methods=["POST"])
def api_check_active_match_finished():
    summary = asyncio.run(run_active_match_finished_scan(db, config))
    return jsonify(summary)


@app.route("/api/deleted-matches/check-results", methods=["POST"])
def api_check_deleted_match_results():
    summary = asyncio.run(run_deleted_match_result_cycle(db, config))
    return jsonify(summary)


@app.route("/api/deleted-matches/<int:alert_id>/check-result", methods=["POST"])
def api_check_single_deleted_match_result(alert_id: int):
    summary = asyncio.run(run_single_deleted_match_result_check(db, config, alert_id))
    return jsonify(summary)


def _normalize_deleted_result(value: str) -> str:
    normalized = str(value or "").strip().lower()
    normalized = (
        normalized.replace("ı", "i").replace("ş", "s")
        .replace("ğ", "g").replace("ü", "u")
        .replace("ö", "o").replace("ç", "c")
    )
    if normalized in {"basarili", "success", "başarılı"}:
        return "Başarılı"
    if normalized in {"basarisiz", "fail", "failed", "başarısız"}:
        return "Başarısız"
    return ""


@app.route("/api/deleted-matches/<int:alert_id>/result", methods=["POST"])
def api_update_deleted_match_result(alert_id: int):
    payload = request.get_json(silent=True) or {}
    raw_result = payload.get("result", "")
    if str(raw_result or "").strip() == "":
        updated = db.update_deleted_alert_result(alert_id, "")
        if not updated:
            return jsonify({"error": "not found"}), 404
        return jsonify({"id": alert_id, "result": "", "updated": True})

    result = _normalize_deleted_result(raw_result)
    if not result:
        return jsonify({"error": "invalid_result"}), 400
    updated = db.update_deleted_alert_result(alert_id, result)
    if not updated:
        return jsonify({"error": "not found"}), 404
    return jsonify({"id": alert_id, "result": result, "updated": True})


def _pct(success: int, total: int) -> float:
    return round((success / total) * 100, 1) if total > 0 else 0.0


def _fold(value) -> str:
    return (
        str(value or "").strip().lower()
        .replace("ı", "i").replace("ş", "s")
        .replace("ğ", "g").replace("ü", "u")
        .replace("ö", "o").replace("ç", "c")
    )


def _bucket_stats(rows: list) -> dict:
    success = sum(1 for r in rows if _fold(r.get("result")) == "basarili")
    fail = sum(1 for r in rows if _fold(r.get("result")) == "basarisiz")
    resolved = success + fail
    return {
        "total": len(rows),
        "resolved": resolved,
        "success": success,
        "fail": fail,
        "rate": _pct(success, resolved),
        "fade_rate": _pct(fail, resolved),
    }


def _dir_stats(signals: list, direction: str) -> dict:
    scoped = [s for s in signals if _fold(s.get("direction")) == direction]
    stats = _bucket_stats(scoped)
    return {
        "total": stats["total"],
        "success": stats["success"],
        "fail": stats["fail"],
        "success_rate": stats["rate"],
    }


def _diff_bucket(value) -> str:
    try:
        v = abs(float(value))
    except (TypeError, ValueError):
        return "Bilinmiyor"
    if v < 1.0:
        return "0 – 1.0"
    if v < 2.0:
        return "1.0 – 2.0"
    if v < 3.0:
        return "2.0 – 3.0"
    return "3.0+"


_DIFF_BUCKET_ORDER = ["0 – 1.0", "1.0 – 2.0", "2.0 – 3.0", "3.0+", "Bilinmiyor"]


def _period_bucket(status) -> str:
    s = _fold(status)
    if not s:
        return "Bilinmiyor"
    if "devre arasi" in s or s in {"ht", "half time", "half-time", "mola"}:
        return "Devre Arası"
    if "uzatma" in s or "ot" == s or s.startswith("ot ") or "overtime" in s:
        return "Uzatma"
    for n, label in [("1", "1. Çeyrek"), ("2", "2. Çeyrek"), ("3", "3. Çeyrek"), ("4", "4. Çeyrek")]:
        if s.startswith(f"{n}.") or s.startswith(f"{n}c") or f"q{n}" in s or f"{n}. ceyrek" in s or s == n:
            return label
    return "Bilinmiyor"


_PERIOD_BUCKET_ORDER = [
    "1. Çeyrek", "2. Çeyrek", "Devre Arası", "3. Çeyrek", "4. Çeyrek", "Uzatma", "Bilinmiyor",
]


def _parse_ts(value):
    if not value:
        return None
    text = str(value).strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _fade_verdict(original_rate: float, fade_rate: float, resolved: int) -> str:
    if resolved < 5:
        return "Örneklem yetersiz, fade yorumu için daha çok sonuç gerekli."
    delta = fade_rate - original_rate
    if abs(delta) < 3:
        return "Fade ile aynı sonuç — ters çevirmek anlamlı değil."
    if delta > 0:
        return f"Ters sinyal %{delta:.1f} puan daha iyi: stratejiyi fade etmeyi ciddi değerlendir."
    return f"Orijinal yön %{-delta:.1f} puan önde: mevcut mantığa sadık kal."


def build_deleted_matches_report(signals: list) -> dict:
    if not signals:
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "headline": "Rapor için yeterli silinen maç verisi yok.",
            "summary": "Silinen maçlara sinyal başarı sonucu işaretlendikçe burada detaylı analiz görünecek.",
            "cards": [],
            "highlights": [],
            "mixed_examples": [],
            "diff_buckets": [],
            "period_buckets": [],
            "fade_analysis": None,
            "tournament_top": [],
            "tournament_bottom": [],
            "trend_7d": None,
            "actions": [],
        }

    total = len(signals)
    success_count = sum(1 for s in signals if _fold(s.get("result")) == "basarili")
    fail_count = sum(1 for s in signals if _fold(s.get("result")) == "basarisiz")
    pending_count = total - success_count - fail_count
    resolved = success_count + fail_count

    alt_stats = _dir_stats(signals, "alt")
    ust_stats = _dir_stats(signals, "üst")

    match_ids = {}
    for s in signals:
        mid = str(s.get("match_id") or s.get("id"))
        if mid not in match_ids:
            match_ids[mid] = []
        match_ids[mid].append(s)

    unique_matches = len(match_ids)
    mixed_matches = [
        (mid, rows) for mid, rows in match_ids.items()
        if any(_fold(r.get("result")) == "basarili" for r in rows)
        and any(_fold(r.get("result")) == "basarisiz" for r in rows)
    ]

    first_signals = [s for s in signals if int(s.get("signal_count") or 1) == 1]
    first_success = sum(1 for s in first_signals if _fold(s.get("result")) == "basarili")
    first_resolved = sum(1 for s in first_signals if _fold(s.get("result")) in {"basarili", "basarisiz"})

    repeat_signals = [s for s in signals if int(s.get("signal_count") or 1) >= 2]
    repeat_success = sum(1 for s in repeat_signals if _fold(s.get("result")) == "basarili")
    repeat_resolved = sum(1 for s in repeat_signals if _fold(s.get("result")) in {"basarili", "basarisiz"})

    overall_rate = _pct(success_count, resolved)
    overall_fade_rate = _pct(fail_count, resolved)
    dir_diff = abs(alt_stats["success_rate"] - ust_stats["success_rate"])

    diff_groups: dict[str, list] = {label: [] for label in _DIFF_BUCKET_ORDER}
    for s in signals:
        diff_groups[_diff_bucket(s.get("diff"))].append(s)
    diff_buckets = []
    for label in _DIFF_BUCKET_ORDER:
        rows = diff_groups.get(label) or []
        if not rows:
            continue
        stats = _bucket_stats(rows)
        stats["label"] = label
        diff_buckets.append(stats)

    period_groups: dict[str, list] = {label: [] for label in _PERIOD_BUCKET_ORDER}
    for s in signals:
        period_groups[_period_bucket(s.get("alert_moment") or s.get("status"))].append(s)
    period_buckets = []
    for label in _PERIOD_BUCKET_ORDER:
        rows = period_groups.get(label) or []
        if not rows:
            continue
        stats = _bucket_stats(rows)
        stats["label"] = label
        period_buckets.append(stats)

    fade_analysis = {
        "overall": {
            "original_rate": overall_rate,
            "fade_rate": overall_fade_rate,
            "resolved": resolved,
            "success": success_count,
            "fail": fail_count,
            "verdict": _fade_verdict(overall_rate, overall_fade_rate, resolved),
        },
        "by_direction": [
            {
                "label": "ALT",
                "resolved": alt_stats["success"] + alt_stats["fail"],
                "original_rate": alt_stats["success_rate"],
                "fade_rate": _pct(alt_stats["fail"], alt_stats["success"] + alt_stats["fail"]),
            },
            {
                "label": "ÜST",
                "resolved": ust_stats["success"] + ust_stats["fail"],
                "original_rate": ust_stats["success_rate"],
                "fade_rate": _pct(ust_stats["fail"], ust_stats["success"] + ust_stats["fail"]),
            },
        ],
        "by_diff": [
            {
                "label": b["label"],
                "resolved": b["resolved"],
                "original_rate": b["rate"],
                "fade_rate": b["fade_rate"],
            }
            for b in diff_buckets if b["resolved"] > 0
        ],
    }

    tour_groups: dict[str, list] = {}
    for s in signals:
        name = (s.get("tournament") or "").strip()
        if not name:
            continue
        tour_groups.setdefault(name, []).append(s)
    tour_stats = []
    for name, rows in tour_groups.items():
        st = _bucket_stats(rows)
        if st["resolved"] < 3:
            continue
        st["label"] = name
        tour_stats.append(st)
    tour_stats.sort(key=lambda x: (-x["rate"], -x["resolved"]))
    tournament_top = tour_stats[:3]
    tournament_bottom = sorted(tour_stats, key=lambda x: (x["rate"], -x["resolved"]))[:3]

    now = datetime.now()
    last7_cutoff = now - timedelta(days=7)
    prior_cutoff = now - timedelta(days=14)
    last7 = [s for s in signals if (_parse_ts(s.get("deleted_at")) or _parse_ts(s.get("alerted_at"))) and
             (_parse_ts(s.get("deleted_at")) or _parse_ts(s.get("alerted_at"))) >= last7_cutoff]
    prior = [s for s in signals if (_parse_ts(s.get("deleted_at")) or _parse_ts(s.get("alerted_at"))) and
             prior_cutoff <= (_parse_ts(s.get("deleted_at")) or _parse_ts(s.get("alerted_at"))) < last7_cutoff]
    last7_stats = _bucket_stats(last7)
    prior_stats = _bucket_stats(prior)
    if last7_stats["resolved"] == 0 and prior_stats["resolved"] == 0:
        trend_7d = None
    else:
        delta = round(last7_stats["rate"] - prior_stats["rate"], 1) if prior_stats["resolved"] > 0 else None
        if delta is None:
            verdict = "Son 7 günde sonuç var, önceki haftayla kıyas için yeterli veri yok."
        elif abs(delta) < 3:
            verdict = "Son hafta genele yakın — trend yatay."
        elif delta > 0:
            verdict = f"Son hafta %{delta:.1f} puan yukarı — iyileşme var."
        else:
            verdict = f"Son hafta %{-delta:.1f} puan aşağı — performans bozuluyor."
        trend_7d = {
            "last7": last7_stats,
            "prior": prior_stats,
            "delta": delta,
            "verdict": verdict,
        }

    actions: list[str] = []

    if resolved >= 5:
        if overall_fade_rate - overall_rate >= 6:
            actions.append(
                f"Genel tablo fade önerisi: ters sinyal %{overall_fade_rate:.1f} vs orijinal %{overall_rate:.1f}. "
                f"Mevcut mantık sistematik yanlış yönde çalışıyor olabilir."
            )
        elif abs(overall_fade_rate - overall_rate) < 3:
            actions.append(
                "Genel başarı %50 civarında — sistem neredeyse yazı-tura. "
                "Fark bandı ve periyotla örneklem daraltılmalı."
            )

    for d in fade_analysis["by_direction"]:
        if d["resolved"] >= 5 and d["fade_rate"] - d["original_rate"] >= 8:
            actions.append(
                f"{d['label']} sinyallerini fade et: ters %{d['fade_rate']:.1f} vs orijinal %{d['original_rate']:.1f} "
                f"({d['resolved']} sonuçlanan)."
            )

    worst_diff = None
    best_diff = None
    for b in diff_buckets:
        if b["resolved"] < 4:
            continue
        if worst_diff is None or b["rate"] < worst_diff["rate"]:
            worst_diff = b
        if best_diff is None or b["rate"] > best_diff["rate"]:
            best_diff = b
    if worst_diff and worst_diff["rate"] < 40:
        actions.append(
            f"Fark {worst_diff['label']} aralığında başarı %{worst_diff['rate']:.1f} — "
            f"bu aralıkta sinyal almamayı veya eşiği yükseltmeyi değerlendir."
        )
    if best_diff and best_diff is not worst_diff and best_diff["rate"] >= 60:
        actions.append(
            f"Fark {best_diff['label']} aralığı en güçlü segment (%{best_diff['rate']:.1f}) — "
            f"bahisleri buraya yoğunlaştır."
        )

    worst_period = None
    for b in period_buckets:
        if b["resolved"] < 4 or b["label"] == "Bilinmiyor":
            continue
        if worst_period is None or b["rate"] < worst_period["rate"]:
            worst_period = b
    if worst_period and worst_period["rate"] < 40:
        actions.append(
            f"{worst_period['label']} sinyalleri %{worst_period['rate']:.1f} başarı — "
            f"bu periyotta sinyalleri filtrele."
        )

    if tournament_bottom:
        worst_t = tournament_bottom[0]
        if worst_t["rate"] < 30 and worst_t["resolved"] >= 4:
            actions.append(
                f"{worst_t['label']}: %{worst_t['rate']:.1f} ({worst_t['success']}/{worst_t['resolved']}) — "
                f"bu turnuvayı kara listeye al."
            )
    if tournament_top:
        best_t = tournament_top[0]
        if best_t["rate"] >= 65 and best_t["resolved"] >= 4:
            actions.append(
                f"{best_t['label']}: %{best_t['rate']:.1f} ({best_t['success']}/{best_t['resolved']}) — "
                f"en güvenilir liga, önceliklendir."
            )

    if trend_7d and trend_7d.get("delta") is not None and trend_7d["delta"] <= -8:
        actions.append(
            f"Son 7 günde başarı %{-trend_7d['delta']:.1f} puan düştü — "
            f"son değişiklikleri gözden geçir."
        )

    if not actions and resolved > 0:
        actions.append("Belirgin bir aksiyon sinyali yok — mevcut stratejiye devam, örneklemi büyüt.")

    if resolved == 0:
        headline = "Sonuçlar işaretlenmemiş. Sinyal başarılarını işaretleyerek raporu oluşturun."
    elif overall_fade_rate - overall_rate >= 6:
        headline = (
            f"Ters sinyal orijinalden %{overall_fade_rate - overall_rate:.1f} puan önde — "
            f"sistem mantığı tersine çalışıyor olabilir."
        )
    elif overall_rate >= 60:
        headline = f"Genel başarı %{overall_rate:.1f} — sinyaller çalışıyor."
    elif overall_rate >= 50:
        headline = f"Genel başarı %{overall_rate:.1f} — dengeli, net avantaj yok."
    else:
        headline = f"Genel başarı %{overall_rate:.1f} — strateji gözden geçirilmeli."

    if dir_diff < 5:
        dir_summary = "ALT ve ÜST performansı birbirine yakın."
    elif alt_stats["success_rate"] > ust_stats["success_rate"]:
        dir_summary = f"ALT tarafı {dir_diff:.1f} puan önde."
    else:
        dir_summary = f"ÜST tarafı {dir_diff:.1f} puan önde."

    summary = (
        f"Toplam {total} sinyal, {unique_matches} benzersiz maçtan. "
        f"Genel başarı %{overall_rate:.1f} — ters oynansaydı %{overall_fade_rate:.1f} olurdu "
        f"({resolved} sonuçlanan). {dir_summary} Karışık maç {len(mixed_matches)}, "
        f"sonuç bekleyen {pending_count}."
    )

    cards = [
        {
            "title": "Toplam Sinyal",
            "value": total,
            "detail": f"{unique_matches} benzersiz maç, {pending_count} sonuç bekliyor.",
        },
        {
            "title": "Genel Başarı",
            "value": f"%{overall_rate:.1f}",
            "detail": f"{success_count} başarılı, {fail_count} başarısız.",
        },
        {
            "title": "Fade Etseydik",
            "value": f"%{overall_fade_rate:.1f}",
            "detail": f"Her sinyalin tersi oynansa {fail_count} başarılı olurdu.",
        },
        {
            "title": "ALT / ÜST",
            "value": f"%{alt_stats['success_rate']:.1f} / %{ust_stats['success_rate']:.1f}",
            "detail": f"ALT {alt_stats['success']}/{alt_stats['total']} | ÜST {ust_stats['success']}/{ust_stats['total']}.",
        },
        {
            "title": "#1 Sinyal",
            "value": f"%{_pct(first_success, first_resolved):.1f}",
            "detail": f"{len(first_signals)} ilk sinyal kaydı.",
        },
        {
            "title": "#2+ Tekrar",
            "value": f"%{_pct(repeat_success, repeat_resolved):.1f}",
            "detail": f"{len(repeat_signals)} tekrar sinyal kaydı.",
        },
        {
            "title": "Karışık Maç",
            "value": len(mixed_matches),
            "detail": "Hem başarılı hem başarısız sinyal içeren maç.",
        },
    ]

    highlights = [
        f"ALT başarı %{alt_stats['success_rate']:.1f} ({alt_stats['success']}/{alt_stats['total']}), "
        f"ÜST başarı %{ust_stats['success_rate']:.1f} ({ust_stats['success']}/{ust_stats['total']}).",

        f"İlk sinyal (#1) başarı oranı %{_pct(first_success, first_resolved):.1f}, "
        f"tekrar sinyaller (#2+) başarı oranı %{_pct(repeat_success, repeat_resolved):.1f}.",

        fade_analysis["overall"]["verdict"],
    ]

    mixed_examples = []
    for mid, rows in sorted(mixed_matches, key=lambda x: -len(x[1]))[:5]:
        sigs_ok = [r for r in rows if _fold(r.get("result")) == "basarili"]
        sigs_fail = [r for r in rows if _fold(r.get("result")) == "basarisiz"]
        match_name = rows[0].get("match_name") or "-"
        tournament = rows[0].get("tournament") or "-"
        parts = []
        if sigs_fail:
            parts.append("Başarısız: " + ", ".join(f"#{r.get('signal_count', 1)}" for r in sigs_fail))
        if sigs_ok:
            parts.append("Başarılı: " + ", ".join(f"#{r.get('signal_count', 1)}" for r in sigs_ok))
        mixed_examples.append({
            "match_name": match_name,
            "tournament": tournament,
            "signal_count": len(rows),
            "summary": " | ".join(parts),
        })

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "headline": headline,
        "summary": summary,
        "cards": cards,
        "highlights": highlights,
        "mixed_examples": mixed_examples,
        "diff_buckets": diff_buckets,
        "period_buckets": period_buckets,
        "fade_analysis": fade_analysis,
        "tournament_top": tournament_top,
        "tournament_bottom": tournament_bottom,
        "trend_7d": trend_7d,
        "actions": actions,
    }


@app.route("/api/deleted-matches/report")
def api_deleted_matches_report():
    signals = db.recent_deleted_alerts(limit=None)
    return jsonify(build_deleted_matches_report(signals))


@app.route("/api/bet-builder")
def api_bet_builder():
    max_count = request.args.get("max_count", default=4, type=int) or 4
    max_count = max(1, min(max_count, 8))
    return jsonify(build_bet_builder(max_count))


@app.route("/api/bet-builder/save", methods=["POST"])
def api_bet_builder_save():
    data = request.get_json(silent=True) or {}
    name = str(data.get("name") or "").strip()
    payload = normalize_bet_builder_payload(data.get("payload"))

    if not name:
        return jsonify({"error": "Kupon ismi bos olamaz."}), 400
    if not payload or not payload.get("slip"):
        return jsonify({"error": "Kaydedilecek gecerli kupon bulunamadi."}), 400

    saved_id = db.save_bet_slip(name=name, payload=payload)
    return jsonify({"saved": True, "id": saved_id, "name": name})


@app.route("/api/bet-builder/saved")
def api_saved_bet_builder_list():
    limit = request.args.get("limit", default=30, type=int) or 30
    limit = max(1, min(limit, 200))
    return jsonify(db.list_saved_bet_slips(limit=limit))


@app.route("/api/bet-builder/saved/<int:slip_id>", methods=["DELETE"])
def api_saved_bet_builder_delete(slip_id: int):
    if not db.delete_saved_bet_slip(slip_id):
        return jsonify({"error": "not found"}), 404
    return jsonify({"deleted": True, "id": slip_id})


@app.route("/api/bet-builder/saved/<int:slip_id>/result", methods=["POST"])
def api_saved_bet_builder_result(slip_id: int):
    saved = db.get_saved_bet_slip(slip_id)
    if not saved:
        return jsonify({"error": "not found"}), 404

    data = request.get_json(silent=True) or {}
    match_id = str(data.get("match_id") or "").strip()
    result = _normalize_saved_bet_result(data.get("result") or "")
    if not match_id:
        return jsonify({"error": "match_id is required"}), 400
    if not result:
        return jsonify({"error": "invalid result"}), 400

    updated = db.update_saved_bet_slip_result(slip_id, match_id, result)
    if not updated:
        return jsonify({"error": "match_not_found"}), 404

    saved = db.get_saved_bet_slip(slip_id)
    return jsonify({
        "updated": True,
        "id": slip_id,
        "match_id": match_id,
        "result": result,
        "payload": saved.get("payload") if saved else {},
    })


@app.route("/api/matches/<path:match_id>/ignore", methods=["POST"])
def api_ignore_match(match_id: str):
    match_key = str(match_id or "").strip()
    if not match_key:
        return jsonify({"error": "match_id is required"}), 400

    affected = db.set_match_statuses(
        match_key,
        ignored=True,
        bet_placed=False,
        followed=False,
    )
    return jsonify({
        "match_id": match_key,
        "ignored": 1,
        "bet_placed": 0,
        "followed": 0,
        "affected": affected,
    })


@app.route("/api/alerts/<int:alert_id>/bet", methods=["POST"])
def api_toggle_bet(alert_id: int):
    alert = db.get_alert(alert_id)
    if not alert:
        return jsonify({"error": "not found"}), 404
    new_val = not bool(alert["bet_placed"])
    affected = db.set_match_statuses(
        alert["match_id"],
        bet_placed=new_val,
        ignored=False if new_val else None,
        followed=False if new_val else None,
    )
    return jsonify({
        "id": alert_id,
        "match_id": alert["match_id"],
        "bet_placed": int(new_val),
        "ignored": 0 if new_val else None,
        "followed": 0 if new_val else None,
        "affected": affected,
    })


@app.route("/api/alerts/<int:alert_id>/ignore", methods=["POST"])
def api_toggle_ignore(alert_id: int):
    alert = db.get_alert(alert_id)
    if not alert:
        return jsonify({"error": "not found"}), 404
    new_val = not bool(alert["ignored"])
    affected = db.set_match_statuses(
        alert["match_id"],
        ignored=new_val,
        bet_placed=False if new_val else None,
        followed=False if new_val else None,
    )
    return jsonify({
        "id": alert_id,
        "match_id": alert["match_id"],
        "ignored": int(new_val),
        "bet_placed": 0 if new_val else None,
        "followed": 0 if new_val else None,
        "affected": affected,
    })


@app.route("/api/alerts/<int:alert_id>/follow", methods=["POST"])
def api_toggle_follow(alert_id: int):
    alert = db.get_alert(alert_id)
    if not alert:
        return jsonify({"error": "not found"}), 404
    new_val = not bool(alert.get("followed", 0))
    affected = db.set_match_statuses(
        alert["match_id"],
        followed=new_val,
        bet_placed=False if new_val else None,
        ignored=False if new_val else None,
    )
    return jsonify({
        "id": alert_id,
        "match_id": alert["match_id"],
        "followed": int(new_val),
        "bet_placed": 0 if new_val else None,
        "ignored": 0 if new_val else None,
        "affected": affected,
    })


@app.route("/api/alerts/<int:alert_id>/note", methods=["POST"])
def api_update_alert_note(alert_id: int):
    alert = db.get_alert(alert_id)
    if not alert:
        return jsonify({"error": "not found"}), 404

    data = request.get_json(silent=True) or {}
    note = str(data.get("note") or "").strip()
    if len(note) > 240:
        note = note[:240]

    affected = db.update_match_note(alert["match_id"], note)
    return jsonify({
        "id": alert_id,
        "match_id": alert["match_id"],
        "note": note,
        "affected": affected,
    })


@app.route("/api/alerts/<int:alert_id>", methods=["DELETE"])
def api_delete_alert(alert_id: int):
    alert = db.get_alert(alert_id)
    if not alert:
        return jsonify({"error": "not found"}), 404
    deleted_count = db.delete_match_data(alert["match_id"])
    return jsonify({
        "id": alert_id,
        "match_id": alert["match_id"],
        "deleted": True,
        "affected": deleted_count,
    })


@app.route("/api/clear", methods=["POST"])
def api_clear_db():
    moved_count = db.clear_all()
    return jsonify({"cleared": True, "moved_count": moved_count})


if __name__ == "__main__":
    port = int(os.getenv("DASHBOARD_PORT", "5050"))
    app.run(host="0.0.0.0", port=port, debug=False)
