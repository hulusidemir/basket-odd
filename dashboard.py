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
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from flask import Flask, Response, jsonify, render_template, request
from db import Database
from config import Config
from finished_match_service import (
    run_active_match_finished_scan,
    run_deleted_match_result_cycle,
    run_single_deleted_match_result_check,
)
from aiscore_scraper import AiscoreScraper
from signal_analysis import build_backtest_profile, build_signal_analysis, enrich_analysis_with_backtest
from signal_buckets import build_signal_bucket_profile, matching_signal_buckets
from signal_lists import build_quality_tag, build_signal_list_markers, build_signal_list_profile
from signal_quality import calculate_signal_quality

logger = logging.getLogger(__name__)

config = Config()
db = Database(config.DB_PATH)
db.init()
app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["TEMPLATES_AUTO_RELOAD"] = True

from upcoming_app import upcoming_bp
app.register_blueprint(upcoming_bp)

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


_TOURNAMENT_PROMO_RE = re.compile(
    r"\bstandings\b|\bpopular\b|\btrending\b|\bfeatured\b|\bepl\b",
    re.I,
)


def _sanitize_tournament_display(value: str) -> str:
    """Strip clearly contaminated tournament strings before they hit the UI.

    Older DB rows can hold values like "EPL Standings 2024-25 : CBA" produced
    by the previous scraper bug. Show "-" instead of the corrupted label.
    """
    text = str(value or "").strip()
    if not text:
        return ""
    if _TOURNAMENT_PROMO_RE.search(text):
        return ""
    return text


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


def _drop_recent_form_analysis(analysis: dict) -> dict:
    if not isinstance(analysis, dict):
        return analysis or {}
    cleaned = {
        **analysis,
        "home_last6": {},
        "away_last6": {},
        "team_recent_total": None,
    }
    if isinstance(cleaned.get("team_context"), dict):
        team_context = dict(cleaned["team_context"])
        for key in (
            "expected_total",
            "regression_delta",
            "regression_direction",
            "regression_note",
            "home_profile",
            "away_profile",
            "home_last6_profile",
            "away_last6_profile",
        ):
            team_context.pop(key, None)
        cleaned["team_context"] = team_context
    if isinstance(cleaned.get("fair_components"), dict):
        cleaned["fair_components"] = {**cleaned["fair_components"], "team_recent": None}
    for key in ("warnings", "h2h_quality_notes", "projection_notes"):
        if isinstance(cleaned.get(key), list):
            cleaned[key] = [
                note for note in cleaned[key]
                if not re.search(r"\bSF\b|son[-\s]?form|son\s+3-4-5|son\s+6", str(note), re.I)
            ]
    return cleaned


def _valid_basket_total(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 60 else None


def _sanitize_h2h_score_rows(rows) -> list[dict]:
    if not isinstance(rows, list):
        return []
    cleaned_rows: list[dict] = []
    seen: set[tuple] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            home = int(row.get("home"))
            away = int(row.get("away"))
        except (TypeError, ValueError):
            continue
        total = home + away
        if not (20 <= home <= 180 and 20 <= away <= 180 and 100 <= total <= 320):
            continue
        date = str(row.get("date") or "").strip()
        key = (date, home, away)
        if key in seen:
            continue
        seen.add(key)
        cleaned_rows.append({
            "date": date,
            "home": home,
            "away": away,
            "total": total,
        })
        if len(cleaned_rows) >= 12:
            break
    return cleaned_rows


def _sanitize_h2h_row_totals(values) -> list[int]:
    if not isinstance(values, list):
        return []
    totals: list[int] = []
    for value in values:
        try:
            total = int(value)
        except (TypeError, ValueError):
            continue
        if 100 <= total <= 320 and total not in totals:
            totals.append(total)
        if len(totals) >= 12:
            break
    return totals


def _sanitize_h2h_analysis(analysis: dict) -> dict:
    if not isinstance(analysis, dict):
        return analysis or {}
    h2h_total = _valid_basket_total(analysis.get("h2h_total"))
    history_total = _valid_basket_total(analysis.get("history_total"))
    h2h_score_rows = _sanitize_h2h_score_rows(analysis.get("h2h_score_rows"))
    h2h_row_totals = [row["total"] for row in h2h_score_rows] or _sanitize_h2h_row_totals(analysis.get("h2h_row_totals"))
    cleaned = {
        **analysis,
        "h2h_total": h2h_total,
        "history_total": history_total,
        "h2h_score_rows": h2h_score_rows,
        "h2h_row_totals": h2h_row_totals,
    }
    if isinstance(cleaned.get("team_context"), dict):
        team_context = dict(cleaned["team_context"])
        team_context["h2h_avg_total"] = _valid_basket_total(team_context.get("h2h_avg_total"))
        if team_context.get("h2h_avg_total") is None and team_context.get("h2h_over_pct") is None:
            team_context.pop("h2h_note", None)
        cleaned["team_context"] = team_context
    if isinstance(cleaned.get("fair_components"), dict):
        cleaned["fair_components"] = {**cleaned["fair_components"], "h2h": h2h_total}
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


def _build_league_quality_profile(rows: list[dict]) -> dict:
    grouped: dict[str, dict] = {}
    for row in rows or []:
        result_key = _fold(row.get("result"))
        if result_key not in {"basarili", "basarisiz"}:
            continue
        tournament = str(row.get("tournament") or "").strip()
        if not tournament:
            continue
        bucket = grouped.setdefault(tournament, {"resolved": 0, "success": 0})
        bucket["resolved"] += 1
        if result_key == "basarili":
            bucket["success"] += 1
    profile = {}
    for tournament, bucket in grouped.items():
        resolved = int(bucket.get("resolved") or 0)
        if resolved <= 0:
            continue
        profile[tournament] = {
            "resolved": resolved,
            "success": int(bucket.get("success") or 0),
            "rate": round((float(bucket.get("success") or 0) / resolved) * 100, 1),
        }
    return profile


def _previous_directions_by_alert_id(alerts: list[dict]) -> dict[int, list[str]]:
    history: dict[str, list[str]] = {}
    previous: dict[int, list[str]] = {}

    def sort_key(row: dict):
        ts = _parse_ts(row.get("alerted_at")) or datetime.min
        return (ts, int(row.get("id") or 0))

    for row in sorted(alerts or [], key=sort_key):
        alert_id = int(row.get("id") or 0)
        match_id = str(row.get("match_id") or "").strip()
        previous[alert_id] = list(history.get(match_id) or [])
        direction = _normalize_direction(row.get("final_direction") or row.get("direction"))
        if match_id and direction in {"ALT", "ÜST"}:
            history.setdefault(match_id, []).append(direction)
    return previous


def _apply_signal_quality(alert: dict, league_profile: dict | None, previous_directions: list[str] | None = None) -> dict:
    tournament = str(alert.get("tournament") or "").strip()
    league_stats = (league_profile or {}).get(tournament) or {}
    quality = calculate_signal_quality({
        **alert,
        "direction": alert.get("final_direction") or alert.get("direction"),
        "league_success_rate": league_stats.get("rate"),
        "league_success_samples": league_stats.get("resolved"),
        "previous_directions": previous_directions or [],
    })
    alert["signal_quality"] = quality
    alert["signal_quality_score"] = quality.get("quality_score")
    alert["signal_quality_label"] = quality.get("quality_label")
    alert["signal_quality_reason"] = quality.get("reason")
    alert["signal_quality_risk_note"] = quality.get("risk_note")
    return quality


def _apply_stored_signal_quality(alert: dict, analysis: dict) -> dict:
    quality = analysis.get("signal_quality") if isinstance(analysis.get("signal_quality"), dict) else {}
    alert["signal_quality"] = quality
    alert["signal_quality_score"] = quality.get("quality_score")
    alert["signal_quality_label"] = quality.get("quality_label")
    alert["signal_quality_reason"] = quality.get("reason")
    alert["signal_quality_risk_note"] = quality.get("risk_note")
    return quality


def enrich_alerts_with_analysis(
    alerts: list[dict],
    backtest_profile: dict | None = None,
    history_profile: dict | None = None,
    list_profile: dict | None = None,
    league_quality_profile: dict | None = None,
    bucket_profile: dict | None = None,
    persist_signal_quality: bool = False,
) -> list[dict]:
    previous_map = _previous_directions_by_alert_id(alerts)
    for alert in alerts:
        alert["tournament"] = _sanitize_tournament_display(alert.get("tournament"))
        raw_analysis = alert.get("ai_analysis")
        analysis = _parse_analysis(raw_analysis)
        alert.pop("ai_analysis", None)
        if _analysis_needs_live_rebuild(analysis):
            analysis = _rebuild_live_analysis_from_alert(alert, analysis, backtest_profile)
        if not analysis:
            analysis = {}
        if backtest_profile is not None:
            analysis = enrich_analysis_with_backtest(alert, analysis, backtest_profile, config.THRESHOLD)
        analysis = _sanitize_h2h_analysis(_sanitize_recent_form_analysis(analysis))
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
        alert["projected_gap"] = analysis.get("projected_gap")
        alert["opening_delta"] = analysis.get("opening_delta")
        alert["market_total"] = analysis.get("market_total")
        alert["team_recent_total"] = analysis.get("team_recent_total")
        alert["home_last6"] = analysis.get("home_last6") if isinstance(analysis.get("home_last6"), dict) else {}
        alert["away_last6"] = analysis.get("away_last6") if isinstance(analysis.get("away_last6"), dict) else {}
        alert["h2h_total"] = analysis.get("h2h_total")
        alert["history_total"] = analysis.get("history_total")
        alert["h2h_games"] = analysis.get("h2h_games")
        alert["h2h_source"] = analysis.get("h2h_source") or ""
        alert["h2h_score_rows"] = analysis.get("h2h_score_rows") if isinstance(analysis.get("h2h_score_rows"), list) else []
        alert["h2h_row_totals"] = analysis.get("h2h_row_totals") if isinstance(analysis.get("h2h_row_totals"), list) else []
        alert["h2h_quality_score"] = analysis.get("h2h_quality_score")
        alert["h2h_quality_notes"] = analysis.get("h2h_quality_notes") if isinstance(analysis.get("h2h_quality_notes"), list) else []
        alert["h2h_note"] = analysis.get("h2h_note") or ""
        alert["history_note"] = analysis.get("history_note") or ""
        alert["team_context"] = analysis.get("team_context") if isinstance(analysis.get("team_context"), dict) else {}
        alert["recommendation"] = analysis.get("recommendation") or ""
        alert["legacy_direction"] = analysis.get("legacy_direction") or alert.get("direction")
        alert["final_direction"] = analysis.get("final_direction") or analysis.get("direction") or alert.get("direction")
        alert["quarter_scores"] = analysis.get("quarter_scores") or {}
        alert["quarter_totals"] = analysis.get("quarter_totals") or []
        alert["quarter_ppm"] = analysis.get("quarter_ppm") or []
        alert["quarter_length"] = analysis.get("quarter_length")
        alert["match_ppm"] = analysis.get("match_ppm")
        alert["sustainable_ppm"] = analysis.get("sustainable_ppm")
        alert["backtest"] = _trim_backtest_payload(analysis.get("backtest"))
        alert["selection_reason"] = analysis.get("selection_reason") or ""
        alert["projection_quality"] = analysis.get("projection_quality")
        alert["signal_gate"] = (
            analysis.get("signal_gate")
            if isinstance(analysis.get("signal_gate"), dict)
            else {}
        )
        alert["gate_state"] = alert["signal_gate"].get("state") or "LEGACY_UNVERIFIED"
        alert["candidate_eligible"] = bool(analysis.get("candidate_eligible"))
        alert["projection_model_version"] = analysis.get("projection_model_version")
        alert["fair_model_version"] = analysis.get("fair_model_version")
        alert["game_format"] = analysis.get("game_format")
        alert["model_validated"] = analysis.get("model_validated")
        alert["warnings"] = analysis.get("warnings") if isinstance(analysis.get("warnings"), list) else []
        _apply_canonical_signal_direction(alert, analysis)
        alert["bucket_stars"] = matching_signal_buckets(alert, analysis, bucket_profile)
        legacy_quality = build_quality_tag(alert, list_profile)
        alert["quality_label"] = legacy_quality["label"]
        alert["quality_tone"] = legacy_quality["tone"]
        alert["quality_title"] = legacy_quality["title"]
        alert["quality_rank"] = legacy_quality["rank"]
        alert["list_markers"] = build_signal_list_markers(alert, list_profile)
        quality = _apply_signal_quality(alert, league_quality_profile, previous_map.get(int(alert.get("id") or 0), []))
        if analysis.get("signal_quality") != quality:
            analysis = {**analysis, "signal_quality": quality}
            alert["analysis"] = analysis
            if persist_signal_quality and int(alert.get("id") or 0):
                db.update_alert_ai_analysis(
                    int(alert["id"]),
                    json.dumps(analysis, ensure_ascii=False),
                    active_only=True,
                )
        if history_profile is not None:
            alert["history_guard"] = _build_alert_history_guard(alert, history_profile)
        for internal_key in (
            "display_snapshot",
            "telegram_status",
            "telegram_retry_count",
            "telegram_last_error",
            "telegram_message_ids",
        ):
            alert.pop(internal_key, None)
    return alerts



def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_direction(value) -> str:
    text = str(value or "").strip().upper().replace("UST", "ÜST")
    if text in {"ALT", "ÜST"}:
        return text
    return text or "-"


def _apply_canonical_signal_direction(alert: dict, analysis: dict) -> None:
    """Expose one playable ALT/ÜST direction to the UI."""
    final_direction = _normalize_direction(
        alert.get("final_direction") or analysis.get("final_direction") or analysis.get("direction") or alert.get("direction")
    )
    if final_direction in {"ALT", "ÜST"}:
        alert["direction"] = final_direction
        alert["final_direction"] = final_direction
        analysis["direction"] = final_direction
        analysis["final_direction"] = final_direction


def _split_match_teams(match_name: str) -> tuple[str, str]:
    parts = re.split(r"\s+-\s+", str(match_name or ""), maxsplit=1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return str(match_name or "").strip(), ""


def _empty_history_bucket() -> dict:
    return {"resolved": 0, "success": 0, "fail": 0, "match_ids": set()}


def _add_history_result(bucket: dict, row: dict, successful: bool):
    bucket["resolved"] += 1
    bucket["success"] += 1 if successful else 0
    bucket["fail"] += 0 if successful else 1
    match_id = str(row.get("match_id") or "").strip()
    if match_id:
        bucket["match_ids"].add(match_id)


def _build_history_profile(rows: list[dict]) -> dict:
    profile = {
        "league": {},
        "league_direction": {},
        "team": {},
        "team_direction": {},
    }
    for row in rows:
        result_key = _fold(row.get("result"))
        if result_key not in {"basarili", "basarisiz"}:
            continue
        successful = result_key == "basarili"
        league = str(row.get("tournament") or "").strip()
        direction = _normalize_direction(row.get("direction"))
        teams = [team for team in _split_match_teams(row.get("match_name") or "") if team]

        if league:
            _add_history_result(profile["league"].setdefault(league, _empty_history_bucket()), row, successful)
            _add_history_result(profile["league_direction"].setdefault((league, direction), _empty_history_bucket()), row, successful)
        for team in teams:
            _add_history_result(profile["team"].setdefault(team, _empty_history_bucket()), row, successful)
            _add_history_result(profile["team_direction"].setdefault((team, direction), _empty_history_bucket()), row, successful)
    return profile


def _history_label(resolved: int, rate: float, min_samples: int) -> tuple[str, str]:
    if resolved <= 0:
        return "empty", "Geçmiş yok"
    if resolved < min_samples:
        return "thin", "Veri az"
    if rate >= 70:
        return "good", "Güven veriyor"
    if rate <= 35:
        return "bad", "Saçmalıyor"
    if rate <= 45:
        return "warn", "Riskli"
    return "neutral", "Nötr"


def _history_stat(bucket: dict | None, *, min_samples: int) -> dict:
    bucket = bucket or _empty_history_bucket()
    resolved = int(bucket.get("resolved") or 0)
    success = int(bucket.get("success") or 0)
    fail = int(bucket.get("fail") or 0)
    rate = _pct(success, resolved)
    level, label = _history_label(resolved, rate, min_samples)
    return {
        "resolved": resolved,
        "success": success,
        "fail": fail,
        "rate": rate,
        "unique_matches": len(bucket.get("match_ids") or []),
        "level": level,
        "label": label,
    }


def _direction_history_stats(history_profile: dict, group_name: str, key_prefix, *, min_samples: int) -> dict:
    return {
        direction: _history_stat(
            history_profile.get(group_name, {}).get((*key_prefix, direction)),
            min_samples=min_samples,
        )
        for direction in ("ALT", "ÜST")
    }


def _build_alert_history_guard(alert: dict, history_profile: dict) -> dict:
    league = str(alert.get("tournament") or "").strip()
    direction = _normalize_direction(alert.get("direction"))
    home_team, away_team = _split_match_teams(alert.get("match_name") or "")
    notes = []
    if league.startswith("EPL Standings"):
        notes.append("Lig etiketi geniş/karışık görünüyor; bu satırı tek bir gerçek lig gibi yorumlama.")
    teams = []
    for role, team in (("Ev", home_team), ("Dep", away_team)):
        if not team:
            continue
        teams.append({
            "role": role,
            "name": team,
            "overall": _history_stat(history_profile.get("team", {}).get(team), min_samples=8),
            "direction": _history_stat(history_profile.get("team_direction", {}).get((team, direction)), min_samples=5),
            "directions": _direction_history_stats(history_profile, "team_direction", (team,), min_samples=5),
        })
    return {
        "direction": direction,
        "league": {
            "name": league,
            "overall": _history_stat(history_profile.get("league", {}).get(league), min_samples=10),
            "direction": _history_stat(history_profile.get("league_direction", {}).get((league, direction)), min_samples=8),
            "directions": _direction_history_stats(history_profile, "league_direction", (league,), min_samples=8),
        },
        "teams": teams,
        "notes": notes,
    }


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/deleted-matches")
def deleted_matches():
    return render_template("deleted_matches.html")


def _build_live_dashboard_rows(
    rows: list[dict],
    *,
    persist_signal_quality: bool = False,
) -> list[dict]:
    deleted_rows = db.recent_deleted_alerts(limit=None)
    bucket_profile = build_signal_bucket_profile(deleted_rows)
    enriched = enrich_alerts_with_analysis(
        rows,
        build_backtest_profile(deleted_rows),
        _build_history_profile(deleted_rows),
        build_signal_list_profile(db.list_signal_list_entries()),
        _build_league_quality_profile(deleted_rows),
        bucket_profile,
        persist_signal_quality=persist_signal_quality,
    )
    followed_ids = db.upcoming_followed_match_ids([row.get("match_id") for row in enriched])
    for row in enriched:
        row["upcoming_followed"] = 1 if str(row.get("match_id") or "") in followed_ids else 0
    return enriched


def _dashboard_snapshot_payloads(rows: list[dict]) -> dict[int, dict]:
    snapshots = {}
    captured_at = datetime.now(timezone.utc).isoformat()
    for row in rows:
        payload = dict(row)
        for internal_key in (
            "display_snapshot",
            "telegram_status",
            "telegram_retry_count",
            "telegram_last_error",
            "telegram_message_ids",
        ):
            payload.pop(internal_key, None)
        payload["snapshot_meta"] = {
            "schema_version": 1,
            "source": "live_dashboard",
            "captured_at": captured_at,
        }
        alert_id = int(payload.get("id") or 0)
        if alert_id:
            snapshots[alert_id] = payload
    return snapshots


def _save_dashboard_snapshots(rows: list[dict]) -> int:
    """Persist snapshots without archiving (used by explicit refresh/tests)."""
    return db.save_active_alert_display_snapshots(
        _dashboard_snapshot_payloads(rows)
    )


def _archive_active_match(match_id: str) -> int:
    """Build the live DTO, persist it, and soft-delete in one DB transaction."""
    rows = db.active_alerts_for_match(match_id)
    if not rows:
        return 0
    enriched = _build_live_dashboard_rows(rows, persist_signal_quality=True)
    return db.archive_match_with_display_snapshots(
        match_id,
        _dashboard_snapshot_payloads(enriched),
    )


def _archive_all_active_rows() -> int:
    rows = db.all_active_alerts(limit=None)
    if not rows:
        return 0
    enriched = _build_live_dashboard_rows(rows, persist_signal_quality=True)
    return db.archive_all_with_display_snapshots(
        _dashboard_snapshot_payloads(enriched)
    )


def _run_async_dashboard_job(name: str, coro_factory, failure_message: str):
    try:
        return jsonify(asyncio.run(coro_factory()))
    except Exception as exc:
        logger.exception("Dashboard async job failed (%s): %s", name, exc)
        return jsonify({"error": failure_message}), 500


@app.route("/api/alerts")
def api_alerts():
    alerts = _build_live_dashboard_rows(db.recent_alerts(limit=500))
    return jsonify(alerts)


@app.route("/api/signal-lists")
def api_signal_lists():
    return jsonify(db.list_signal_list_entries())


@app.route("/api/signal-lists", methods=["POST"])
def api_add_signal_list_entry():
    payload = request.get_json(silent=True) or {}
    entry = db.add_signal_list_entry(
        payload.get("list_type") or "",
        payload.get("scope") or "",
        payload.get("value") or "",
    )
    if entry is None:
        return jsonify({"error": "invalid_list_entry"}), 400
    return jsonify({"entry": entry, "created": True})


@app.route("/api/signal-lists/<int:entry_id>", methods=["DELETE"])
def api_delete_signal_list_entry(entry_id: int):
    if not db.delete_signal_list_entry(entry_id):
        return jsonify({"error": "not_found"}), 404
    return jsonify({"id": entry_id, "deleted": True})


def _analysis_needs_live_rebuild(analysis: dict) -> bool:
    """Older ai_analysis payloads predate fair-line/projection fields."""
    return (
        not isinstance(analysis, dict)
        or analysis.get("projected_total") is None
        or analysis.get("fair_line") is None
    )


def _rebuild_live_analysis_from_alert(
    alert: dict,
    analysis: dict | None,
    backtest_profile: dict | None = None,
) -> dict:
    existing = analysis if isinstance(analysis, dict) else {}
    try:
        rebuilt = build_signal_analysis(
            {
                **alert,
                "opening_total": alert.get("opening"),
                "inplay_total": alert.get("live"),
                "prematch_total": alert.get("prematch"),
                "quarter_scores": alert.get("quarter_scores") or existing.get("quarter_scores") or {},
                "signal_count": alert.get("signal_count") or existing.get("signal_count") or 1,
            },
            {},
            config.THRESHOLD,
            backtest_profile=backtest_profile,
        )
    except (TypeError, ValueError, KeyError):
        return existing
    return {**existing, **rebuilt}


_DELETED_LIST_FIELDS = (
    "id", "match_id", "match_name", "tournament", "url",
    "deleted_at", "alerted_at", "alert_moment", "alert_period",
    "direction", "final_direction", "signal_count",
    "status", "score", "final_status", "final_score", "opening", "prematch", "live", "diff",
    "projected", "projected_gap", "opening_delta", "fair_line", "fair_edge",
    "projection_quality", "recommendation", "selection_reason",
    "signal_gate", "gate_state", "candidate_eligible",
    "signal_quality", "signal_quality_score", "signal_quality_label",
    "signal_quality_reason", "signal_quality_risk_note",
    "bucket_stars", "result", "result_source", "settled_at", "note",
    "bet_placed", "ignored", "followed",
)


def _enrich_deleted_alert(
    alert: dict,
    *,
    full: bool,
) -> dict:
    """Read frozen live-dashboard display state without recomputing signal labels."""
    raw = dict(alert)
    snapshot = _parse_analysis(raw.get("display_snapshot"))
    result = dict(snapshot) if snapshot else raw

    if not snapshot:
        analysis = _parse_analysis(raw.get("ai_analysis"))
        result["analysis"] = analysis
        result["fair_line"] = analysis.get("fair_line")
        result["fair_edge"] = analysis.get("fair_edge")
        result["projected"] = analysis.get("projected_total")
        result["projected_gap"] = analysis.get("projected_gap")
        result["opening_delta"] = analysis.get("opening_delta")
        result["projection_quality"] = analysis.get("projection_quality")
        result["recommendation"] = analysis.get("recommendation") or ""
        result["signal_gate"] = (
            analysis.get("signal_gate")
            if isinstance(analysis.get("signal_gate"), dict)
            else {}
        )
        result["gate_state"] = result["signal_gate"].get("state") or "LEGACY_UNVERIFIED"
        _apply_stored_signal_quality(result, analysis)

    # Settlement and deletion metadata may legitimately change after the snapshot.
    for key in (
        "id", "match_id", "deleted_at", "result", "result_source", "settled_at",
        "final_status", "final_score", "note",
        "bet_placed", "ignored", "followed", "alerted_at", "alert_moment",
    ):
        if key in raw:
            result[key] = raw[key]
    if "bucket_stars" not in result or not isinstance(result.get("bucket_stars"), list):
        result["bucket_stars"] = []
    result.pop("display_snapshot", None)
    result.pop("ai_analysis", None)
    if full:
        return result
    return {
        key: result.get(key)
        for key in _DELETED_LIST_FIELDS
        if key in result
    }


def _lightweight_enrich_deleted_alerts(
    alerts: list[dict],
) -> list[dict]:
    return [
        _enrich_deleted_alert(alert, full=False)
        for alert in alerts
    ]


@app.route("/api/deleted-matches")
def api_deleted_matches():
    raw_limit = request.args.get("limit", type=int)
    rows = db.recent_deleted_alerts(
        limit=raw_limit if raw_limit is not None and raw_limit > 0 else None
    )
    return jsonify(_lightweight_enrich_deleted_alerts(rows))


@app.route("/api/deleted-matches/<int:alert_id>/details")
def api_deleted_match_details(alert_id: int):
    target = db.get_deleted_alert_by_id(alert_id)
    if not target:
        return jsonify({"error": "not_found"}), 404
    return jsonify(_enrich_deleted_alert(
        target,
        full=True,
    ))


@app.route("/api/deleted-matches/export.csv")
def api_export_finished_deleted_matches_csv():
    deleted_rows = db.recent_deleted_alerts(limit=None)
    rows = [
        row for row in _lightweight_enrich_deleted_alerts(
            deleted_rows,
        )
        if str(row.get("result") or "").strip()
    ]

    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output)
    writer.writerow([
        "Maç", "Lig", "Sinyal Tarihi (TR)", "Sinyal Saati (TR)", "Sinyal Anı", "Sinyal Türü",
        "Sinyal Anı Skoru", "Final Skor",
        "Açılış", "Canlı", "Proj.", "Adil Barem", "S.K. Puan", "S.K. Etiket", "Sonuç", "Not",
    ])

    for row in rows:
        direction = str(row.get("direction") or "").strip()
        match_name = re.sub(r"\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}\s*", "", str(row.get("match_name") or ""))
        match_name = re.sub(r"\s*betting odds\s*", "", match_name, flags=re.IGNORECASE).strip()
        projected = row.get("projected")
        projected_cell = f"{float(projected):.1f}" if projected is not None else ""
        fair_line = row.get("fair_line")
        fair_line_cell = f"{float(fair_line):.1f}" if fair_line is not None else "Hesaplanamıyor"
        alerted_at = str(row.get("alerted_at") or "").strip()
        alerted_date, _, alerted_time = alerted_at.partition(" ")
        writer.writerow([
            match_name,
            row.get("tournament") or "",
            alerted_date,
            alerted_time,
            row.get("alert_moment") or "",
            direction,
            row.get("score") or "",
            row.get("final_score") or "",
            row.get("opening") if row.get("opening") is not None else "",
            row.get("live") if row.get("live") is not None else "",
            projected_cell,
            fair_line_cell,
            row.get("signal_quality_score") if row.get("signal_quality_score") is not None else "",
            row.get("signal_quality_label") or "",
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
    outcome = db.purge_deleted_matches()
    return jsonify({
        "cleared": int(outcome.get("protected_count") or 0) == 0,
        **outcome,
    })


@app.route("/api/deleted-matches/<int:alert_id>", methods=["DELETE"])
def api_purge_deleted_alert(alert_id: int):
    removed = db.delete_alert(alert_id)
    if not removed:
        if db.is_deleted_alert_protected(alert_id):
            return jsonify({
                "error": (
                    "Bu kayıt sonuçlanmamış prospektif kanıta bağlı; "
                    "otomatik final sonucu alınmadan kalıcı silinemez."
                )
            }), 409
        return jsonify({"error": "not found"}), 404
    return jsonify({"id": alert_id, "deleted": True})


@app.route("/api/alerts/check-finished", methods=["POST"])
def api_check_active_match_finished():
    return _run_async_dashboard_job(
        "active-match-finished-scan",
        lambda: run_active_match_finished_scan(
            db,
            config,
            before_delete=_archive_active_match,
        ),
        "Biten maçlar kontrol edilemedi. Sunucu loglarını kontrol edin.",
    )


@app.route("/api/deleted-matches/check-results", methods=["POST"])
def api_check_deleted_match_results():
    return _run_async_dashboard_job(
        "deleted-match-result-cycle",
        lambda: run_deleted_match_result_cycle(db, config),
        "Bitmiş maç sonuçları kontrol edilemedi. Sunucu loglarını kontrol edin.",
    )


@app.route("/api/deleted-matches/<int:alert_id>/check-result", methods=["POST"])
def api_check_single_deleted_match_result(alert_id: int):
    return _run_async_dashboard_job(
        "single-deleted-match-result-check",
        lambda: run_single_deleted_match_result_check(db, config, alert_id),
        "Maç sonucu kontrol edilemedi. Sunucu loglarını kontrol edin.",
    )


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
        "fail_rate": _pct(fail, resolved),
    }


def _dir_stats(signals: list, direction: str) -> dict:
    target = _normalize_direction(direction)
    scoped = [s for s in signals if _normalize_direction(s.get("direction")) == target]
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


_QUALITY_BUCKET_ORDER = ["80-100", "60-79", "50-59", "0-49", "Bilinmiyor"]


def _quality_bucket(value) -> str:
    try:
        score = int(round(float(value)))
    except (TypeError, ValueError):
        return "Bilinmiyor"
    if score >= 80:
        return "80-100"
    if score >= 60:
        return "60-79"
    if score >= 50:
        return "50-59"
    return "0-49"


def _line_diff_bucket(row: dict) -> str:
    opening = _safe_float(row.get("opening"), None)
    live = _safe_float(row.get("live"), None)
    if opening is None or live is None:
        return "Bilinmiyor"
    diff = live - opening
    abs_diff = abs(diff)
    prefix = "+" if diff >= 0 else "-"
    if abs_diff < 10:
        return "0-9.9"
    if abs_diff < 15:
        return f"{prefix}10-14.9"
    if abs_diff < 20:
        return f"{prefix}15-19.9"
    if abs_diff <= 30:
        return f"{prefix}20-30"
    return f"{prefix}30+"


def _projection_diff_bucket(value) -> str:
    try:
        diff = float(value)
    except (TypeError, ValueError):
        return "Bilinmiyor"
    if diff <= -10:
        return "<= -10"
    if diff <= -5:
        return "-10 - -5"
    if diff < 5:
        return "-5 - +5"
    if diff < 10:
        return "+5 - +10"
    return ">= +10"


def _score_gap_bucket(value) -> str:
    try:
        gap = abs(int(value))
    except (TypeError, ValueError):
        return "Bilinmiyor"
    if gap <= 7:
        return "0-7"
    if gap <= 15:
        return "8-15"
    if gap <= 24:
        return "16-24"
    return "25+"


def _stats_for_grouped_rows(groups: dict[str, list], order: list[str] | None = None, min_resolved: int = 0) -> list[dict]:
    labels = order or sorted(groups.keys())
    rows_out = []
    for label in labels:
        rows = groups.get(label) or []
        if not rows:
            continue
        stats = _bucket_stats(rows)
        if stats["resolved"] < min_resolved:
            continue
        stats["label"] = label
        rows_out.append(stats)
    return rows_out


def build_signal_quality_report(signals: list[dict]) -> dict:
    resolved = [s for s in signals if _fold(s.get("result")) in {"basarili", "basarisiz"}]
    quality_groups: dict[str, list] = {label: [] for label in _QUALITY_BUCKET_ORDER}
    for row in resolved:
        quality_groups[_quality_bucket(row.get("signal_quality_score"))].append(row)

    by_direction: dict[str, list[dict]] = {}
    for direction in ("ALT", "ÜST"):
        scoped = [row for row in resolved if _normalize_direction(row.get("direction")) == direction]
        groups = {label: [] for label in _QUALITY_BUCKET_ORDER}
        for row in scoped:
            groups[_quality_bucket(row.get("signal_quality_score"))].append(row)
        by_direction[direction] = _stats_for_grouped_rows(groups, _QUALITY_BUCKET_ORDER)

    league_groups: dict[str, list] = {}
    period_groups: dict[str, list] = {label: [] for label in _PERIOD_BUCKET_ORDER}
    line_diff_groups: dict[str, list] = {}
    projection_diff_groups: dict[str, list] = {}
    score_gap_groups: dict[str, list] = {}
    for row in resolved:
        tournament = str(row.get("tournament") or "").strip() or "Bilinmiyor"
        league_groups.setdefault(tournament, []).append(row)
        period_groups[_period_bucket(row.get("alert_moment") or row.get("status"))].append(row)
        line_diff_groups.setdefault(_line_diff_bucket(row), []).append(row)
        quality = row.get("signal_quality") if isinstance(row.get("signal_quality"), dict) else {}
        projection_diff_groups.setdefault(_projection_diff_bucket(quality.get("projection_diff")), []).append(row)
        score_gap_groups.setdefault(_score_gap_bucket(quality.get("score_gap")), []).append(row)

    league_rows = _stats_for_grouped_rows(league_groups, min_resolved=3)
    league_rows.sort(key=lambda item: (-item["resolved"], -item["rate"], item["label"]))

    return {
        "quality_buckets": _stats_for_grouped_rows(quality_groups, _QUALITY_BUCKET_ORDER),
        "by_direction": by_direction,
        "league": league_rows,
        "period": _stats_for_grouped_rows(period_groups, _PERIOD_BUCKET_ORDER),
        "line_diff": _stats_for_grouped_rows(line_diff_groups),
        "projection_diff": _stats_for_grouped_rows(projection_diff_groups),
        "score_gap": _stats_for_grouped_rows(score_gap_groups, ["0-7", "8-15", "16-24", "25+", "Bilinmiyor"]),
    }


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
            "half_buckets": [],
            "best_dir_period": [],
            "worst_dir_period": [],
            "tournament_top": [],
            "tournament_bottom": [],
            "signal_quality_report": build_signal_quality_report([]),
            "trend_7d": None,
            "actions": [],
        }

    total = len(signals)
    success_count = sum(1 for s in signals if _fold(s.get("result")) == "basarili")
    fail_count = sum(1 for s in signals if _fold(s.get("result")) == "basarisiz")
    pending_count = total - success_count - fail_count
    resolved = success_count + fail_count

    alt_stats = _dir_stats(signals, "ALT")
    ust_stats = _dir_stats(signals, "ÜST")

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
        alt_rows_p = [r for r in rows if _normalize_direction(r.get("direction")) == "ALT"]
        ust_rows_p = [r for r in rows if _normalize_direction(r.get("direction")) == "ÜST"]
        if alt_rows_p:
            stats["alt"] = _bucket_stats(alt_rows_p)
        if ust_rows_p:
            stats["ust"] = _bucket_stats(ust_rows_p)
        period_buckets.append(stats)

    # 1. Yarı vs 2. Yarı karşılaştırması
    h1_rows = [r for lbl in ["1. Çeyrek", "2. Çeyrek", "Devre Arası"] for r in period_groups.get(lbl, [])]
    h2_rows = [r for lbl in ["3. Çeyrek", "4. Çeyrek"] for r in period_groups.get(lbl, [])]
    half_buckets: list[dict] = []
    for half_label, h_rows in [("1. Yarı (Q1–Q2)", h1_rows), ("2. Yarı (Q3–Q4)", h2_rows)]:
        if not h_rows:
            continue
        st = _bucket_stats(h_rows)
        st["label"] = half_label
        h_alt = [r for r in h_rows if _normalize_direction(r.get("direction")) == "ALT"]
        h_ust = [r for r in h_rows if _normalize_direction(r.get("direction")) == "ÜST"]
        if h_alt:
            st["alt"] = _bucket_stats(h_alt)
        if h_ust:
            st["ust"] = _bucket_stats(h_ust)
        half_buckets.append(st)

    # Yön × Periyot kombinasyon matrisi
    dir_period_combos: list[dict] = []
    for direction, dir_label in [("ALT", "ALT"), ("ÜST", "ÜST")]:
        for period_label in _PERIOD_BUCKET_ORDER:
            if period_label == "Bilinmiyor":
                continue
            combo_rows = [
                r for r in period_groups.get(period_label, [])
                if _normalize_direction(r.get("direction")) == direction
            ]
            if not combo_rows:
                continue
            st = _bucket_stats(combo_rows)
            if st["resolved"] < 3:
                continue
            dir_period_combos.append({"label": f"{dir_label} · {period_label}", "direction": dir_label, "period": period_label, **st})
    dir_period_combos.sort(key=lambda x: -x["rate"])
    best_dir_period = dir_period_combos[:3]
    worst_dir_period = sorted(dir_period_combos, key=lambda x: x["rate"])[:3]

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
        if abs(overall_rate - 50) < 3:
            actions.append(
                "Genel başarı %50 civarında — sistem neredeyse yazı-tura. "
                "Fark bandı ve periyot kırılımları yalnız araştırma hipotezi olarak izlenmeli."
            )
        elif overall_rate < 44:
            actions.append(
                f"Genel başarı %{overall_rate:.1f} — ham sinyallerde doğrulanmış bir avantaj görünmüyor."
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
            "bu yalnız aynı örneklemde gözlenen zayıf bir tarihsel segmenttir."
        )
    if best_diff and best_diff is not worst_diff and best_diff["rate"] >= 60:
        actions.append(
            f"Fark {best_diff['label']} aralığı en güçlü segment (%{best_diff['rate']:.1f}) — "
            "ileri tarihli bağımsız veride doğrulanmadan oynanabilir kabul edilmemeli."
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
            "bu tarihsel fark ileri tarihli test için not edildi."
        )

    if best_dir_period:
        b = best_dir_period[0]
        if b["rate"] >= 65 and b["resolved"] >= 4:
            actions.append(
                f"En güçlü kombinasyon {b['label']}: %{b['rate']:.1f} başarı "
                f"({b['success']}/{b['resolved']}) — küçük ve aynı örneklemden gelen betimsel bir bulgu."
            )
    if worst_dir_period:
        w = worst_dir_period[0]
        if w["rate"] < 35 and w["resolved"] >= 4:
            actions.append(
                f"En zayıf kombinasyon {w['label']}: %{w['rate']:.1f} başarı "
                f"({w['success']}/{w['resolved']}) — bağımsız doğrulama gerektiren betimsel bir bulgu."
            )

    if len(half_buckets) == 2:
        h1, h2 = half_buckets[0], half_buckets[1]
        if h1["resolved"] >= 4 and h2["resolved"] >= 4:
            delta_h = abs(h1["rate"] - h2["rate"])
            if delta_h >= 10:
                better = "1. Yarı" if h1["rate"] > h2["rate"] else "2. Yarı"
                actions.append(
                    f"{better} sinyalleri tarihsel örneklemde {delta_h:.1f} puan önde; bu fark doğrulanmış değildir."
                )

    if tournament_bottom:
        worst_t = tournament_bottom[0]
        if worst_t["rate"] < 30 and worst_t["resolved"] >= 4:
            actions.append(
                f"{worst_t['label']}: %{worst_t['rate']:.1f} ({worst_t['success']}/{worst_t['resolved']}) — "
                "ileri tarihli test için zayıf segment adayı."
            )
    if tournament_top:
        best_t = tournament_top[0]
        if best_t["rate"] >= 65 and best_t["resolved"] >= 4:
            actions.append(
                f"{best_t['label']}: %{best_t['rate']:.1f} ({best_t['success']}/{best_t['resolved']}) — "
                "ileri tarihli test için pozitif segment adayı; oynanabilirlik kanıtı değildir."
            )

    if trend_7d and trend_7d.get("delta") is not None and trend_7d["delta"] <= -8:
        actions.append(
            f"Son 7 günde başarı %{-trend_7d['delta']:.1f} puan düştü — "
            f"son değişiklikleri gözden geçir."
        )

    if not actions and resolved > 0:
        actions.append("Belirgin bir tarihsel ayrışma yok; ileri tarihli örneklemi büyüt.")

    if resolved == 0:
        headline = "Sonuçlar işaretlenmemiş. Sinyal başarılarını işaretleyerek raporu oluşturun."
    elif overall_rate >= 60:
        headline = f"Tarihsel kayıt başarısı %{overall_rate:.1f}; bu oran ileri tarihli doğrulama değildir."
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
        f"Genel başarı %{overall_rate:.1f} ({resolved} sonuçlanan). "
        f"{dir_summary} Karışık maç {len(mixed_matches)}, "
        f"sonuç bekleyen {pending_count}. Tekrarlı sinyaller korelasyonludur; bu rapor oynama tavsiyesi üretmez."
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

        f"Sonuçlanan sinyallerde {success_count} başarılı, {fail_count} başarısız kayıt var.",
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
        "half_buckets": half_buckets,
        "best_dir_period": best_dir_period,
        "worst_dir_period": worst_dir_period,
        "tournament_top": tournament_top,
        "tournament_bottom": tournament_bottom,
        "signal_quality_report": build_signal_quality_report(signals),
        "trend_7d": trend_7d,
        "actions": actions,
    }


@app.route("/api/deleted-matches/report")
def api_deleted_matches_report():
    signals = db.recent_deleted_alerts(limit=None)
    signals = _lightweight_enrich_deleted_alerts(signals)
    return jsonify(build_deleted_matches_report(signals))


def _segment_stats(scope: str, label: str, rows: list, baseline_rate: float, *,
                   min_resolved: int = 5) -> dict | None:
    if not rows:
        return None
    success = sum(1 for r in rows if _fold(r.get("result")) == "basarili")
    fail = sum(1 for r in rows if _fold(r.get("result")) == "basarisiz")
    resolved = success + fail
    if resolved == 0:
        return None
    rate = _pct(success, resolved)
    return {
        "scope": scope,
        "label": label,
        "total": len(rows),
        "resolved": resolved,
        "success": success,
        "fail": fail,
        "rate": rate,
        "fail_rate": _pct(fail, resolved),
        "delta": round(rate - baseline_rate, 1),
        "weight": round((abs(rate - baseline_rate)) * (resolved ** 0.5), 2),
        "min_resolved_met": resolved >= min_resolved,
    }


def _diff_label(direction: str, diff_label: str) -> str:
    return f"{direction} · Fark {diff_label}"


def _period_label(direction: str, period_label: str) -> str:
    return f"{direction} · {period_label}"


def build_deleted_matches_insights(signals: list) -> dict:
    resolved_signals = [s for s in signals if _fold(s.get("result")) in {"basarili", "basarisiz"}]
    total_signals = len(signals)
    total_resolved = len(resolved_signals)
    overall_success = sum(1 for s in resolved_signals if _fold(s.get("result")) == "basarili")
    overall_rate = _pct(overall_success, total_resolved)
    overall_fail = total_resolved - overall_success

    if total_resolved == 0:
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "headline": "Sonuçlanmış silinen sinyal yok — çıkarım üretmek için sinyalleri Başarılı/Başarısız olarak işaretleyin.",
            "overall": {
                "total_signals": total_signals,
                "resolved": 0,
                "success": 0,
                "fail": 0,
                "rate": 0.0,
            },
            "play": [], "avoid": [], "neutral": [],
            "watchlist": [],
            "advice": [],
            "neutral_count": 0,
            "insufficient_count": 0,
            "rules": {},
            "simulation": {},
        }

    segments: list[dict] = []

    # 1. Yön
    for label in ("ALT", "ÜST"):
        rows = [s for s in resolved_signals if _fold(s.get("direction")) == _fold(label)]
        seg = _segment_stats("Yön", label, rows, overall_rate)
        if seg:
            segments.append(seg)

    # 2. Fark aralığı
    for bucket in _DIFF_BUCKET_ORDER:
        if bucket == "Bilinmiyor":
            continue
        rows = [s for s in resolved_signals if _diff_bucket(s.get("diff")) == bucket]
        seg = _segment_stats("Fark Aralığı", bucket, rows, overall_rate)
        if seg:
            segments.append(seg)

    # 3. Periyot
    for bucket in _PERIOD_BUCKET_ORDER:
        if bucket == "Bilinmiyor":
            continue
        rows = [
            s for s in resolved_signals
            if _period_bucket(s.get("alert_moment") or s.get("status")) == bucket
        ]
        seg = _segment_stats("Periyot", bucket, rows, overall_rate)
        if seg:
            segments.append(seg)

    # 4. Sinyal sırası
    first_rows = [s for s in resolved_signals if int(s.get("signal_count") or 1) == 1]
    repeat_rows = [s for s in resolved_signals if int(s.get("signal_count") or 1) >= 2]
    for label, rows in (("İlk Sinyal (#1)", first_rows), ("Tekrar Sinyali (#2+)", repeat_rows)):
        seg = _segment_stats("Sinyal Sırası", label, rows, overall_rate)
        if seg:
            segments.append(seg)

    # 5. Yön × Fark
    for direction in ("ALT", "ÜST"):
        for bucket in _DIFF_BUCKET_ORDER:
            if bucket == "Bilinmiyor":
                continue
            rows = [
                s for s in resolved_signals
                if _fold(s.get("direction")) == _fold(direction)
                and _diff_bucket(s.get("diff")) == bucket
            ]
            seg = _segment_stats("Yön × Fark", _diff_label(direction, bucket), rows, overall_rate)
            if seg:
                segments.append(seg)

    # 6. Yön × Periyot
    for direction in ("ALT", "ÜST"):
        for bucket in _PERIOD_BUCKET_ORDER:
            if bucket == "Bilinmiyor":
                continue
            rows = [
                s for s in resolved_signals
                if _fold(s.get("direction")) == _fold(direction)
                and _period_bucket(s.get("alert_moment") or s.get("status")) == bucket
            ]
            seg = _segment_stats("Yön × Periyot", _period_label(direction, bucket), rows, overall_rate)
            if seg:
                segments.append(seg)

    # 7. Turnuva (en az 6 sonuçlanan)
    by_tour: dict[str, list] = {}
    for s in resolved_signals:
        name = (s.get("tournament") or "").strip()
        if not name:
            continue
        by_tour.setdefault(name, []).append(s)
    for name, rows in by_tour.items():
        seg = _segment_stats("Turnuva", name, rows, overall_rate, min_resolved=6)
        if seg and seg["resolved"] >= 6:
            segments.append(seg)

    play: list[dict] = []
    avoid: list[dict] = []
    neutral: list[dict] = []
    insufficient: list[dict] = []

    for seg in segments:
        if not seg["min_resolved_met"]:
            insufficient.append(seg)
            continue
        rate = seg["rate"]
        delta = seg["delta"]
        if rate >= 58 and delta >= 6:
            play.append({
                **seg,
                "verdict": "TARİHSEL POZİTİF",
                "advice": (
                    f"{seg['scope']} → {seg['label']}: %{rate:.1f} başarı "
                    f"({seg['success']}/{seg['resolved']}), genelden +{delta:.1f}p önde. "
                    "Aynı örneklemde keşfedildiği için oynanabilirlik kanıtı değildir."
                ),
            })
        elif rate <= 42 and delta <= -6:
            avoid.append({
                **seg,
                "verdict": "TARİHSEL ZAYIF",
                "advice": (
                    f"{seg['scope']} → {seg['label']}: sadece %{rate:.1f} başarı "
                    f"({seg['success']}/{seg['resolved']}), genelden {-delta:.1f}p geride. "
                    "İleri tarihli test için zayıf segment hipotezidir."
                ),
            })
        else:
            neutral.append(seg)

    play.sort(key=lambda s: -s["weight"])
    avoid.sort(key=lambda s: -s["weight"])

    # Watchlist: yeterli örneklem yok ama eğilim güçlü
    watchlist = [
        s for s in insufficient
        if 3 <= s["resolved"] < 5 and abs(s["delta"]) >= 10
    ]
    watchlist.sort(key=lambda s: -abs(s["delta"]))
    watchlist = watchlist[:6]

    advice: list[str] = []
    if abs(overall_rate - 50) < 3 and total_resolved >= 30:
        advice.append(
            f"Ham sinyallerin başarısı %{overall_rate:.1f} — istatistiksel olarak yazı-tura. "
            "Aşağıdaki segmentler aynı veriden çıkarıldığı için yalnız keşif amaçlıdır."
        )
    elif overall_rate <= 44 and total_resolved >= 30:
        advice.append(
            f"Ham sinyal başarısı %{overall_rate:.1f}; negatif segmentleri elemeden tüm sinyalleri oynamak zayıf görünüyor."
        )

    if play:
        top = play[0]
        advice.append(
            f"En güçlü pozitif segment: {top['scope']} → {top['label']} "
            f"(%{top['rate']:.1f}, n={top['resolved']}). İleri tarihli bağımsız test için adaydır."
        )
    if avoid:
        top_bad = avoid[0]
        advice.append(
            f"En zayıf segment: {top_bad['scope']} → {top_bad['label']} "
            f"(%{top_bad['rate']:.1f}, n={top_bad['resolved']}). İleri tarihli test için zayıf segment adayıdır."
        )
    if not (play or avoid):
        advice.append(
            "Belirgin pozitif veya negatif segment yok — örneklemi büyütmek ya da yeni filtre boyutları "
            "(saat, takım, oran açılışı) eklemek faydalı olabilir."
        )

    if abs(overall_rate - 50) < 3:
        headline = (
            f"{total_resolved} sonuçlanan sinyal · genel başarı %{overall_rate:.1f} (yazı-tura). "
            "Segment ayrışmaları doğrulanmış avantaj değildir."
        )
    elif overall_rate >= 55:
        headline = (
            f"{total_resolved} sonuçlanan sinyal · genel başarı %{overall_rate:.1f}. "
            "Bu yalnız geçmiş örneklemin betimsel sonucudur."
        )
    else:
        headline = (
            f"{total_resolved} sonuçlanan sinyal · genel başarı %{overall_rate:.1f}. "
            "Aşağıdaki kırılımlar yalnız araştırma hipotezidir."
        )

    advice.insert(
        0,
        "Bu görünüm aynı geçmiş veriden segment keşfeder; kural seçip aynı örneklemde ölçüm yapmaz "
        "ve bahis/oynama tavsiyesi değildir.",
    )

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "headline": headline,
        "overall": {
            "total_signals": total_signals,
            "resolved": total_resolved,
            "success": overall_success,
            "fail": overall_fail,
            "rate": overall_rate,
        },
        "play": play[:8],
        "avoid": avoid[:8],
        "watchlist": watchlist,
        "neutral_count": len(neutral),
        "insufficient_count": len(insufficient),
        "advice": advice,
        "rules": {},
        "simulation": {},
    }


@app.route("/api/deleted-matches/insights")
def api_deleted_matches_insights():
    signals = db.recent_deleted_alerts(limit=None)
    signals = _lightweight_enrich_deleted_alerts(signals)
    return jsonify(build_deleted_matches_insights(signals))


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


async def _fetch_alert_overview_snapshot(url: str) -> dict:
    scraper = AiscoreScraper(
        aiscore_url=config.AISCORE_URL,
        max_matches_per_cycle=1,
        page_timeout_ms=config.PAGE_TIMEOUT_MS,
        skip_h2h=True,
    )
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser, context = await scraper._create_browser_context(p)
        page = await context.new_page()
        page.set_default_timeout(config.PAGE_TIMEOUT_MS)
        try:
            return await scraper._fetch_overview_data(page, url)
        finally:
            await context.close()
            await browser.close()


async def _fetch_alert_h2h_body(url: str) -> str:
    scraper = AiscoreScraper(
        aiscore_url=config.AISCORE_URL,
        max_matches_per_cycle=1,
        page_timeout_ms=config.PAGE_TIMEOUT_MS,
        skip_h2h=False,
    )
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser, context = await scraper._create_browser_context(p)
        page = await context.new_page()
        page.set_default_timeout(config.PAGE_TIMEOUT_MS)
        try:
            return await scraper._fetch_h2h_body(page, url)
        finally:
            await context.close()
            await browser.close()


@app.route("/api/alerts/<int:alert_id>/h2h/refresh", methods=["POST"])
def api_refresh_alert_h2h(alert_id: int):
    alert = db.get_alert(alert_id)
    if not alert:
        return jsonify({"error": "not found"}), 404
    url = str(alert.get("url") or "").strip()
    if not url:
        return jsonify({"error": "missing_url"}), 400

    try:
        h2h_body = asyncio.run(_fetch_alert_h2h_body(url))
    except Exception as exc:
        return jsonify({"error": "fetch_failed", "detail": str(exc)}), 502

    if not h2h_body:
        return jsonify({"error": "h2h_not_found"}), 404

    existing_analysis = _parse_analysis(alert.get("ai_analysis"))
    rebuilt = build_signal_analysis(
        {
            **alert,
            "opening_total": alert.get("opening"),
            "inplay_total": alert.get("live"),
            "prematch_total": alert.get("prematch"),
            "quarter_scores": existing_analysis.get("quarter_scores") or {},
            "signal_count": alert.get("signal_count") or existing_analysis.get("signal_count") or 1,
        },
        {"h2h": {"body_text": h2h_body}},
        config.THRESHOLD,
    )
    analysis = {
        **existing_analysis,
        **rebuilt,
    }
    db.update_alert_live_snapshot(
        alert_id,
        ai_analysis=json.dumps(analysis, ensure_ascii=False),
    )
    updated = db.get_alert(alert_id) or alert
    updated["ai_analysis"] = json.dumps(analysis, ensure_ascii=False)
    enriched = enrich_alerts_with_analysis([updated])[0]
    return jsonify({"ok": True, "alert": enriched, "analysis": analysis})


@app.route("/api/alerts/<int:alert_id>/quarter-scores/refresh", methods=["POST"])
def api_refresh_alert_quarter_scores(alert_id: int):
    alert = db.get_alert(alert_id)
    if not alert:
        return jsonify({"error": "not found"}), 404
    url = str(alert.get("url") or "").strip()
    if not url:
        return jsonify({"error": "missing_url"}), 400

    try:
        overview = asyncio.run(_fetch_alert_overview_snapshot(url))
    except Exception as exc:
        return jsonify({"error": "fetch_failed", "detail": str(exc)}), 502

    quarter_scores = overview.get("quarterScores") if isinstance(overview, dict) else {}
    if not isinstance(quarter_scores, dict) or not quarter_scores.get("home") or not quarter_scores.get("away"):
        return jsonify({"error": "quarter_scores_not_found"}), 404

    status = str(overview.get("status") or alert.get("status") or "")
    score = str(overview.get("score") or alert.get("score") or "")
    existing_analysis = _parse_analysis(alert.get("ai_analysis"))
    rebuilt = build_signal_analysis(
        {
            **alert,
            "status": status,
            "score": score,
            "opening_total": alert.get("opening"),
            "inplay_total": alert.get("live"),
            "prematch_total": alert.get("prematch"),
            "quarter_scores": quarter_scores,
            "signal_count": alert.get("signal_count") or existing_analysis.get("signal_count") or 1,
        },
        {},
        config.THRESHOLD,
    )
    analysis = {
        **existing_analysis,
        **rebuilt,
        "quarter_scores": quarter_scores,
    }
    db.update_alert_live_snapshot(
        alert_id,
        status=status,
        score=score,
        ai_analysis=json.dumps(analysis, ensure_ascii=False),
    )
    updated = db.get_alert(alert_id) or {**alert, "status": status, "score": score}
    updated["ai_analysis"] = json.dumps(analysis, ensure_ascii=False)
    enriched = enrich_alerts_with_analysis([updated])[0]
    return jsonify({"ok": True, "alert": enriched, "analysis": analysis})


@app.route("/api/alerts/<int:alert_id>", methods=["DELETE"])
def api_delete_alert(alert_id: int):
    alert = db.get_alert(alert_id)
    if not alert:
        return jsonify({"error": "not found"}), 404
    deleted_count = _archive_active_match(alert["match_id"])
    if deleted_count <= 0:
        return jsonify({"error": "active alert changed before it could be archived"}), 409
    return jsonify({
        "id": alert_id,
        "match_id": alert["match_id"],
        "deleted": True,
        "affected": deleted_count,
    })


def _team_history_summary(matches: list[dict]) -> dict:
    resolved = 0
    success = 0
    fail = 0
    push = 0
    alt = 0
    ust = 0
    for match in matches:
        direction = _normalize_direction(match.get("direction"))
        if direction == "ALT":
            alt += 1
        elif direction == "ÜST":
            ust += 1
        result = str(match.get("result") or "").strip()
        if result == "Başarılı":
            resolved += 1
            success += 1
        elif result == "Başarısız":
            resolved += 1
            fail += 1
        elif result == "İade":
            resolved += 1
            push += 1
    rate = round((success / resolved) * 100, 1) if resolved else None
    return {
        "total": len(matches),
        "resolved": resolved,
        "success": success,
        "fail": fail,
        "push": push,
        "alt": alt,
        "ust": ust,
        "win_rate": rate,
    }


def _serialize_team_history_entry(row: dict) -> dict:
    return {
        "id": int(row.get("id") or 0),
        "match_id": str(row.get("match_id") or ""),
        "match_name": str(row.get("match_name") or ""),
        "tournament": str(row.get("tournament") or ""),
        "direction": _normalize_direction(row.get("direction")),
        "opening": row.get("opening"),
        "live": row.get("live"),
        "diff": row.get("diff"),
        "status": str(row.get("status") or ""),
        "score": str(row.get("score") or ""),
        "result": str(row.get("result") or ""),
        "alerted_at": row.get("alerted_at") or "",
        "deleted_at": row.get("deleted_at") or "",
        "url": str(row.get("url") or ""),
    }


def _build_team_history(match_name: str, current_match_id: str) -> dict:
    home_team, away_team = _split_match_teams(match_name or "")
    deleted_rows = db.recent_deleted_alerts(limit=None)
    current_match_id = str(current_match_id or "").strip()

    def matches_team(row: dict, team: str) -> bool:
        if not team:
            return False
        row_home, row_away = _split_match_teams(row.get("match_name") or "")
        team_key = team.casefold()
        return row_home.casefold() == team_key or row_away.casefold() == team_key

    def collect(team: str) -> list[dict]:
        if not team:
            return []
        seen_match_ids: set[str] = set()
        out: list[dict] = []
        for row in deleted_rows:
            row_match_id = str(row.get("match_id") or "").strip()
            if row_match_id and row_match_id == current_match_id:
                continue
            if row_match_id and row_match_id in seen_match_ids:
                continue
            if not matches_team(row, team):
                continue
            if row_match_id:
                seen_match_ids.add(row_match_id)
            out.append(_serialize_team_history_entry(row))
        return out

    home_matches = collect(home_team)
    away_matches = collect(away_team)

    return {
        "match_id": current_match_id,
        "match_name": str(match_name or ""),
        "teams": [
            {
                "role": "Ev",
                "name": home_team,
                "matches": home_matches,
                "summary": _team_history_summary(home_matches),
            },
            {
                "role": "Dep",
                "name": away_team,
                "matches": away_matches,
                "summary": _team_history_summary(away_matches),
            },
        ],
    }


@app.route("/api/alerts/<int:alert_id>/team-history")
def api_alert_team_history(alert_id: int):
    alert = db.get_alert(alert_id)
    if not alert:
        for row in db.recent_deleted_alerts(limit=None):
            if int(row.get("id") or 0) == alert_id:
                alert = row
                break
    if not alert:
        return jsonify({"error": "not found"}), 404
    payload = _build_team_history(
        alert.get("match_name") or "",
        alert.get("match_id") or "",
    )
    payload["alert_id"] = alert_id
    return jsonify(payload)


@app.route("/api/team-history")
def api_team_history_lookup():
    match_name = (request.args.get("match_name") or "").strip()
    match_id = (request.args.get("match_id") or "").strip()
    if not match_name:
        return jsonify({"error": "match_name required"}), 400
    return jsonify(_build_team_history(match_name, match_id))


@app.route("/api/clear", methods=["POST"])
def api_clear_db():
    moved_count = _archive_all_active_rows()
    return jsonify({"cleared": True, "moved_count": moved_count})


if __name__ == "__main__":
    port = int(os.getenv("DASHBOARD_PORT", "5151"))
    app.run(host="0.0.0.0", port=port, debug=False)
