"""Flask dashboard for raw opening-versus-live total-line signals."""

import asyncio
import csv
import io
import json
import logging
import math
import os
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from flask import Flask, Response, jsonify, render_template, request

from config import Config
from db import Database
from finished_match_service import (
    run_active_match_finished_scan,
    run_deleted_match_result_cycle,
    run_single_deleted_match_result_check,
)
from match_state import current_pace_projection
from signal_lists import (
    build_signal_list_markers,
    build_signal_list_profile,
    split_match_teams,
)
from upcoming_app import upcoming_bp


logger = logging.getLogger(__name__)
config = Config()
db = Database(config.DB_PATH)
db.init()
app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.register_blueprint(upcoming_bp)


def _normalize_direction(value) -> str:
    text = str(value or "").strip().upper().replace("UST", "ÜST")
    return text if text in {"ALT", "ÜST"} else ""


def _sanitize_tournament(value: str) -> str:
    text = str(value or "").strip()
    if re.search(r"standings|popular|trending|featured", text, re.IGNORECASE):
        return ""
    return text


def _stored_quarter_scores(value) -> dict:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _raw_alert(row: dict, list_profile: dict | None = None) -> dict:
    item = dict(row)
    item["direction"] = _normalize_direction(item.get("direction"))
    item["tournament"] = _sanitize_tournament(item.get("tournament"))
    try:
        item["barem_change"] = round(float(item["live"]) - float(item["opening"]), 2)
    except (KeyError, TypeError, ValueError):
        item["barem_change"] = None
    pace = current_pace_projection(
        str(item.get("score") or ""),
        str(item.get("status") or ""),
        str(item.get("match_name") or ""),
        str(item.get("tournament") or ""),
        quarter_scores=_stored_quarter_scores(item.get("quarter_scores_json")),
        live_total=item.get("live"),
    )
    item["pace_projection"] = pace["total"]
    item["pace_ppm"] = pace["ppm"]
    item["pace_score_total"] = pace["score_total"]
    item["pace_elapsed_minutes"] = pace["elapsed_minutes"]
    item["pace_game_minutes"] = pace["game_minutes"]
    item["quarter_pace_periods"] = pace["periods"]
    item["ppm_comparison"] = dict(pace["comparison"])
    try:
        opening_ppm = float(item["opening"]) / float(pace["game_minutes"])
        item["opening_ppm"] = opening_ppm if math.isfinite(opening_ppm) else None
    except (KeyError, TypeError, ValueError, ZeroDivisionError):
        item["opening_ppm"] = None
    comparison = item["ppm_comparison"]
    minutes = comparison.get("remaining_minutes")
    seconds = math.floor(minutes * 60 + 0.5) if isinstance(minutes, (int, float)) and math.isfinite(minutes) else None
    comparison["remaining_time"] = f"{seconds // 60}:{seconds % 60:02d}" if seconds is not None else None
    pace_direction = {"above": "ÜST", "below": "ALT"}.get(comparison.get("status"), "")
    comparison["heading"] = f"Tempo {pace_direction} yönünde" if pace_direction else "Tempo yorumu"
    comparison["signal_relation"] = None
    comparison["signal_relation_tone"] = None
    if pace_direction and item["direction"] in {"ALT", "ÜST"}:
        aligned = pace_direction == item["direction"]
        comparison["signal_relation"] = f"{item['direction']} sinyaliyle {'uyumlu' if aligned else 'ters'}"
        comparison["signal_relation_tone"] = "aligned" if aligned else "opposed"
    try:
        timestamp = datetime.fromisoformat(str(item.get("alerted_at") or "").replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        item["signal_time"] = timestamp.astimezone(ZoneInfo("Europe/Istanbul")).strftime("%H:%M")
    except ValueError:
        item["signal_time"] = None
    item["list_markers"] = build_signal_list_markers(item, list_profile)
    for key in (
        "display_snapshot",
        "quarter_scores_json",
        "telegram_message_ids",
        "telegram_last_error",
    ):
        item.pop(key, None)
    return item


def _build_live_dashboard_rows(rows: list[dict]) -> list[dict]:
    profile = build_signal_list_profile(db.list_signal_list_entries())
    result = [_raw_alert(row, profile) for row in rows]
    followed = db.upcoming_followed_match_ids([row.get("match_id") for row in result])
    for row in result:
        row["upcoming_followed"] = int(str(row.get("match_id") or "") in followed)
    return result


def _dashboard_snapshot_payloads(rows: list[dict]) -> dict[int, dict]:
    captured_at = datetime.now(timezone.utc).isoformat()
    snapshots: dict[int, dict] = {}
    history_rows = _unique_signal_rows(_deleted_rows()) if rows else []
    histories = {}
    for row in rows:
        payload = dict(row)
        history_key = (str(row.get("match_name") or ""), str(row.get("match_id") or ""))
        if history_key not in histories:
            histories[history_key] = _team_history(*history_key, history_rows=history_rows)
        payload["team_history"] = histories[history_key]
        payload["snapshot_meta"] = {
            "schema_version": 2,
            "source": "raw_line_dashboard",
            "captured_at": captured_at,
        }
        alert_id = int(payload.get("id") or 0)
        if alert_id:
            snapshots[alert_id] = payload
    return snapshots


def _archive_active_match(match_id: str) -> int:
    rows = db.active_alerts_for_match(match_id)
    if not rows:
        return 0
    rendered = _build_live_dashboard_rows(rows)
    return db.archive_match_with_display_snapshots(
        match_id,
        _dashboard_snapshot_payloads(rendered),
    )


def _archive_all_active_rows() -> int:
    rows = db.all_active_alerts(limit=None)
    if not rows:
        return 0
    rendered = _build_live_dashboard_rows(rows)
    return db.archive_all_with_display_snapshots(
        _dashboard_snapshot_payloads(rendered)
    )


def _run_async_dashboard_job(name: str, coroutine, failure_message: str):
    try:
        return jsonify(asyncio.run(coroutine))
    except Exception as exc:
        logger.exception("Dashboard job failed (%s): %s", name, exc)
        return jsonify({"error": failure_message}), 500


def _frozen_deleted_alert(row: dict) -> dict:
    """Read archived display data without recalculating any live-only field."""
    stored = dict(row)
    raw_snapshot = stored.get("display_snapshot")
    snapshot: dict = {}
    if isinstance(raw_snapshot, dict):
        snapshot = dict(raw_snapshot)
    elif raw_snapshot:
        try:
            parsed = json.loads(str(raw_snapshot))
            if isinstance(parsed, dict):
                snapshot = parsed
        except (TypeError, ValueError, json.JSONDecodeError):
            snapshot = {}

    item = {key: value for key, value in stored.items() if key != "display_snapshot"}
    item.update(snapshot)
    # Final observations may arrive after archiving. They are persisted facts,
    # not recalculated dashboard display fields.
    for key in (
        "id",
        "match_id",
        "deleted_at",
        "result",
        "result_source",
        "settled_at",
        "final_status",
        "final_score",
    ):
        if key in stored:
            item[key] = stored[key]
    item["final_total"] = stored.get("final_total")
    if "pace_projection" not in snapshot:
        item["pace_projection"] = None
        item["pace_score_total"] = None
        item["pace_elapsed_minutes"] = None
        item["pace_game_minutes"] = None
    for key in ("opening_ppm", "team_history", "signal_time"):
        item[key] = snapshot.get(key)
    if "pace_ppm" not in snapshot:
        item["pace_ppm"] = None
    if "quarter_pace_periods" not in snapshot:
        item["quarter_pace_periods"] = []
    if "ppm_comparison" not in snapshot:
        item["ppm_comparison"] = None
    if "barem_change" not in snapshot:
        item["barem_change"] = None
    if "list_markers" not in snapshot:
        item["list_markers"] = []
    for key in (
        "display_snapshot",
        "quarter_scores_json",
        "telegram_message_ids",
        "telegram_last_error",
    ):
        item.pop(key, None)
    return item


def _deleted_rows() -> list[dict]:
    return [_frozen_deleted_alert(row) for row in db.recent_deleted_alerts(limit=None)]


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/deleted-matches")
def deleted_matches():
    return render_template("deleted_matches.html")


@app.route("/api/alerts")
def api_alerts():
    return jsonify(_build_live_dashboard_rows(db.recent_alerts(limit=500)))


@app.route("/api/signal-lists")
def api_signal_lists():
    return jsonify(db.list_signal_list_entries())


@app.route("/api/signal-lists", methods=["POST"])
def api_add_signal_list_entry():
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({"error": "Geçersiz liste kaydı."}), 400
    entry = db.add_signal_list_entry(
        payload.get("list_type") or "",
        payload.get("scope") or "",
        payload.get("value") or "",
    )
    if entry is None:
        return jsonify({"error": "Liste ve tür seçin; 1–200 karakterlik bir ad girin."}), 400
    return jsonify({"entry": entry})


@app.route("/api/signal-lists/<int:entry_id>", methods=["DELETE"])
def api_delete_signal_list_entry(entry_id: int):
    if not db.delete_signal_list_entry(entry_id):
        return jsonify({"error": "Kayıt zaten kaldırılmış. Listeyi yenileyin."}), 404
    return jsonify({"id": entry_id, "deleted": True})


@app.route("/api/deleted-matches")
def api_deleted_matches():
    return jsonify(_deleted_rows())


@app.route("/api/deleted-matches/<int:alert_id>/details")
def api_deleted_match_details(alert_id: int):
    row = db.get_deleted_alert_by_id(alert_id)
    if not row:
        return jsonify({"error": "not_found"}), 404
    return jsonify(_frozen_deleted_alert(row))


@app.route("/api/deleted-matches/export.csv")
def api_export_finished_deleted_matches_csv():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "id", "match_id", "match_name", "tournament", "direction",
        "opening", "live", "barem_change", "status", "score",
        "final_status", "final_score", "result", "alerted_at", "deleted_at",
    ])
    for row in _deleted_rows():
        writer.writerow([row.get(key, "") for key in (
            "id", "match_id", "match_name", "tournament", "direction",
            "opening", "live", "barem_change", "status", "score",
            "final_status", "final_score", "result", "alerted_at", "deleted_at",
        )])
    return Response(
        output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=silinen-sinyaller.csv"},
    )


@app.route("/api/deleted-matches/clear", methods=["POST"])
def api_clear_deleted_matches():
    return jsonify(db.purge_deleted_matches())


@app.route("/api/deleted-matches/<int:alert_id>", methods=["DELETE"])
def api_purge_deleted_alert(alert_id: int):
    if not db.delete_alert(alert_id):
        return jsonify({"error": "not_found"}), 404
    return jsonify({"id": alert_id, "deleted": True})


@app.route("/api/alerts/check-finished", methods=["POST"])
def api_check_active_match_finished():
    return _run_async_dashboard_job(
        "active_finished",
        run_active_match_finished_scan(db, config, before_delete=_archive_active_match),
        "Biten maçlar kontrol edilemedi.",
    )


@app.route("/api/deleted-matches/check-results", methods=["POST"])
def api_check_deleted_match_results():
    return _run_async_dashboard_job(
        "deleted_results",
        run_deleted_match_result_cycle(db, config),
        "Silinen maç sonuçları kontrol edilemedi.",
    )


@app.route("/api/deleted-matches/<int:alert_id>/check-result", methods=["POST"])
def api_check_single_deleted_match_result(alert_id: int):
    return _run_async_dashboard_job(
        "single_deleted_result",
        run_single_deleted_match_result_check(db, config, alert_id),
        "Maç sonucu kontrol edilemedi.",
    )


def _unique_signal_rows(rows: list[dict]) -> list[dict]:
    """Keep the earliest signal for every match and direction pair."""
    chosen: dict[tuple[str, str], dict] = {}
    for index, row in enumerate(rows):
        match_id = str(row.get("match_id") or f"alert:{row.get('id') or index}").strip()
        direction = _normalize_direction(row.get("direction")) or str(row.get("direction") or "").strip()
        key = (match_id, direction)
        current = chosen.get(key)
        try:
            signal_number = int(row.get("signal_count") or 1)
        except (TypeError, ValueError):
            signal_number = 1
        try:
            current_number = int(current.get("signal_count") or 1) if current else None
        except (TypeError, ValueError):
            current_number = 1
        if current is None or signal_number < current_number:
            chosen[key] = row
    return list(chosen.values())


def _basic_result_report(rows: list[dict]) -> dict:
    settled_rows = [
        row for row in rows
        if row.get("result") in {"Başarılı", "Başarısız"}
    ]

    def breakdown(selected_rows: list[dict]) -> dict:
        unique_rows = _unique_signal_rows(selected_rows)
        successful = sum(
            row.get("result") == "Başarılı" for row in selected_rows
        )
        unique_successful = sum(
            row.get("result") == "Başarılı" for row in unique_rows
        )
        return {
            "total": len(selected_rows),
            "successful": successful,
            "failed": sum(
                row.get("result") == "Başarısız" for row in selected_rows
            ),
            "success_rate": (
                round(successful * 100 / len(selected_rows), 1)
                if selected_rows else None
            ),
            "unique_total": len(unique_rows),
            "unique_successful": unique_successful,
            "unique_failed": sum(
                row.get("result") == "Başarısız" for row in unique_rows
            ),
            "unique_success_rate": (
                round(unique_successful * 100 / len(unique_rows), 1)
                if unique_rows else None
            ),
        }

    directions = {
        direction: breakdown([
            row for row in settled_rows
            if _normalize_direction(row.get("direction")) == direction
        ])
        for direction in ("ALT", "ÜST")
    }
    return {
        **breakdown(settled_rows),
        "directions": directions,
    }


@app.route("/api/deleted-matches/report")
@app.route("/api/deleted-matches/insights")
def api_deleted_matches_report():
    return jsonify(_basic_result_report(_deleted_rows()))


@app.route("/api/matches/<path:match_id>/ignore", methods=["POST"])
def api_ignore_match(match_id: str):
    match_key = str(match_id or "").strip()
    if not match_key:
        return jsonify({"error": "match_id is required"}), 400
    affected = db.set_match_statuses(
        match_key, ignored=True, bet_placed=False, followed=False
    )
    return jsonify({"match_id": match_key, "ignored": 1, "affected": affected})


def _toggle_alert_status(alert_id: int, field: str):
    alert = db.get_alert(alert_id)
    if not alert:
        return jsonify({"error": "not_found"}), 404
    value = not bool(alert.get(field))
    kwargs = {field: value}
    if value:
        for other in {"bet_placed", "ignored", "followed"} - {field}:
            kwargs[other] = False
    affected = db.set_match_statuses(alert["match_id"], **kwargs)
    return jsonify({
        "id": alert_id,
        "match_id": alert["match_id"],
        field: int(value),
        "affected": affected,
    })


@app.route("/api/alerts/<int:alert_id>/bet", methods=["POST"])
def api_toggle_bet(alert_id: int):
    return _toggle_alert_status(alert_id, "bet_placed")


@app.route("/api/alerts/<int:alert_id>/ignore", methods=["POST"])
def api_toggle_ignore(alert_id: int):
    return _toggle_alert_status(alert_id, "ignored")


@app.route("/api/alerts/<int:alert_id>/follow", methods=["POST"])
def api_toggle_follow(alert_id: int):
    return _toggle_alert_status(alert_id, "followed")


@app.route("/api/alerts/<int:alert_id>/note", methods=["POST"])
def api_update_alert_note(alert_id: int):
    alert = db.get_alert(alert_id)
    if not alert:
        return jsonify({"error": "not_found"}), 404
    note = str((request.get_json(silent=True) or {}).get("note") or "").strip()[:240]
    affected = db.update_match_note(alert["match_id"], note)
    return jsonify({"id": alert_id, "note": note, "affected": affected})


@app.route("/api/alerts/<int:alert_id>", methods=["DELETE"])
def api_delete_alert(alert_id: int):
    alert = db.get_alert(alert_id)
    if not alert:
        return jsonify({"error": "not_found"}), 404
    affected = _archive_active_match(alert["match_id"])
    if affected <= 0:
        return jsonify({"error": "active_alert_changed"}), 409
    return jsonify({"id": alert_id, "match_id": alert["match_id"], "affected": affected})


def _history_display_row(row: dict) -> dict:
    """Prepare live modal history facts once, before they become a snapshot."""
    item = {key: row.get(key) for key in (
        "id", "match_id", "match_name", "url", "direction", "opening", "live",
        "barem_change", "final_score", "result",
    )}
    scores = re.fullmatch(r"\s*(\d{1,3})\s*[-–]\s*(\d{1,3})\s*", str(item.get("final_score") or ""))
    total = sum(map(int, scores.groups())) if scores else None
    item["final_total"] = total if total is not None and 60 <= total <= 400 else None
    item["verdict"] = None
    try:
        line = float(item["live"])
        direction = _normalize_direction(item["direction"])
        if item["final_total"] is not None and math.isfinite(line) and direction:
            won = total < line if direction == "ALT" else total > line
            item["verdict"] = {
                "relation": "=" if total == line else ">" if total > line else "<",
                "margin": abs(total - line),
                "label": "İade" if total == line else f"{direction} {'tuttu' if won else 'kaybetti'}",
                "tone": "" if total == line else "success" if won else "failed",
            }
    except (TypeError, ValueError):
        pass
    return item


def _team_history(match_name: str, current_match_id: str, *, history_rows=None) -> dict:
    teams = split_match_teams(match_name)
    rows = _unique_signal_rows(_deleted_rows()) if history_rows is None else history_rows
    result = []
    for role, team in zip(("Ev", "Dep"), teams):
        matches = []
        seen = set()
        for row in rows:
            match_id = str(row.get("match_id") or "")
            direction = _normalize_direction(row.get("direction"))
            signal_key = (match_id, direction)
            if not team or match_id == current_match_id or signal_key in seen:
                continue
            if team.casefold() not in {
                value.casefold() for value in split_match_teams(row.get("match_name") or "") if value
            }:
                continue
            seen.add(signal_key)
            matches.append(row)
        result.append({
            "role": role,
            "name": team,
            # Freeze only visible history facts, never nest whole snapshots.
            "matches": [_history_display_row(row) for row in matches[:5]],
            "summary": _basic_result_report(matches),
        })
    return {"match_id": current_match_id, "match_name": match_name, "teams": result}


@app.route("/api/alerts/<int:alert_id>/team-history")
def api_alert_team_history(alert_id: int):
    alert = db.get_alert(alert_id)
    if not alert:
        archived = db.get_deleted_alert_by_id(alert_id)
        if not archived:
            return jsonify({"error": "not_found"}), 404
        frozen = _frozen_deleted_alert(archived)
        return jsonify({**(frozen.get("team_history") or {"teams": []}), "alert_id": alert_id})
    payload = _team_history(
        str(alert.get("match_name") or ""),
        str(alert.get("match_id") or ""),
    )
    payload["alert_id"] = alert_id
    return jsonify(payload)


@app.route("/api/team-history")
def api_team_history_lookup():
    match_name = str(request.args.get("match_name") or "").strip()
    if not match_name:
        return jsonify({"error": "match_name required"}), 400
    return jsonify(_team_history(match_name, str(request.args.get("match_id") or "")))


@app.route("/api/clear", methods=["POST"])
def api_clear_db():
    return jsonify({"cleared": True, "moved_count": _archive_all_active_rows()})


if __name__ == "__main__":
    port = int(os.getenv("DASHBOARD_PORT", "5151"))
    app.run(host="0.0.0.0", port=port, debug=False)
