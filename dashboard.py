"""
dashboard.py — Anomaly detection dashboard.
Flask-powered web interface.

Usage:
    python dashboard.py
"""

import os
import re
from datetime import datetime, timedelta
from typing import Optional
from flask import Flask, jsonify, render_template, request
from db import Database
from config import Config
from finished_matches import finished_matches_bp

config = Config()
db = Database(config.DB_PATH)
db.init()

app = Flask(__name__, template_folder="templates", static_folder="static")
app.register_blueprint(finished_matches_bp)

BET_BUILDER_ALERT_WINDOW_MINUTES = 240


# ──────────────────────────────────────────────────────────────────────────────
# Projected Score Calculation
# ──────────────────────────────────────────────────────────────────────────────

def calculate_projected_score(score: str, status: str, match_name: str = "", tournament: str = "") -> Optional[float]:
    if not score: return None
    match = re.search(r'(\d+)\s*[-–]\s*(\d+)', score.strip())
    if not match: return None
    total_score = float(match.group(1)) + float(match.group(2))

    period = None
    remaining_min = None
    status_clean = (status or "").strip()
    
    if not status_clean or re.match(r'^OT', status_clean, re.IGNORECASE):
        return None
        
    if re.match(r'^HT$', status_clean, re.IGNORECASE):
        period = 2
        remaining_min = 0.0
    else:
        m1 = re.search(r'(?:Q(\d)|(\d)Q|(\d{1,2}))[-:\s]*(\d{1,2}):(\d{2})', status_clean, re.IGNORECASE)
        if m1:
            period = int(m1.group(1) or m1.group(2) or m1.group(3))
            remaining_min = int(m1.group(4)) + int(m1.group(5)) / 60.0
            
    if period is None or remaining_min is None:
        return None

    text_to_check = f"{match_name} {tournament}".upper()
    if "NBA" in text_to_check:
        quarter_length = 12
        total_game_min = 48
    elif "NCAA" in text_to_check:
        quarter_length = 20
        total_game_min = 40
    else:
        quarter_length = 10
        total_game_min = 40
        
    if quarter_length == 20:
        elapsed_min = (period - 1) * 20 + (20 - remaining_min)
    else:
        elapsed_min = (period - 1) * quarter_length + (quarter_length - remaining_min)

    if elapsed_min < 1:
        return None
        
    projected_total = (total_score / elapsed_min) * total_game_min
    return round(projected_total, 1)


def enrich_alerts_with_projection(alerts: list[dict]) -> list[dict]:
    for alert in alerts:
        alert["projected"] = calculate_projected_score(
            score=alert.get("score", ""),
            status=alert.get("status", ""),
            match_name=alert.get("match_name", ""),
            tournament=alert.get("tournament", "")
        )
    return alerts


def _classify_projection_signal(projected: float, live: float) -> dict | None:
    projection_gap = round(live - projected, 1)
    absolute_gap = abs(projection_gap)

    if absolute_gap < 2:
        return None

    if absolute_gap >= 10:
        tier = "A"
        priority = 3
    elif absolute_gap >= 5:
        tier = "B"
        priority = 2
    else:
        tier = "C"
        priority = 1

    direction = "ALT" if projection_gap > 0 else "ÜST"
    return {
        "direction": direction,
        "tier": tier,
        "signal_code": f"{direction}-{tier}",
        "priority": priority,
        "projection_gap": projection_gap,
        "projection_edge": round(absolute_gap, 1),
    }


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


def build_bet_builder(max_count: int) -> dict:
    alerts = [
        alert
        for alert in enrich_alerts_with_projection(db.recent_alerts(limit=500))
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

        projected = alert.get("projected")
        if projected is None:
            continue

        opening = float(alert.get("opening") or 0)
        live = float(alert.get("live") or 0)
        signal = _classify_projection_signal(float(projected), live)
        if signal is None:
            continue

        opening_gap = round(live - opening, 1)
        candidates.append({
            "match_id": alert.get("match_id"),
            "match_name": alert.get("match_name", ""),
            "tournament": alert.get("tournament", ""),
            "url": alert.get("url", ""),
            "direction": signal["direction"],
            "signal_tier": signal["tier"],
            "signal_code": signal["signal_code"],
            "opening": round(opening, 1),
            "live": round(live, 1),
            "projected": round(float(projected), 1),
            "status": alert.get("status", ""),
            "score": alert.get("score", ""),
            "opening_gap": opening_gap,
            "projection_gap": signal["projection_gap"],
            "projection_edge": signal["projection_edge"],
            "signal_priority": signal["priority"],
            "bet_placed": int(alert.get("bet_placed") or 0),
            "followed": int(alert.get("followed") or 0),
            "ignored": int(alert.get("ignored") or 0),
        })

    candidates.sort(
        key=lambda item: (
            item["signal_priority"],
            item["projection_edge"],
        ),
        reverse=True,
    )

    eligible_candidates = candidates
    leg_count = min(max(max_count, 1), len(eligible_candidates))
    can_build = leg_count >= 1
    slip = eligible_candidates[:leg_count] if can_build else []

    if not can_build:
        message = (
            f"Kupon oluşturulmadı. Canlı maçlar içinde en az 1 uygun maç gerekiyor; şu an yalnızca "
            f"{len(eligible_candidates)} maç projeksiyon-canlı barem farkı kriterini geçti."
        )
    else:
        message = (
            f"Kupon hazır. Canlı {len(eligible_candidates)} uygun maç içinden en güçlü {leg_count} seçim alındı."
        )

    excluded_total = excluded_finished + excluded_ignored + excluded_bet + excluded_follow + excluded_saved
    if excluded_total > 0:
        message += (
            f" {excluded_total} maç daha önce işaretlendiği/kaydedildiği için otomatik dışlandı "
            f"(Biten: {excluded_finished}, Gözardı: {excluded_ignored}, Bahis: {excluded_bet}, Takip: {excluded_follow}, Eski kupon: {excluded_saved})."
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
        "opening",
        "live",
        "projected",
        "opening_gap",
        "projection_gap",
        "projection_edge",
    }
    numeric_int_fields = {"signal_priority"}
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


def _normalize_text_key(value: str) -> str:
    return (
        str(value or "")
        .strip()
        .lower()
        .replace("ı", "i")
        .replace("ş", "s")
        .replace("ğ", "g")
        .replace("ü", "u")
        .replace("ö", "o")
        .replace("ç", "c")
    )


def _evaluate_leg_result(direction: str, live_line: float, final_total: float) -> str:
    if abs(final_total - live_line) < 1e-9:
        return "İade"
    if str(direction or "").upper() == "ALT":
        return "Başarılı" if final_total < live_line else "Başarısız"
    return "Başarılı" if final_total > live_line else "Başarısız"


def evaluate_saved_bet_slip(payload: dict) -> dict:
    raw_slip = payload.get("slip")
    slip = raw_slip if isinstance(raw_slip, list) else []

    match_ids = []
    for leg in slip:
        if not isinstance(leg, dict):
            continue
        match_id = str(leg.get("match_id") or "").strip()
        if match_id and match_id not in match_ids:
            match_ids.append(match_id)

    finished_by_match = db.latest_finished_by_match_ids(match_ids)
    live_by_match = db.latest_alerts_by_match_ids(match_ids)

    details = []
    success_count = 0
    fail_count = 0
    push_count = 0
    pending_count = 0
    unknown_count = 0

    for idx, leg in enumerate(slip, start=1):
        leg_item = leg if isinstance(leg, dict) else {}
        match_id = str(leg_item.get("match_id") or "").strip()
        direction = str(leg_item.get("direction") or "").strip()
        match_name = str(leg_item.get("match_name") or "")
        tournament = str(leg_item.get("tournament") or "")
        signal_code = str(leg_item.get("signal_code") or "")

        try:
            live_line = float(leg_item.get("live") or 0)
        except (TypeError, ValueError):
            live_line = 0.0
        try:
            projected = float(leg_item.get("projected") or 0)
        except (TypeError, ValueError):
            projected = 0.0

        finished = finished_by_match.get(match_id, {})
        live = live_by_match.get(match_id, {})
        final_total_raw = finished.get("final_total")

        status_key = "unknown"
        result_label = "Bilinmiyor"
        final_total = None

        if final_total_raw is not None:
            try:
                final_total = float(final_total_raw)
            except (TypeError, ValueError):
                final_total = None

        if final_total is not None:
            result_label = _evaluate_leg_result(direction, live_line, final_total)
            result_key = _normalize_text_key(result_label)
            if result_key == "basarili":
                status_key = "success"
                success_count += 1
            elif result_key == "basarisiz":
                status_key = "fail"
                fail_count += 1
            else:
                status_key = "push"
                push_count += 1
        elif live:
            status_key = "pending"
            result_label = "Bekliyor"
            pending_count += 1
        else:
            status_key = "unknown"
            result_label = "Bilinmiyor"
            unknown_count += 1

        details.append({
            "index": idx,
            "match_id": match_id,
            "match_name": match_name,
            "tournament": tournament,
            "signal_code": signal_code,
            "direction": direction,
            "live_line": round(live_line, 1),
            "projected": round(projected, 1),
            "status_key": status_key,
            "result_label": result_label,
            "final_status": str(finished.get("final_status") or ""),
            "final_score": str(finished.get("final_score") or ""),
            "final_total": final_total,
            "finished_at": str(finished.get("finished_at") or ""),
            "live_status": str(live.get("status") or ""),
            "live_score": str(live.get("score") or ""),
            "last_alerted_at": str(live.get("alerted_at") or ""),
            "bet_placed": 1 if bool(leg_item.get("bet_placed") or live.get("bet_placed")) else 0,
            "followed": 1 if bool(leg_item.get("followed") or live.get("followed")) else 0,
            "ignored": 1 if bool(leg_item.get("ignored") or live.get("ignored")) else 0,
        })

    total = len(slip)
    resolved_count = success_count + fail_count + push_count

    if total == 0:
        overall = "Kupon boş"
    elif resolved_count == total:
        overall = "Başarısız" if fail_count > 0 else "Başarılı"
    elif resolved_count > 0:
        overall = "Kısmi Sonuç"
    elif pending_count > 0:
        overall = "Beklemede"
    else:
        overall = "Bilinmiyor"

    return {
        "overall": overall,
        "total": total,
        "resolved_count": resolved_count,
        "success_count": success_count,
        "fail_count": fail_count,
        "push_count": push_count,
        "pending_count": pending_count,
        "unknown_count": unknown_count,
        "details": details,
    }


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/alerts")
def api_alerts():
    """Returns all anomaly records with projected scores."""
    return jsonify(enrich_alerts_with_projection(db.recent_alerts(limit=500)))


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


@app.route("/api/bet-builder/saved/<int:slip_id>/check")
def api_saved_bet_builder_check(slip_id: int):
    saved = db.get_saved_bet_slip(slip_id)
    if not saved:
        return jsonify({"error": "not found"}), 404

    payload = normalize_bet_builder_payload(saved.get("payload"))
    if not payload:
        return jsonify({"error": "invalid payload"}), 400

    return jsonify({
        "id": saved.get("id"),
        "name": saved.get("name"),
        "created_at": saved.get("created_at"),
        "check": evaluate_saved_bet_slip(payload),
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
    """Toggle bet placed/not placed for all alerts of the same match."""
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
    """Toggle ignore status for all alerts of the same match."""
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
    """Toggle follow status for all alerts of the same match."""
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


@app.route("/api/alerts/<int:alert_id>", methods=["DELETE"])
def api_delete_alert(alert_id: int):
    """Delete all records belonging to the same match."""
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
    """Wipe all alerts, match_actions and opening_lines."""
    db.clear_all()
    return jsonify({"cleared": True})


if __name__ == "__main__":
    port = int(os.getenv("DASHBOARD_PORT", "5050"))
    app.run(host="0.0.0.0", port=port, debug=False)
