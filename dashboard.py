"""
dashboard.py — Anomaly detection dashboard.
Flask-powered web interface.

Usage:
    python dashboard.py
"""

import os
import re
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


def _parse_status_phase(status: str) -> tuple[int | None, float | None]:
    status_clean = (status or "").strip()
    if not status_clean:
        return None, None

    if re.match(r"^HT$", status_clean, re.IGNORECASE):
        return 2, 0.0

    match = re.search(r"(?:Q(\d)|(\d)Q|([1-4]))[-:\s]*(\d{1,2}):(\d{2})", status_clean, re.IGNORECASE)
    if match:
        period = int(match.group(1) or match.group(2) or match.group(3))
        remaining = int(match.group(4)) + int(match.group(5)) / 60.0
        return period, remaining

    match = re.match(r"^(Q(\d)|(\d)Q|([1-4]))$", status_clean, re.IGNORECASE)
    if match:
        period = int(match.group(2) or match.group(3) or match.group(4))
        return period, None

    return None, None


def build_bet_builder(max_count: int) -> dict:
    alerts = enrich_alerts_with_projection(db.recent_alerts(limit=500))
    latest_by_match = {}
    for alert in alerts:
        match_id = alert.get("match_id")
        if not match_id or match_id in latest_by_match:
            continue
        latest_by_match[match_id] = alert

    candidates = []
    skipped = []

    for alert in latest_by_match.values():
        projected = alert.get("projected")
        if projected is None:
            skipped.append({"match_id": alert.get("match_id"), "reason": "missing_projection"})
            continue

        opening = float(alert.get("opening") or 0)
        live = float(alert.get("live") or 0)
        direction = str(alert.get("direction") or "").upper()
        market_edge = abs(live - opening)
        projection_edge = abs(projected - live)
        projection_gap = projected - live
        period, remaining = _parse_status_phase(alert.get("status", ""))

        supports_direction = (
            (direction == "ALT" and projection_gap <= -3.0) or
            (direction == "ÜST" and projection_gap >= 3.0)
        )
        if not supports_direction:
            skipped.append({"match_id": alert.get("match_id"), "reason": "projection_disagrees"})
            continue

        if market_edge < 10 or projection_edge < 3:
            skipped.append({"match_id": alert.get("match_id"), "reason": "edge_too_small"})
            continue

        confidence = 42.0
        confidence += min((market_edge / 18.0) * 24.0, 24.0)
        confidence += min((projection_edge / 12.0) * 28.0, 28.0)
        confidence += min(float(alert.get("quality_score") or 0) * 0.12, 10.0)

        if period == 2:
            confidence += 4
        elif period == 3:
            confidence += 7
        elif period == 1:
            confidence -= 4
        elif period == 4:
            confidence -= 5
            if remaining is not None and remaining <= 6:
                confidence -= 4

        if alert.get("counter_level") == "YÜKSEK":
            confidence -= 7
        elif alert.get("counter_level") == "ORTA":
            confidence -= 4
        elif alert.get("counter_level") == "DÜŞÜK":
            confidence -= 2

        confidence = round(max(52.0, min(93.0, confidence)), 1)
        candidates.append({
            "match_id": alert.get("match_id"),
            "match_name": alert.get("match_name", ""),
            "tournament": alert.get("tournament", ""),
            "url": alert.get("url", ""),
            "direction": direction,
            "opening": round(opening, 1),
            "live": round(live, 1),
            "projected": round(float(projected), 1),
            "status": alert.get("status", ""),
            "score": alert.get("score", ""),
            "quality_score": round(float(alert.get("quality_score") or 0), 1),
            "market_edge": round(market_edge, 1),
            "projection_edge": round(projection_edge, 1),
            "confidence": confidence,
        })

    candidates.sort(
        key=lambda item: (
            item["confidence"],
            item["projection_edge"],
            item["market_edge"],
            item["quality_score"],
        ),
        reverse=True,
    )

    usable_candidates = [item for item in candidates if item["confidence"] >= 64]
    leg_count = min(max(max_count, 1), len(usable_candidates))
    can_build = leg_count >= 1
    slip = usable_candidates[:leg_count] if can_build else []

    if not can_build:
        message = (
            f"Kupon oluşturulmadı. En az 1 uygun maç gerekiyor; şu an yalnızca "
            f"{len(usable_candidates)} maç projeksiyon ve barem filtresini geçti."
        )
    else:
        message = (
            f"Kupon hazır. {len(usable_candidates)} uygun maç içinden en güçlü {leg_count} seçim alındı."
        )

    return {
        "created": can_build,
        "requested_max_count": max(max_count, 1),
        "selected_count": leg_count if can_build else 0,
        "eligible_count": len(usable_candidates),
        "total_candidates": len(candidates),
        "message": message,
        "slip": slip,
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
