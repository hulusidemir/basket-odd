"""
dashboard.py — Anomaly detection dashboard.
Flask-powered web interface.

Usage:
    python dashboard.py
"""

import os
import re
from typing import Optional
from flask import Flask, jsonify, render_template
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


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/alerts")
def api_alerts():
    """Returns all anomaly records with projected scores."""
    alerts = db.recent_alerts(limit=500)
    
    # Add projected score to each alert
    for alert in alerts:
        projected = calculate_projected_score(
            score=alert.get("score", ""),
            status=alert.get("status", ""),
            match_name=alert.get("match_name", ""),
            tournament=alert.get("tournament", "")
        )
        alert["projected"] = projected
    
    return jsonify(alerts)


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
