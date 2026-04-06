"""
dashboard.py — Anomaly detection dashboard.
Flask-powered web interface.

Usage:
    python dashboard.py
"""

import os
import re
from typing import Optional, Tuple
from flask import Flask, jsonify, render_template, request
from db import Database
from config import Config

config = Config()
db = Database(config.DB_PATH)
db.init()

app = Flask(__name__, template_folder="templates", static_folder="static")


# ──────────────────────────────────────────────────────────────────────────────
# Projected Score Calculation
# ──────────────────────────────────────────────────────────────────────────────

def parse_time_from_status(status: str) -> Optional[Tuple[int, float]]:
    """
    Parse quarter and remaining time from status string.
    Examples:
    - "Q4 09:29" -> (4, 9.483)  [Q4, 9:29 remaining]
    - "3rd Quarter" -> None (no time info)
    - "HT" -> None
    
    Returns: (quarter_number, remaining_minutes) or None
    """
    if not status:
        return None
    
    # Match patterns like "Q1 10:00", "Q4 05:30", etc.
    match = re.search(r'Q(\d)\s*(\d{1,2}):(\d{2})', status)
    if match:
        quarter = int(match.group(1))
        minutes = int(match.group(2))
        seconds = int(match.group(3))
        remaining_minutes = minutes + seconds / 60.0
        return (quarter, remaining_minutes)
    
    return None


def parse_score(score: str) -> Optional[Tuple[float, float]]:
    """
    Parse current score from string.
    Examples:
    - "105-98" -> (105.0, 98.0)
    - "105-98 " -> (105.0, 98.0)
    
    Returns: (home_score, away_score) or None
    """
    if not score:
        return None
    
    match = re.search(r'(\d+)\s*-\s*(\d+)', score.strip())
    if match:
        home = float(match.group(1))
        away = float(match.group(2))
        return (home, away)
    
    return None


def calculate_projected_score(score: str, status: str, total_quarter_minutes: int = 12) -> Optional[float]:
    """
    Calculate projected final total based on current pace.
    
    Args:
        score: Current score string, e.g., "105-98"
        status: Status string with quarter and time, e.g., "Q4 09:29"
        total_quarter_minutes: Minutes per quarter (NBA=12, FIBA=10)
    
    Returns:
        Projected total points or None if calculation not possible
    """
    time_info = parse_time_from_status(status)
    score_info = parse_score(score)
    
    if not time_info or not score_info:
        return None
    
    quarter, remaining_minutes = time_info
    home_score, away_score = score_info
    
    # Total game minutes
    total_game_minutes = total_quarter_minutes * 4
    
    # Calculate elapsed minutes
    elapsed_minutes = (quarter - 1) * total_quarter_minutes + (total_quarter_minutes - remaining_minutes)
    
    # Need at least some minutes elapsed to have meaningful pace
    if elapsed_minutes < 1:
        return None
    
    current_total = home_score + away_score
    pace = current_total / elapsed_minutes
    remaining_game_time = remaining_minutes + (4 - quarter) * total_quarter_minutes
    
    projected_total = current_total + (pace * remaining_game_time)
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
            status=alert.get("status", "")
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
    """Delete a record."""
    if not db.delete_alert(alert_id):
        return jsonify({"error": "not found"}), 404
    return jsonify({"id": alert_id, "deleted": True})


@app.route("/api/clear", methods=["POST"])
def api_clear_db():
    """Wipe all alerts, match_actions and opening_lines."""
    db.clear_all()
    return jsonify({"cleared": True})


@app.route("/api/alerts/<int:alert_id>/analysis")
def api_get_analysis(alert_id: int):
    """Returns AI analysis for a specific alert."""
    alert = db.get_alert(alert_id)
    if not alert:
        return jsonify({"error": "not found"}), 404
    return jsonify({"id": alert_id, "ai_analysis": alert.get("ai_analysis", "")})


if __name__ == "__main__":
    port = int(os.getenv("DASHBOARD_PORT", "5050"))
    app.run(host="0.0.0.0", port=port, debug=False)
