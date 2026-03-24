"""
dashboard.py — Anomaly detection dashboard.
Flask-powered web interface.

Usage:
    python dashboard.py
"""

import os
from flask import Flask, jsonify, render_template, request
from db import Database
from config import Config

config = Config()
db = Database(config.DB_PATH)
db.init()

app = Flask(__name__, template_folder="templates", static_folder="static")


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/alerts")
def api_alerts():
    """Returns all anomaly records."""
    alerts = db.recent_alerts(limit=500)
    return jsonify(alerts)


@app.route("/api/alerts/<int:alert_id>/bet", methods=["POST"])
def api_toggle_bet(alert_id: int):
    """Toggle bet placed/not placed."""
    alert = db.get_alert(alert_id)
    if not alert:
        return jsonify({"error": "not found"}), 404
    new_val = not bool(alert["bet_placed"])
    db.set_bet_placed(alert_id, new_val)
    return jsonify({"id": alert_id, "bet_placed": int(new_val)})


@app.route("/api/alerts/<int:alert_id>/ignore", methods=["POST"])
def api_toggle_ignore(alert_id: int):
    """Toggle ignore status."""
    alert = db.get_alert(alert_id)
    if not alert:
        return jsonify({"error": "not found"}), 404
    new_val = not bool(alert["ignored"])
    db.set_ignored(alert_id, new_val)
    return jsonify({"id": alert_id, "ignored": int(new_val)})


@app.route("/api/alerts/<int:alert_id>", methods=["DELETE"])
def api_delete_alert(alert_id: int):
    """Delete a record."""
    if not db.delete_alert(alert_id):
        return jsonify({"error": "not found"}), 404
    return jsonify({"id": alert_id, "deleted": True})


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
