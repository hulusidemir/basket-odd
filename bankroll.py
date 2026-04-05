"""
bankroll.py — Bankroll management blueprint.
Cumulative betting tracker with group-based money splitting.

Registers a Flask Blueprint; does NOT modify any existing routes.
Wire it up via:
    from bankroll import bankroll_bp
    app.register_blueprint(bankroll_bp)
"""

import json
from flask import Blueprint, render_template, jsonify, request
from db import Database
from config import Config

config = Config()
db = Database(config.DB_PATH)

bankroll_bp = Blueprint("bankroll", __name__, template_folder="templates")


def _init_bankroll_table():
    """Create bankroll_sessions table if not exists."""
    with db._conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS bankroll_sessions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL DEFAULT '',
                budget      REAL NOT NULL,
                group_count INTEGER NOT NULL,
                default_rate REAL NOT NULL,
                state_json  TEXT NOT NULL,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)


_init_bankroll_table()


@bankroll_bp.route("/bankroll")
def bankroll_page():
    """Render the bankroll management page."""
    return render_template("bankroll.html")


@bankroll_bp.route("/api/bankroll/sessions", methods=["GET"])
def api_list_sessions():
    """List all saved sessions."""
    with db._conn() as conn:
        rows = conn.execute(
            "SELECT id, name, budget, group_count, default_rate, state_json, created_at, updated_at "
            "FROM bankroll_sessions ORDER BY updated_at DESC"
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@bankroll_bp.route("/api/bankroll/sessions", methods=["POST"])
def api_save_session():
    """Create or update a session. If id is given, update; otherwise insert."""
    data = request.get_json(force=True)
    sid = data.get("id")
    name = data.get("name", "")
    budget = data.get("budget", 0)
    group_count = data.get("group_count", 0)
    default_rate = data.get("default_rate", 0)
    state_json = json.dumps(data.get("state", {}), ensure_ascii=False)

    with db._conn() as conn:
        if sid:
            conn.execute(
                "UPDATE bankroll_sessions SET name=?, budget=?, group_count=?, default_rate=?, "
                "state_json=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (name, budget, group_count, default_rate, state_json, sid),
            )
        else:
            cur = conn.execute(
                "INSERT INTO bankroll_sessions (name, budget, group_count, default_rate, state_json) "
                "VALUES (?, ?, ?, ?, ?)",
                (name, budget, group_count, default_rate, state_json),
            )
            sid = cur.lastrowid
    return jsonify({"id": sid, "saved": True})


@bankroll_bp.route("/api/bankroll/sessions/<int:sid>", methods=["DELETE"])
def api_delete_session(sid: int):
    """Delete a single session."""
    with db._conn() as conn:
        conn.execute("DELETE FROM bankroll_sessions WHERE id=?", (sid,))
    return jsonify({"id": sid, "deleted": True})


@bankroll_bp.route("/api/bankroll/sessions/clear", methods=["POST"])
def api_clear_sessions():
    """Delete all sessions."""
    with db._conn() as conn:
        conn.execute("DELETE FROM bankroll_sessions")
    return jsonify({"cleared": True})
