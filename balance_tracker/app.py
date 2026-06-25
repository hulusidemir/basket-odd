"""
Standalone and embeddable betting balance tracker.

Run:
    python -m balance_tracker.app
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import Blueprint, Flask, jsonify, render_template, request


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("BALANCE_TRACKER_DB", BASE_DIR / "balance_tracker.sqlite3"))


def create_balance_tracker_blueprint(
    name: str = "balance_tracker",
    url_prefix: str = "/balance-tracker",
    api_base: str = "/balance-tracker/api",
    home_href: str = "/",
) -> Blueprint:
    bp = Blueprint(
        name,
        __name__,
        template_folder=str(BASE_DIR / "templates"),
        static_folder=str(BASE_DIR / "static"),
        static_url_path="/balance-tracker-static",
        url_prefix=url_prefix,
    )

    init_db()

    @bp.route("/")
    def index():
        return render_template("index.html", api_base=api_base, home_href=home_href)

    @bp.get("/api/state")
    def api_state():
        active_plan_id = get_app_value("active_plan_id")
        plans = list_plans()
        if not active_plan_id and plans:
            active_plan_id = str(plans[0]["id"])
            set_app_value("active_plan_id", active_plan_id)

        active_plan = get_plan(int(active_plan_id)) if active_plan_id else None
        return jsonify(
            {
                "plans": plans,
                "activePlanId": int(active_plan_id) if active_plan_id else None,
                "activePlan": active_plan,
            }
        )

    @bp.post("/api/plans")
    def api_create_plan():
        data = request.get_json(force=True) or {}
        plan = create_plan(data)
        set_app_value("active_plan_id", str(plan["id"]))
        return jsonify({"plan": get_plan(plan["id"])})

    @bp.put("/api/plans/<int:plan_id>/activate")
    def api_activate_plan(plan_id: int):
        if not get_plan(plan_id):
            return jsonify({"error": "Plan bulunamadı"}), 404
        set_app_value("active_plan_id", str(plan_id))
        return jsonify({"activePlanId": plan_id})

    @bp.delete("/api/plans/<int:plan_id>")
    def api_delete_plan(plan_id: int):
        delete_plan(plan_id)
        active_plan_id = get_app_value("active_plan_id")
        if active_plan_id == str(plan_id):
            plans = list_plans()
            set_app_value("active_plan_id", str(plans[0]["id"]) if plans else "")
        return jsonify({"deleted": True})

    @bp.put("/api/plans/<int:plan_id>/progress")
    def api_update_progress(plan_id: int):
        data = request.get_json(force=True) or {}
        day = int(data.get("day", 0))
        actual_balance = data.get("actualBalance")
        note = str(data.get("note") or "")
        if day <= 0:
            return jsonify({"error": "Geçersiz gün"}), 400
        update_progress(plan_id, day, actual_balance, note)
        return jsonify({"saved": True})

    @bp.post("/api/plans/<int:plan_id>/withdrawals")
    def api_add_withdrawal(plan_id: int):
        data = request.get_json(force=True) or {}
        withdrawal = add_withdrawal(plan_id, data)
        return jsonify({"withdrawal": withdrawal})

    @bp.delete("/api/plans/<int:plan_id>/withdrawals/<int:withdrawal_id>")
    def api_delete_withdrawal(plan_id: int, withdrawal_id: int):
        delete_withdrawal(plan_id, withdrawal_id)
        return jsonify({"deleted": True})

    return bp


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=str(BASE_DIR / "templates"),
        static_folder=str(BASE_DIR / "static"),
        static_url_path="/balance-tracker-static",
    )
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    app.register_blueprint(
        create_balance_tracker_blueprint(
            name="balance_tracker_standalone",
            url_prefix="",
            api_base="/api",
            home_href="/",
        )
    )
    return app


def conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    with conn() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS plans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                start_balance REAL NOT NULL,
                start_date TEXT NOT NULL,
                daily_target_percent REAL NOT NULL,
                days INTEGER NOT NULL,
                currency TEXT NOT NULL DEFAULT 'TRY',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS progress (
                plan_id INTEGER NOT NULL,
                day INTEGER NOT NULL,
                actual_balance REAL,
                note TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (plan_id, day),
                FOREIGN KEY (plan_id) REFERENCES plans(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                plan_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                date TEXT NOT NULL,
                type TEXT NOT NULL DEFAULT 'one-time',
                frequency TEXT,
                note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                FOREIGN KEY (plan_id) REFERENCES plans(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS app_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )


def now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row else None


def get_app_value(key: str) -> str:
    with conn() as db:
        row = db.execute("SELECT value FROM app_state WHERE key = ?", (key,)).fetchone()
    return str(row["value"]) if row else ""


def set_app_value(key: str, value: str) -> None:
    with conn() as db:
        db.execute(
            """
            INSERT INTO app_state (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )


def list_plans() -> list[dict[str, Any]]:
    with conn() as db:
        rows = db.execute(
            """
            SELECT id, name, start_balance, start_date, daily_target_percent,
                   days, currency, created_at, updated_at
            FROM plans
            ORDER BY updated_at DESC, id DESC
            """
        ).fetchall()
    return [plan_payload(row_to_dict(row) or {}) for row in rows]


def get_plan(plan_id: int) -> dict[str, Any] | None:
    with conn() as db:
        plan_row = db.execute(
            """
            SELECT id, name, start_balance, start_date, daily_target_percent,
                   days, currency, created_at, updated_at
            FROM plans WHERE id = ?
            """,
            (plan_id,),
        ).fetchone()
        if not plan_row:
            return None
        progress_rows = db.execute(
            """
            SELECT day, actual_balance, note, updated_at
            FROM progress
            WHERE plan_id = ?
            ORDER BY day
            """,
            (plan_id,),
        ).fetchall()
        withdrawal_rows = db.execute(
            """
            SELECT id, amount, date, type, frequency, note, created_at
            FROM withdrawals
            WHERE plan_id = ?
            ORDER BY date, id
            """,
            (plan_id,),
        ).fetchall()

    payload = plan_payload(row_to_dict(plan_row) or {})
    payload["progress"] = {
        str(row["day"]): {
            "actualBalance": row["actual_balance"],
            "note": row["note"],
            "updatedAt": row["updated_at"],
        }
        for row in progress_rows
    }
    payload["withdrawals"] = [dict(row) for row in withdrawal_rows]
    return payload


def plan_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "name": row.get("name", ""),
        "settings": {
            "startBalance": row.get("start_balance", 0),
            "startDate": row.get("start_date", ""),
            "dailyTargetPercent": row.get("daily_target_percent", 0),
            "days": row.get("days", 0),
            "currency": row.get("currency", "TRY"),
        },
        "createdAt": row.get("created_at"),
        "updatedAt": row.get("updated_at"),
    }


def create_plan(data: dict[str, Any]) -> dict[str, Any]:
    settings = data.get("settings") or {}
    name = str(data.get("name") or "Bakiye Takip").strip() or "Bakiye Takip"
    start_balance = number(settings.get("startBalance"), 0)
    daily_target = number(settings.get("dailyTargetPercent"), 20)
    days = int(number(settings.get("days"), 365))
    currency = str(settings.get("currency") or "TRY").upper()[:3]
    start_date = str(settings.get("startDate") or datetime.now().date().isoformat())
    timestamp = now_iso()

    with conn() as db:
        cursor = db.execute(
            """
            INSERT INTO plans (
                name, start_balance, start_date, daily_target_percent,
                days, currency, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (name, start_balance, start_date, daily_target, days, currency, timestamp, timestamp),
        )
    return {"id": cursor.lastrowid}


def delete_plan(plan_id: int) -> None:
    with conn() as db:
        db.execute("DELETE FROM progress WHERE plan_id = ?", (plan_id,))
        db.execute("DELETE FROM withdrawals WHERE plan_id = ?", (plan_id,))
        db.execute("DELETE FROM plans WHERE id = ?", (plan_id,))


def update_progress(plan_id: int, day: int, actual_balance: Any, note: str = "") -> None:
    value = None if actual_balance in ("", None) else number(actual_balance, 0)
    with conn() as db:
        db.execute(
            """
            INSERT INTO progress (plan_id, day, actual_balance, note, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(plan_id, day) DO UPDATE SET
                actual_balance = excluded.actual_balance,
                note = excluded.note,
                updated_at = excluded.updated_at
            """,
            (plan_id, day, value, note, now_iso()),
        )
        db.execute("UPDATE plans SET updated_at = ? WHERE id = ?", (now_iso(), plan_id))


def add_withdrawal(plan_id: int, data: dict[str, Any]) -> dict[str, Any]:
    amount = number(data.get("amount"), 0)
    date = str(data.get("date") or datetime.now().date().isoformat())
    withdrawal_type = str(data.get("type") or "one-time")
    frequency = data.get("frequency") if withdrawal_type == "periodic" else None
    note = str(data.get("note") or "")
    timestamp = now_iso()
    with conn() as db:
        cursor = db.execute(
            """
            INSERT INTO withdrawals (plan_id, amount, date, type, frequency, note, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (plan_id, amount, date, withdrawal_type, frequency, note, timestamp),
        )
        db.execute("UPDATE plans SET updated_at = ? WHERE id = ?", (timestamp, plan_id))
        row = db.execute(
            """
            SELECT id, amount, date, type, frequency, note, created_at
            FROM withdrawals
            WHERE id = ?
            """,
            (cursor.lastrowid,),
        ).fetchone()
    return dict(row)


def delete_withdrawal(plan_id: int, withdrawal_id: int) -> None:
    with conn() as db:
        db.execute("DELETE FROM withdrawals WHERE plan_id = ? AND id = ?", (plan_id, withdrawal_id))
        db.execute("UPDATE plans SET updated_at = ? WHERE id = ?", (now_iso(), plan_id))


def number(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


balance_tracker_bp = create_balance_tracker_blueprint()
app = create_app()


if __name__ == "__main__":
    port = int(os.getenv("BALANCE_TRACKER_PORT", "5161"))
    app.run(host="0.0.0.0", port=port, debug=False)
