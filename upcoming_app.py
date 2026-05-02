"""
upcoming_app.py — "Gelecek Maçlar" Flask Blueprint.

Mounted on the main dashboard from dashboard.py:
    from upcoming_app import upcoming_bp
    app.register_blueprint(upcoming_bp)

Routes:
    GET  /upcoming           — renders templates/upcoming_matches.html
    POST /upcoming/api/fetch — runs the scraper and returns JSON
"""

import asyncio
import logging
import threading
from datetime import datetime, timezone

from flask import Blueprint, jsonify, render_template

from config import Config
from db import Database
from upcoming_scraper import UpcomingScraper

log = logging.getLogger("upcoming_app")

_fetch_lock = threading.RLock()
_fetch_state = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "matches": [],
    "count": 0,
    "saved_matches": 0,
    "saved_signals": 0,
    "error": None,
}

upcoming_bp = Blueprint(
    "upcoming",
    __name__,
    url_prefix="/upcoming",
    template_folder="templates",
)


@upcoming_bp.route("/")
def upcoming_index():
    return render_template("upcoming_matches.html")


@upcoming_bp.route("/api/fetch", methods=["POST"])
def upcoming_api_fetch():
    with _fetch_lock:
        if _fetch_state["running"]:
            return jsonify(_public_state()), 202

        _fetch_state.update(
            {
                "running": True,
                "started_at": _now_iso(),
                "finished_at": None,
                "matches": [],
                "count": 0,
                "saved_matches": 0,
                "saved_signals": 0,
                "error": None,
            }
        )

    thread = threading.Thread(target=_run_fetch_job, daemon=True)
    thread.start()
    return jsonify(_public_state()), 202


@upcoming_bp.route("/api/status")
def upcoming_api_status():
    return jsonify(_public_state())


@upcoming_bp.route("/api/list")
def upcoming_api_list():
    config = Config()
    db = Database(config.DB_PATH)
    db.init()
    matches = db.list_upcoming_matches(limit=500)
    saved_signals = sum(1 for match in matches if match.get("signal_direction"))
    return jsonify(
        {
            "matches": matches,
            "count": len(matches),
            "saved_matches": len(matches),
            "saved_signals": saved_signals,
            "running": False,
            "error": None,
        }
    )


@upcoming_bp.route("/api/clear", methods=["POST"])
def upcoming_api_clear():
    config = Config()
    db = Database(config.DB_PATH)
    db.init()
    summary = db.clear_upcoming_matches()
    return jsonify({"cleared": True, **summary})


@upcoming_bp.route("/api/matches/<path:match_id>/<action>", methods=["POST"])
def upcoming_api_match_action(match_id: str, action: str):
    match_key = str(match_id or "").strip()
    if not match_key:
        return jsonify({"error": "match_id is required"}), 400

    config = Config()
    db = Database(config.DB_PATH)
    db.init()

    if action == "delete":
        affected = db.delete_upcoming_match_data(match_key)
        return jsonify(
            {
                "match_id": match_key,
                "deleted": True,
                "affected": affected,
            }
        )

    current = db.get_upcoming_match_action_status(match_key)
    if action == "bet":
        new_val = not bool(current.get("bet_placed"))
        affected = db.set_upcoming_match_statuses(
            match_key,
            bet_placed=new_val,
            ignored=False if new_val else None,
            followed=False if new_val else None,
        )
        return jsonify(
            {
                "match_id": match_key,
                "bet_placed": int(new_val),
                "ignored": 0 if new_val else None,
                "followed": 0 if new_val else None,
                "affected": affected,
            }
        )

    if action == "ignore":
        new_val = not bool(current.get("ignored"))
        affected = db.set_upcoming_match_statuses(
            match_key,
            ignored=new_val,
            bet_placed=False if new_val else None,
            followed=False if new_val else None,
        )
        return jsonify(
            {
                "match_id": match_key,
                "ignored": int(new_val),
                "bet_placed": 0 if new_val else None,
                "followed": 0 if new_val else None,
                "affected": affected,
            }
        )

    if action == "follow":
        new_val = not bool(current.get("followed"))
        affected = db.set_upcoming_match_statuses(
            match_key,
            followed=new_val,
            bet_placed=False if new_val else None,
            ignored=False if new_val else None,
        )
        return jsonify(
            {
                "match_id": match_key,
                "followed": int(new_val),
                "bet_placed": 0 if new_val else None,
                "ignored": 0 if new_val else None,
                "affected": affected,
            }
        )

    return jsonify({"error": "unknown action"}), 404


def _run_fetch_job():
    config = Config()
    scraper = UpcomingScraper(
        aiscore_url=config.AISCORE_URL,
        page_timeout_ms=config.PAGE_TIMEOUT_MS,
        max_matches=None,
        days_ahead=config.UPCOMING_DAYS_AHEAD,
        timezone_id=config.AISCORE_TIMEZONE,
    )
    try:
        matches = asyncio.run(scraper.fetch())
        db = Database(config.DB_PATH)
        db.init()
        saved = db.save_upcoming_matches_and_signals(matches)
        matches = saved["matches"]
    except Exception as exc:
        log.exception("Upcoming fetch failed: %s", exc)
        with _fetch_lock:
            _fetch_state.update(
                {
                    "running": False,
                    "finished_at": _now_iso(),
                    "matches": [],
                    "count": 0,
                    "saved_matches": 0,
                    "saved_signals": 0,
                    "error": str(exc),
                }
            )
        return

    with _fetch_lock:
        _fetch_state.update(
            {
                "running": False,
                "finished_at": _now_iso(),
                "matches": matches,
                "count": len(matches),
                "saved_matches": saved["saved_matches"],
                "saved_signals": saved["saved_signals"],
                "error": None,
            }
        )


def _public_state():
    with _fetch_lock:
        return {
            "running": _fetch_state["running"],
            "started_at": _fetch_state["started_at"],
            "finished_at": _fetch_state["finished_at"],
            "matches": list(_fetch_state["matches"]),
            "count": _fetch_state["count"],
            "saved_matches": _fetch_state["saved_matches"],
            "saved_signals": _fetch_state["saved_signals"],
            "error": _fetch_state["error"],
        }


def _now_iso():
    return datetime.now(timezone.utc).isoformat()
