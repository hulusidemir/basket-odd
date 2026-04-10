"""
finished_matches.py — Finished matches blueprint.
Provides an isolated page and API for archived signal results.
"""

import asyncio
import threading

from flask import Blueprint, jsonify, render_template

from config import Config
from db import Database
from finished_match_service import run_finished_match_cycle

config = Config()
db = Database(config.DB_PATH)
db.init()
manual_check_lock = threading.Lock()

finished_matches_bp = Blueprint(
    "finished_matches",
    __name__,
    template_folder="templates",
)


@finished_matches_bp.route("/finished-matches")
def finished_matches_page():
    return render_template("finished_matches.html")


@finished_matches_bp.route("/api/finished-matches")
def api_finished_matches():
    return jsonify(db.recent_finished_matches(limit=1000))


@finished_matches_bp.route("/api/finished-matches/check-now", methods=["POST"])
def api_finished_matches_check_now():
    if not manual_check_lock.acquire(blocking=False):
        return jsonify({"error": "running"}), 409

    try:
        summary = asyncio.run(run_finished_match_cycle(db, config))
        return jsonify(summary)
    finally:
        manual_check_lock.release()


@finished_matches_bp.route("/api/finished-matches/<int:finished_match_id>", methods=["DELETE"])
def api_delete_finished_match(finished_match_id: int):
    deleted = db.delete_finished_match(finished_match_id)
    if not deleted:
        return jsonify({"error": "not found"}), 404
    return jsonify({"id": finished_match_id, "deleted": True})


@finished_matches_bp.route("/api/finished-matches/clear", methods=["POST"])
def api_clear_finished_matches():
    deleted_count = db.clear_finished_matches()
    return jsonify({"cleared": True, "deleted_count": deleted_count})
