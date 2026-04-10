"""
finished_matches.py — Finished matches blueprint.
Provides an isolated page and API for archived signal results.
"""

from flask import Blueprint, jsonify, render_template

from config import Config
from db import Database

config = Config()
db = Database(config.DB_PATH)
db.init()

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
