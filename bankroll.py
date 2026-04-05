"""
bankroll.py — Bankroll management blueprint.
Cumulative betting tracker with group-based money splitting.

Registers a Flask Blueprint; does NOT modify any existing routes.
Wire it up via:
    from bankroll import bankroll_bp
    app.register_blueprint(bankroll_bp)
"""

from flask import Blueprint, render_template

bankroll_bp = Blueprint("bankroll", __name__, template_folder="templates")


@bankroll_bp.route("/bankroll")
def bankroll_page():
    """Render the bankroll management page."""
    return render_template("bankroll.html")
