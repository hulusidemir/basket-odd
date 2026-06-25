"""
run.py — Unified launcher that registers all blueprints
without modifying dashboard.py.

Usage:
    python run.py
"""

import logging
import os
from dashboard import app
from bankroll import bankroll_bp
from balance_tracker.app import balance_tracker_bp
import scheduled_tasks

app.register_blueprint(bankroll_bp)
app.register_blueprint(balance_tracker_bp)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    scheduled_tasks.start()
    port = int(os.getenv("DASHBOARD_PORT", "5151"))
    app.run(host="0.0.0.0", port=port, debug=False)
