"""
run.py — Unified launcher that registers all blueprints
without modifying dashboard.py.

Usage:
    python run.py
"""

import os
from dashboard import app
from bankroll import bankroll_bp

app.register_blueprint(bankroll_bp)

if __name__ == "__main__":
    port = int(os.getenv("DASHBOARD_PORT", "5050"))
    app.run(host="0.0.0.0", port=port, debug=False)
