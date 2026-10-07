"""Read-only signal-count replay on saved observations; no Telegram or DB writes."""

import argparse
import json
import sqlite3
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from config import Config
from forward_validation import _timestamp
from live_market import verify_current_market, verify_market
from live_signals import evaluate_live_signal
from main import _next_signal_count, _snapshot_has_verified_market
from match_state import confirmed_12_minute_quarters
from signal_lists import build_signal_blacklist_matches, build_signal_list_profile


class ReplayAlerts:
    def __init__(self):
        self.rows = {}

    def count_match_alerts(self, match_id):
        return len(self.rows.get(match_id, []))

    def was_alerted_in_period(self, match_id, period):
        return any(row["period"] == period for row in self.rows.get(match_id, []))

    def latest_match_alert_in_direction(self, match_id, direction):
        return next((row for row in reversed(self.rows.get(match_id, []))
                     if row["direction"] == direction), None)

    def add(self, match, decision):
        self.rows.setdefault(match["match_id"], []).append({
            "period": decision.period, "direction": decision.direction,
            "live": match["inplay_total"]})


def replay_day(path, day, thresholds=(0, .6)):
    selected_day = date.fromisoformat(day)
    start = datetime.combine(selected_day, datetime.min.time(), ZoneInfo("Europe/Istanbul"))
    end = start + timedelta(days=1)
    config = Config()
    states = {threshold: ReplayAlerts() for threshold in thresholds}
    counts = {threshold: Counter() for threshold in thresholds}
    history, excluded, observations = {}, Counter(), 0
    with sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        profile = build_signal_list_profile([
            dict(row) for row in connection.execute("SELECT * FROM signal_lists")])
        rows = connection.execute("SELECT * FROM match_live_snapshots ORDER BY recorded_at,id")
        for raw in rows:
            row = dict(raw)
            previous = history.setdefault(row["match_id"], [])
            try:
                context = json.loads(row["forecast_json"] or "{}")
                forecast = context.get("forecast", {})
                captured = _timestamp(context.get("market_captured_at"))
                if (captured is None or not start <= captured < end
                        or forecast.get("base_engine", forecast.get("engine")) != "future_pace_v9"):
                    continue
                observations += 1
                proof = json.loads(row["market_provenance_json"])
                match = {"match_id": row["match_id"], "match_name": context["match_name"],
                    "tournament": context["tournament"], "url": context.get("url", ""),
                    "score": context["score"], "status": context["status"],
                    "opening_total": proof["opening"], "prematch_total": proof.get("prematch"),
                    "inplay_total": forecast["line"], "market_provenance": proof,
                    "market_captured_at": context["market_captured_at"]}
                if proof["version"] == "aiscore_live_v2":
                    verified, reason = verify_current_market(proof["source"], row["match_id"],
                        match["status"], match["score"], now=captured.timestamp())
                else:
                    verified, reason = verify_market(proof["source"], [proof["history_latest"]],
                        row["match_id"], match["status"], match["score"], now=captured.timestamp())
                if (reason or verified["live"] != forecast["line"]
                        or verified["opening"] != match["opening_total"]
                        or verified.get("prematch") != match["prematch_total"]
                        or verified["bookmaker_id"] != proof["bookmaker_id"]):
                    excluded["invalid_source"] += 1
                    continue
                if (build_signal_blacklist_matches(match, profile)
                        or any(term in f"{match['match_name']} {match['tournament']} {match['url']}".lower()
                               for term in config.BLACKLIST)):
                    excluded["blacklisted"] += 1
                    continue
                duration = forecast["elapsed_minutes"] + forecast["remaining_minutes"]
                with confirmed_12_minute_quarters(duration == 48):
                    decision = evaluate_live_signal(match, previous, config)
                if decision.skip_reason or decision.direction not in ("ALT", "ÜST"):
                    excluded[decision.skip_reason or "no_direction"] += 1
                    continue
                probability = (decision.win_probability or {}).get("probability")
                for threshold, state in states.items():
                    if threshold and (probability is None or probability < threshold):
                        continue
                    if _next_signal_count(match, decision, state, config) is not None:
                        state.add(match, decision)
                        counts[threshold][decision.direction] += 1
            except (KeyError, ValueError, TypeError):
                excluded["invalid_saved_forecast"] += 1
            finally:
                if _snapshot_has_verified_market(row):
                    previous.append(row)
    return {"day": day, "saved_observations": observations, "excluded_observations": dict(excluded),
        "thresholds": [{"minimum_probability": threshold, "signals": sum(counts[threshold].values()),
            "matches": len(states[threshold].rows), "directions": dict(counts[threshold])}
            for threshold in thresholds],
        "limitations": ["Saved observations and current lists/rules/model; not a prospective full-day count.",
            "New visible-DOM checks cannot be replayed on old source proofs.",
            "Historical manual archive times and blacklist changes are not reconstructed."]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()
    print(json.dumps(replay_day(args.db, args.day), ensure_ascii=False, indent=2))
