"""Read outcomes against immutable forecast-time totals, without re-running a model."""

import json
import math
from collections import Counter
from datetime import datetime, timedelta, timezone

from live_signals import direction_for_total, valid_total


def _timestamp(value):
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)


def frozen_context(row):
    try:
        result = json.loads(row.get("forecast_json") or "null")
    except (TypeError, ValueError):
        return {}
    return result if isinstance(result, dict) else {}


def forecast_outcome(row, *, as_of=None):
    """Only a verified automatic final can settle the saved line and direction."""
    as_of = as_of or datetime.now(timezone.utc)
    context = frozen_context(row)
    try:
        forecast = context["forecast"]
        line = valid_total(forecast["line"])
        center = float(forecast["predicted_total"])
        captured = _timestamp(context.get("market_captured_at"))
        recorded = _timestamp(row.get("recorded_at"))
        if (line is None or not math.isfinite(center) or center < 0
                or forecast["direction"] != direction_for_total(center, line)
                or captured is None or recorded is None
                or captured > as_of or recorded > as_of):
            return "invalid"
    except (KeyError, TypeError, ValueError):
        return "invalid"
    settled = _timestamp(row.get("settled_at"))
    if row.get("result_source") != "automatic_final_score" or settled is None or settled > as_of:
        return "pending"
    # SQLite CURRENT_TIMESTAMP has second precision, while capture has fractions.
    if settled + timedelta(seconds=1) <= captured or settled < recorded:
        return "invalid"
    total = row.get("final_total")
    if isinstance(total, bool) or not isinstance(total, (int, float)) or not math.isfinite(total) or total < 0:
        return "invalid"
    if forecast["direction"] == "EŞİT":
        return "no_direction"
    if total == line:
        return "push"
    return "win" if (total < line) == (forecast["direction"] == "ALT") else "loss"


def _outcome_summary(rows, outcome, as_of):
    counts = Counter(outcome(row, as_of=as_of) for row in rows)
    decided = counts["win"] + counts["loss"]
    return {"matches": len(rows), "wins": counts["win"], "losses": counts["loss"],
            "pending": counts["pending"], "pushes": counts["push"], "no_direction": counts["no_direction"],
            "invalid": counts["invalid"], "decided": decided,
            "success_rate": round(100 * counts["win"] / decided, 1) if decided else None}


def _forecast_direction(row):
    forecast = frozen_context(row).get("forecast")
    direction = forecast.get("direction") if isinstance(forecast, dict) else None
    return direction if direction in ("ALT", "ÜST", "EŞİT") else "unknown"


def tracking_summary(data, *, as_of=None):
    """Every match has one fixed first record, even if a later direction changes."""
    as_of = as_of or datetime.now(timezone.utc)
    first = data["first"]
    return {"sampling": "first_saved_forecast_per_match", "forecast_count": data["forecast_count"],
            **_outcome_summary(first, forecast_outcome, as_of),
            "by_direction": {
                direction: _outcome_summary([row for row in first if _forecast_direction(row) == direction],
                                            forecast_outcome, as_of)
                for direction in ("ALT", "ÜST", "EŞİT", "unknown")}}


def signal_outcome(row, *, as_of=None):
    """Read saved signal direction/line and an automatic final; write nothing."""
    as_of = as_of or datetime.now(timezone.utc)
    recorded = _timestamp(row.get("alerted_at"))
    line = valid_total(row.get("live"))
    if row.get("direction") not in ("ALT", "ÜST") or line is None or recorded is None or recorded > as_of:
        return "invalid"
    settled = _timestamp(row.get("settled_at"))
    if row.get("result_source") != "automatic_final_score" or settled is None or settled > as_of:
        return "pending"
    total = row.get("final_total")
    if (settled < recorded or isinstance(total, bool) or not isinstance(total, (int, float))
            or not math.isfinite(total) or total < 0):
        return "invalid"
    if total == line:
        return "push"
    return "win" if (total < line) == (row["direction"] == "ALT") else "loss"


def signal_tracking_summary(first, *, as_of=None):
    as_of = as_of or datetime.now(timezone.utc)
    return {"sampling": "first_saved_signal_per_match", "model_scope": "mixed_recorded_policies",
            **_outcome_summary(first, signal_outcome, as_of),
            "by_direction": {
                direction: _outcome_summary([row for row in first if row.get("direction") == direction],
                                            signal_outcome, as_of)
                for direction in ("ALT", "ÜST")}}
