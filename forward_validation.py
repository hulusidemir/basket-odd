"""Freeze real published predictions and report them read-only, without fitting rules."""

import argparse
import base64
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from live_market import decode_history, verify_market
from match_state import game_clock


RULE_PARAMETERS = (
    "PRIOR_EQUIV_MINUTES", "MIN_VALID_FUTURE_PACES", "MIN_EDGE_POINTS", "MIN_EDGE_RATIO",
    "BLOWOUT_MARGIN", "BLOWOUT_EDGE_MULTIPLIER", "EXTREME_BLOWOUT_MARGIN",
    "EXTREME_BLOWOUT_EDGE_MULTIPLIER", "LARGE_REPRICE_RATIO", "LARGE_REPRICE_EDGE_MULTIPLIER",
    "MAX_FUTURE_BAND_WIDTH", "MIN_ELAPSED_MINUTES", "MIN_REMAINING_MINUTES",
    "ANCHOR_TOLERANCE_MIN_PCT", "ANCHOR_TOLERANCE_MAX_PCT", "MIN_QUARTER_ELAPSED_SEC",
    "UNDER_MAX_NEGATIVE_LINE_MOVE", "OVER_MIN_PACE_MARGIN_RATIO", "OVER_Q2_MIN_PACE_MARGIN_RATIO",
    "OVER_MIN_FAIR_EDGE_POINTS", "REPEAT_SIGNAL_QUALITY_PENALTY",
    "MAX_SIGNALS_PER_MATCH", "SAME_DIRECTION_MIN_LIVE_DELTA", "HEARTBEAT_SECONDS",
    "MAX_LIVE_PROVIDER_AGE_SECONDS", "MAX_LIVE_OBSERVATION_AGE_SECONDS",
)


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def _implementation_hash() -> str:
    root = Path(__file__).resolve().parent
    names = ("main.py", "live_signals.py", "pace_calculator.py", "signal_quality.py",
             "match_state.py", "live_market.py", "aiscore_scraper.py", "signal_repeat.py",
             "signal_lists.py", "forward_validation.py")
    return _digest({name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in names})


# Freeze at process import: reading edited files at alert time could mislabel running code.
IMPLEMENTATION_HASH = _implementation_hash()


def _timestamp(value) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def freeze_prediction(match, decision, config, snapshots) -> dict:
    """Only called for an actual publication; contains no credentials or final score."""
    policy = {"implementation_sha256": IMPLEMENTATION_HASH,
              "parameters": {name: getattr(config, name) for name in RULE_PARAMETERS},
              "engine": "future_pace_v5", "quality": "v1", "source": "bet365_history_v1",
              "publication": "verified_future_pace_v5"}
    clock = game_clock(match["status"], match["match_name"], match["tournament"])
    return {
        "schema_version": 1, "policy_id": _digest(policy), "policy": policy,
        "predicted_at": datetime.now(timezone.utc).isoformat(),
        "market_captured_at": (match.get("market_provenance") or {}).get("captured_at"),
        "clock": clock, "decision": asdict(decision),
        "snapshot_ids": [row["id"] for row in snapshots if row.get("id") is not None],
    }


def _verified_prediction(row, context) -> bool:
    """Validate the historical source at capture time, never against today's odds."""
    try:
        if context.get("schema_version") != 1 or context["policy_id"] != _digest(context["policy"]):
            return False
        if context["decision"]["direction"] != row["direction"]:
            return False
        if row.get("quality_version") != "v1" or row.get("quality_score") is None:
            return False
        publication = context["policy"].get("publication")
        if publication is None:
            # Earlier frozen policies genuinely required a minimum; do not rewrite history.
            if row["quality_score"] < context["policy"]["parameters"]["MIN_SIGNAL_QUALITY"]:
                return False
        elif publication != "verified_future_pace_v5":
            return False
        predicted = _timestamp(context["predicted_at"])
        alerted = _timestamp(row["alerted_at"])
        captured = _timestamp(context["market_captured_at"])
        if not predicted or not alerted or not captured or not captured <= predicted < alerted + timedelta(seconds=1):
            return False
        proof = json.loads(row.get("market_provenance_json") or "null")
        if not isinstance(proof, dict) or proof.get("verified") is not True or proof.get("version") != "bet365_history_v1":
            return False
        if _timestamp(proof["captured_at"]) != captured:
            return False
        raw = base64.b64decode(proof["raw_history_base64"], validate=True)
        if hashlib.sha256(raw).hexdigest() != proof["raw_history_sha256"]:
            return False
        verified, error = verify_market(
            proof["source"], decode_history(raw), row["match_id"], row["status"], row["score"],
            now=captured.timestamp(),
            max_age=context["policy"]["parameters"]["MAX_LIVE_PROVIDER_AGE_SECONDS"],
        )
        return (not error
                and all(verified[key] == row[column] == proof[key] for key, column in
                        (("live", "live"), ("opening", "opening"), ("prematch", "prematch")))
                and all(verified[key] == proof.get(key) for key in
                        ("match_id", "market", "bookmaker_id", "history_latest", "provider_updated_at")))
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def _summary(rows, as_of) -> dict:
    counts = Counter()
    for row in rows:
        settled = _timestamp(row.get("settled_at"))
        result = row.get("result")
        if not settled or settled > as_of or row.get("result_source") != "automatic_final_score":
            counts["pending"] += 1
            continue
        # Check persisted arithmetic; an inconsistent outcome is not a successful prediction.
        try:
            final = float(row["final_total"])
            line = float(row["live"])
            if not math.isfinite(final) or not math.isfinite(line):
                raise ValueError("invalid total")
            expected = ("İade" if abs(final - line) < 0.0001 else
                        "Başarılı" if ((final < line) == (row["direction"] == "ALT")) else "Başarısız")
        except (TypeError, ValueError, KeyError):
            counts["invalid_result"] += 1
            continue
        if result != expected:
            counts["invalid_result"] += 1
        else:
            counts[{"Başarılı": "wins", "Başarısız": "losses", "İade": "pushes"}[result]] += 1
    n = counts["wins"] + counts["losses"]
    interval = None
    if n:
        p = counts["wins"] / n
        z = 1.959963984540054
        denominator = 1 + z * z / n
        center = (p + z * z / (2 * n)) / denominator
        half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denominator
        interval = [round(center - half, 6), round(center + half, 6)]
    return {"matches": len(rows), **{key: counts[key] for key in
            ("wins", "losses", "pushes", "pending", "invalid_result")},
            "win_rate": round(counts["wins"] / n, 6) if n else None,
            "wilson_95": interval}


def report(db_path, *, as_of=None) -> dict:
    """First published signal per match/policy, selected BEFORE checking its outcome."""
    as_of = datetime.now(timezone.utc) if as_of is None else as_of
    as_of = _timestamp(as_of)
    if as_of is None:
        raise ValueError("invalid as_of timestamp")
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        connection.row_factory = sqlite3.Row
        rows = [dict(row) for row in connection.execute("SELECT * FROM alerts ORDER BY alerted_at, id")]
    first = {}
    excluded = Counter()
    for row in rows:
        alerted = _timestamp(row.get("alerted_at"))
        if not alerted or alerted > as_of:
            continue
        try:
            context = json.loads(row.get("prediction_context_json") or "null")
            if not isinstance(context, dict) or not context.get("policy_id"):
                excluded["legacy_without_frozen_policy"] += 1
                continue
            predicted = _timestamp(context.get("predicted_at"))
            if predicted and predicted > as_of:
                continue
            key = (context["policy_id"], row["match_id"])
            if key in first:
                excluded["repeat_prediction"] += 1
                continue
            first[key] = (row, context)
        except (TypeError, ValueError):
            excluded["invalid_prediction_context"] += 1
    cohorts = defaultdict(list)
    for (policy_id, _), (row, context) in first.items():
        if row["direction"] not in ("ALT", "ÜST") or not _verified_prediction(row, context):
            excluded["invalid_prediction_or_source"] += 1
            continue
        cohorts[policy_id].append(row)
    output = {}
    for policy_id, group in sorted(cohorts.items()):
        days = defaultdict(list)
        for row in group:
            days[_timestamp(row["alerted_at"]).date().isoformat()].append(row)
        output[policy_id] = {
            "first_prediction_utc": group[0]["alerted_at"], "overall": _summary(group, as_of),
            "by_direction": {direction: _summary([r for r in group if r["direction"] == direction], as_of)
                             for direction in ("ALT", "ÜST")},
            "by_day_utc": {day: _summary(day_rows, as_of) for day, day_rows in sorted(days.items())},
        }
    return {"as_of_utc": as_of.isoformat(), "sampling": "first_published_signal_per_match_per_policy",
            "excluded": dict(excluded), "cohorts": output,
            "limitations": ["Win rate is not profitability or a calibrated probability.",
                            "Intervals do not account for league/day correlation.",
                            "Final-score evaluation does not verify bookmaker settlement terms."]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--as-of", help="UTC/ISO timestamp for the reporting cutoff")
    args = parser.parse_args()
    print(json.dumps(report(args.db, as_of=args.as_of), indent=2, ensure_ascii=False))
