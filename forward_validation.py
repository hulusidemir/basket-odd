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

from live_market import decode_history, verify_current_market, verify_market
from live_signals import ENGINE_VERSION, direction_for_total
from match_state import game_clock, parse_score
from pace_calculator import get_future_pace_windows
from over_calibration import MODEL_SHA256
from win_probability import MODEL_SHA256 as PROBABILITY_MODEL_SHA256, frozen_probability_direction


RULE_PARAMETERS = (
    "MIN_SIGNAL_WIN_PROBABILITY",
    "OVER_CONTINUATION_ENABLED",
    "OVER_CALIBRATION_ENABLED",
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
             "signal_lists.py", "forward_validation.py", "over_calibration.py",
             "models/over_calibration_v1.json", "win_probability.py", "models/win_probability_v1.json")
    return _digest({name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in names})


# Freeze at process import: reading edited files at alert time could mislabel running code.
IMPLEMENTATION_HASH = _implementation_hash()


def freeze_forecast(match, forecast, config) -> dict | None:
    """Persist the forecast at observation time, including those without an alert."""
    if forecast is None:
        return None
    policy = {
        "implementation_sha256": IMPLEMENTATION_HASH, "engine": ENGINE_VERSION,
        "prior_equivalent_minutes": config.PRIOR_EQUIV_MINUTES,
        "over_continuation_enabled": config.OVER_CONTINUATION_ENABLED,
        "over_calibration_enabled": config.OVER_CALIBRATION_ENABLED,
        "over_calibration_model_sha256": MODEL_SHA256,
        "win_probability_model_sha256": PROBABILITY_MODEL_SHA256,
        "probability_publication_filter_enabled": False,
        "source": (match.get("market_provenance") or {}).get("version"),
    }
    return {
        "schema_version": 1, "policy_id": _digest(policy), "policy": policy,
        "match_name": match["match_name"], "tournament": match["tournament"],
        "url": match.get("url", ""),
        "bookmaker": (match.get("market_provenance") or {}).get("bookmaker"),
        "bookmaker_id": (match.get("market_provenance") or {}).get("bookmaker_id"),
        "status": match["status"], "score": match["score"],
        "market_captured_at": (match.get("market_provenance") or {}).get("captured_at"),
        "forecast": forecast,
    }


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
              "engine": ENGINE_VERSION, "quality": "v1",
              "source": (match.get("market_provenance") or {}).get("version"),
              "win_probability_model_sha256": PROBABILITY_MODEL_SHA256,
              "probability_publication_filter_enabled": False,
              "publication": "verified_" + ENGINE_VERSION}
    clock = game_clock(match["status"], match["match_name"], match["tournament"])
    home, away = parse_score(match.get("score", ""))
    windows = []
    if clock.get("period") and clock.get("remaining_min") is not None and home is not None:
        elapsed = ((clock["period"] - 1) * clock["quarter_length"]
                   + clock["quarter_length"] - clock["remaining_min"])
        regulation = clock["quarter_length"] * clock["period_count"]
        windows = get_future_pace_windows(snapshots, {
            "elapsed_game_seconds": round(elapsed * 60), "total_score": home + away,
            "period": clock["period"], "observed_at": match.get("market_captured_at"),
        }, decision.reference_total / regulation, config)
    return {
        "schema_version": 1, "policy_id": _digest(policy), "policy": policy,
        "predicted_at": datetime.now(timezone.utc).isoformat(),
        "market_captured_at": (match.get("market_provenance") or {}).get("captured_at"),
        "clock": clock, "decision": asdict(decision),
        "snapshot_ids": [row["id"] for row in snapshots if row.get("id") is not None],
        "pace_windows": windows,
        "pace_window_semantics": "distinct_source_intervals_with_overlap",
        "pregame_prior_ppm": decision.reference_total / (
            clock["quarter_length"] * clock["period_count"]),
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
        elif publication not in {"verified_future_pace_v5", "verified_future_pace_v6", "verified_future_pace_v7",
                                 "verified_future_pace_v8", "verified_future_pace_v9", "verified_future_pace_v10"}:
            return False
        predicted = _timestamp(context["predicted_at"])
        alerted = _timestamp(row["alerted_at"])
        captured = _timestamp(context["market_captured_at"])
        if not predicted or not alerted or not captured or not captured <= predicted < alerted + timedelta(seconds=1):
            return False
        proof = json.loads(row.get("market_provenance_json") or "null")
        if (not isinstance(proof, dict) or proof.get("verified") is not True
                or proof.get("version") not in {"bet365_history_v1", "aiscore_history_v2", "aiscore_live_v2"}):
            return False
        if context["policy"].get("source") != proof["version"]:
            return False
        bookmaker_id = proof.get("bookmaker_id")
        if (type(bookmaker_id) is not int or bookmaker_id <= 0
                or (proof["version"] == "bet365_history_v1" and bookmaker_id != 2)):
            return False
        if _timestamp(proof["captured_at"]) != captured:
            return False
        if proof["version"] == "aiscore_live_v2":
            if (predicted - captured).total_seconds() > context["policy"]["parameters"]["MAX_LIVE_OBSERVATION_AGE_SECONDS"]:
                return False
            verified, error = verify_current_market(
                proof["source"], row["match_id"], row["status"], row["score"], now=captured.timestamp(),
            )
            return (not error and verified == proof
                    and all(verified[key] == row[column] for key, column in
                            (("live", "live"), ("opening", "opening"), ("prematch", "prematch"))))
        raw = base64.b64decode(proof["raw_history_base64"], validate=True)
        if hashlib.sha256(raw).hexdigest() != proof["raw_history_sha256"]:
            return False
        verified, error = verify_market(
            proof["source"], decode_history(raw, bookmaker_id), row["match_id"], row["status"], row["score"],
            now=captured.timestamp(),
            max_age=context["policy"]["parameters"]["MAX_LIVE_PROVIDER_AGE_SECONDS"],
        )
        return (not error
                and all(verified[key] == row[column] == proof[key] for key, column in
                        (("live", "live"), ("opening", "opening"), ("prematch", "prematch")))
                and all(verified[key] == proof.get(key) for key in
                        ("version", "match_id", "market", "bookmaker_id", "history_latest", "provider_updated_at")))
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def _summary(rows, as_of) -> dict:
    counts = Counter()
    errors = []
    baseline_errors = []
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
            try:
                context = json.loads(row.get("prediction_context_json") or "null")
                center = float(context["decision"]["sustainable_projection_center"])
                if math.isfinite(center):
                    errors.append((center - final, line - final))
                    clock = context["clock"]
                    duration = clock["quarter_length"] * clock["period_count"]
                    elapsed = ((clock["period"] - 1) * clock["quarter_length"]
                               + clock["quarter_length"] - clock["remaining_min"])
                    home, away = parse_score(row.get("score", ""))
                    prior = float(context["decision"]["reference_total"]) / duration
                    baseline = home + away + (duration - elapsed) * prior
                    if math.isfinite(baseline) and 0 <= elapsed <= duration:
                        baseline_errors.append(baseline - final)
            except (KeyError, TypeError, ValueError, ZeroDivisionError):
                pass
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
            "wilson_95": interval,
            "forecast_error": {
                "samples": len(errors),
                "model_mae": round(sum(abs(a) for a, _ in errors) / len(errors), 3) if errors else None,
                "market_mae": round(sum(abs(b) for _, b in errors) / len(errors), 3) if errors else None,
                "model_bias": round(sum(a for a, _ in errors) / len(errors), 3) if errors else None,
                "pregame_baseline_samples": len(baseline_errors),
                "pregame_baseline_mae": round(sum(abs(a) for a in baseline_errors) / len(baseline_errors), 3)
                                        if baseline_errors else None,
                "model_closer": sum(abs(a) < abs(b) for a, b in errors),
                "market_closer": sum(abs(b) < abs(a) for a, b in errors),
                "equal_error": sum(abs(a) == abs(b) for a, b in errors),
            }}


def _m2_availability(rows):
    states, failures = Counter(), Counter()
    for row in rows:
        try:
            analysis = json.loads(row.get("m2_analysis_json") or "null")
        except (TypeError, ValueError):
            states["invalid_json"] += 1
            continue
        if not isinstance(analysis, dict):
            states["no_record"] += 1
            continue
        state = analysis.get("state")
        states[state if state in {"ready", "pending", "disabled", "unavailable"} else "unknown"] += 1
        if state == "unavailable":
            code = analysis.get("reason_code") or analysis.get("source_error")
            failures[str(code or "unspecified")] += 1
    return {"states": dict(states), "failure_reasons": dict(failures)}


def _forecast_report(rows, finals, as_of):
    """Select the first frozen observation before looking at its final score."""
    first, excluded = {}, Counter()
    for row in rows:
        recorded = _timestamp(row.get('recorded_at'))
        if not recorded or recorded > as_of:
            continue
        try:
            context = json.loads(row.get('forecast_json') or 'null')
            if not isinstance(context, dict):
                continue
            key = (context['policy_id'], row['match_id'])
            if key in first:
                continue
            first[key] = (row, context)
        except (KeyError, TypeError, ValueError):
            excluded['invalid_context'] += 1
    groups = defaultdict(list)
    for (policy_id, match_id), (row, context) in first.items():
        try:
            forecast = context['forecast']
            proof = json.loads(row['market_provenance_json'])
            captured = _timestamp(context['market_captured_at'])
            if not captured or captured > as_of:
                continue
            if (context['policy_id'] != _digest(context['policy'])
                    or context['policy']['source'] != proof['version']
                    or forecast['engine'] != context['policy']['engine']
                    or forecast['line'] != row['live_total']
                    or context['score'] != f"{row['home_score']} - {row['away_score']}"):
                raise ValueError('inconsistent forecast')
            if proof['version'] == 'aiscore_live_v2':
                verified, reason = verify_current_market(proof['source'], match_id, context['status'], context['score'], now=captured.timestamp())
            else:
                verified, reason = verify_market(proof['source'], [proof['history_latest']], match_id, context['status'], context['score'], now=captured.timestamp())
            if (reason or verified['version'] != proof['version'] or verified['live'] != forecast['line']
                    or verified['bookmaker_id'] != context['bookmaker_id']):
                raise ValueError('inconsistent source')
            center = float(forecast['predicted_total'])
            if not math.isfinite(center):
                raise ValueError('invalid forecast')
            expected_direction = (frozen_probability_direction(forecast["win_probability"])
                if forecast["engine"] == "future_pace_v10" else direction_for_total(center, forecast["line"]))
            if forecast['direction'] != expected_direction:
                raise ValueError('inconsistent direction')
            final = finals.get(match_id)
            groups[policy_id].append((forecast, final))
        except (KeyError, TypeError, ValueError):
            excluded['invalid_first_forecast'] += 1
    output = {}
    for policy_id, group in groups.items():
        counts, errors = Counter(), []
        for forecast, final in group:
            settled = _timestamp(final.get('settled_at')) if final else None
            if not final or not settled or settled > as_of or final['result_source'] != 'automatic_final_score':
                counts['pending'] += 1
                continue
            total = final['final_total']
            line = forecast['line']
            if forecast['direction'] == 'EŞİT':
                counts['no_direction'] += 1
            elif total == line:
                counts['pushes'] += 1
            else:
                counts['wins' if (total < line) == (forecast['direction'] == 'ALT') else 'losses'] += 1
            errors.append((abs(forecast['predicted_total'] - total), abs(line - total)))
        output[policy_id] = {'matches': len(group),
            **{key: counts[key] for key in ('pending', 'wins', 'losses', 'pushes', 'no_direction')},
            'forecast_error': {'samples': len(errors),
                'model_mae': sum(pair[0] for pair in errors) / len(errors) if errors else None,
                'market_mae': sum(pair[1] for pair in errors) / len(errors) if errors else None}}
    return {'sampling': 'first_recorded_forecast_per_match_per_policy', 'cohorts': output, 'excluded': dict(excluded)}


def report(db_path, *, as_of=None) -> dict:
    """First published signal per match/policy, selected BEFORE checking its outcome."""
    as_of = datetime.now(timezone.utc) if as_of is None else as_of
    as_of = _timestamp(as_of)
    if as_of is None:
        raise ValueError("invalid as_of timestamp")
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.row_factory = sqlite3.Row
        rows = [dict(row) for row in connection.execute("SELECT * FROM alerts ORDER BY alerted_at, id")]
        snapshot_columns = {row['name'] for row in connection.execute('PRAGMA table_info(match_live_snapshots)')}
        forecasts = ([dict(row) for row in connection.execute('SELECT * FROM match_live_snapshots WHERE forecast_json IS NOT NULL ORDER BY recorded_at,id')]
                     if 'forecast_json' in snapshot_columns else [])
        finals = ({row['match_id']: dict(row) for row in connection.execute('SELECT * FROM forecast_match_results')}
                  if connection.execute("SELECT 1 FROM sqlite_master WHERE name='forecast_match_results' AND type='table'").fetchone() else {})
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
            "delivery_status": dict(Counter(r.get("telegram_status") or "unknown" for r in group)),
            "delivered": _summary([r for r in group if r.get("telegram_status") == "sent"], as_of),
            "m2_availability": _m2_availability(group),
        }
    return {"as_of_utc": as_of.isoformat(), "sampling": "first_recorded_signal_per_match_per_policy",
            "excluded": dict(excluded), "cohorts": output,
            "all_forecasts": _forecast_report(forecasts, finals, as_of),
            "limitations": ["Win rate is not profitability or a calibrated probability.",
                            "Intervals do not account for league/day correlation.",
                            "Recorded, delivered and actually placed bets are distinct; entry prices are not recorded.",
                            "Final-score evaluation does not verify bookmaker settlement terms."]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--as-of", help="UTC/ISO timestamp for the reporting cutoff")
    args = parser.parse_args()
    print(json.dumps(report(args.db, as_of=args.as_of), indent=2, ensure_ascii=False))
