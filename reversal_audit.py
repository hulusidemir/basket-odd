"""Read-only, chronological tests of future scoring-rate changes.

Legacy observations are research data, not retroactively verified forecasts.
The CLI emits aggregates only and never changes alerts, outcomes or services.
"""

import argparse
import json
import math
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from live_signals import valid_total
from forward_validation import _verified_prediction
from match_state import confirmed_12_minute_quarters, game_clock, parse_score
from reversal_features import collect_reversal_features


HORIZON_SECONDS = 300
ENDPOINT_TOLERANCE_SECONDS = 30
FEATURE_NAMES = (
    "current_deviation", "recent_deviation", "recent_missing", "trend",
    "trend_missing", "elapsed_fraction", "score_margin", "quarter_fraction",
)


def timestamp(value):
    try:
        value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def object_json(value):
    try:
        result = json.loads(value or "{}")
    except (ValueError, TypeError):
        return {}
    return result if isinstance(result, dict) else {}


def first_signals(alerts):
    """Choose before checking history/outcome; never substitute a later winner."""
    selected = {}
    for row in sorted(alerts, key=lambda r: (r["alerted_at"], r["id"])):
        selected.setdefault(row["match_id"], row)
    return list(selected.values())


def valid_snapshot_clock(row, state):
    period = row.get("period")
    if not isinstance(period, int) or not 1 <= period <= state["period_count"]:
        return False
    # Earlier schema did not save clock text. Its period, elapsed and remaining
    # still delimit a valid regulation state; expose this weaker provenance.
    if not row.get("game_clock"):
        return ((period - 1) * state["quarter_length"] * 60 <= row["elapsed_game_seconds"]
                <= period * state["quarter_length"] * 60
                and abs(row["elapsed_game_seconds"] / 60 + row["remaining_minutes"] - state["duration"]) <= .02)
    parsed = re.fullmatch(r"(\d{1,2}):([0-5]\d)", row["game_clock"])
    if not parsed:
        return False
    seconds = int(parsed[1]) * 60 + int(parsed[2])
    expected = period * state["quarter_length"] * 60 - seconds
    return (seconds <= state["quarter_length"] * 60
            and abs(expected - row["elapsed_game_seconds"]) <= 1)


def observation(alert, rows):
    """Reconstruct only the prefix that existed at the original decision."""
    as_of = timestamp(alert["alerted_at"])
    prefix = [r for r in rows if timestamp(r["recorded_at"]) is not None
              and timestamp(r["recorded_at"]) <= as_of] if as_of else []
    if not prefix:
        return None, "missing_prefix"
    clock_hint = object_json(alert.get("prediction_context_json")).get("clock", {})
    frozen_features = object_json(alert.get("reversal_features_json"))
    duration_hint = frozen_features.get("regulation_minutes")
    override = duration_hint == 48 or clock_hint.get("quarter_length") == 12
    # A prefix observation above ten minutes also proves a 12-minute quarter.
    override = override or any(r.get("period") in (1, 2, 3, 4)
                               and str(r.get("game_clock", "")).split(":")[0] in ("11", "12")
                               for r in prefix)
    with confirmed_12_minute_quarters(override):
        clock = game_clock(alert["status"], alert.get("match_name", ""), alert.get("tournament", ""))
        if clock["period"] is None or clock["remaining_min"] is None:
            return None, "missing_clock"
        duration = clock["quarter_length"] * clock["period_count"]
        elapsed = (clock["period"] * clock["quarter_length"] - clock["remaining_min"])
        home, away = parse_score(alert["score"])
        if home is None or away is None or elapsed <= 0 or duration - elapsed < 5.5:
            return None, "invalid_or_late_state"
        score = home + away
        current = prefix[-1]
        if (abs(current["elapsed_game_seconds"] - round(elapsed * 60)) > 1
                or current["total_score"] != score
                or abs(current["remaining_minutes"] + elapsed - duration) > .02
                or (as_of - timestamp(current["recorded_at"])).total_seconds() > 120):
            return None, "prefix_state_mismatch"
        if not valid_snapshot_clock(current, {**clock, "duration": duration}):
            return None, "prefix_clock_mismatch"
        reference = valid_total(alert.get("prematch")) or valid_total(alert.get("opening"))
        line = valid_total(alert.get("live"))
        if reference is None or line is None:
            return None, "missing_line"
        match = {
            "status": alert["status"], "score": alert["score"],
            "match_name": alert.get("match_name", ""), "tournament": alert.get("tournament", ""),
            "prematch_total": alert.get("prematch"), "opening_total": alert.get("opening"),
            "inplay_total": alert.get("live"),
        }
        features = collect_reversal_features(match, prefix, SimpleNamespace(sustainable_projection_center=None), as_of=as_of)
    return {
        "match_id": alert["match_id"], "at": as_of, "elapsed": elapsed,
        "duration": duration, "remaining": duration - elapsed, "score": score,
        "line": line, "prior": reference / duration, "current": score / elapsed,
        "recent": features["recent_ppm"], "trend": features["pace_delta"],
        "margin": abs(home - away) / max(score, 1),
        "quarter_fraction": 1 - clock["remaining_min"] / clock["quarter_length"],
        "period": clock["period"],
        "period_count": clock["period_count"], "quarter_length": clock["quarter_length"],
        "legacy_clock_missing": not bool(current.get("game_clock")),
        "source_verified_at_recording": _verified_prediction(alert, object_json(alert.get("prediction_context_json"))),
    }, None


def future_endpoint(state, rows, *, remaining_game=False):
    """Future scores label the target only, and cannot enter the predictor."""
    candidates = []
    for row in rows:
        recorded = timestamp(row["recorded_at"])
        if recorded is None or not state["at"] < recorded <= state["at"] + timedelta(minutes=90):
            continue
        elapsed = row["elapsed_game_seconds"] / 60
        if (abs(elapsed + row["remaining_minutes"] - state["duration"]) > .02
                or not valid_snapshot_clock(row, state)
                or not state["elapsed"] < elapsed <= state["duration"]
                or row["total_score"] < state["score"]):
            continue
        if remaining_game:
            if row["remaining_minutes"] > 1:
                continue
            distance = row["remaining_minutes"] * 60
        else:
            distance = abs(row["elapsed_game_seconds"] - state["elapsed"] * 60 - HORIZON_SECONDS)
            if distance > ENDPOINT_TOLERANCE_SECONDS:
                continue
        candidates.append((distance, recorded, row))
    if not candidates:
        return None
    _, recorded, endpoint = min(candidates, key=lambda item: (item[0], item[1]))
    # Reject clock/score rollbacks rather than using corrected future scores
    # against an uncorrected starting state.
    last_second, last_score = round(state["elapsed"] * 60), state["score"]
    for row in rows:
        at = timestamp(row["recorded_at"])
        if at is None or not state["at"] < at <= recorded:
            continue
        if row["elapsed_game_seconds"] < last_second or row["total_score"] < last_score:
            return None
        last_second, last_score = row["elapsed_game_seconds"], row["total_score"]
    minutes = (endpoint["elapsed_game_seconds"] / 60 - state["elapsed"])
    return {"future": (endpoint["total_score"] - state["score"]) / minutes,
            "target_minutes": minutes, "label_at": recorded}


def vector(state, with_market=False):
    prior = state["prior"]
    values = [state["current"] / prior - 1,
              state["recent"] / prior - 1 if state["recent"] is not None else 0,
              float(state["recent"] is None),
              state["trend"] / prior if state["trend"] is not None else 0,
              float(state["trend"] is None), state["elapsed"] / state["duration"],
              state["margin"], state["quarter_fraction"]]
    if with_market:
        values.append((state["line"] - state["score"]) / state["remaining"] / prior - 1)
    return values


def solve(matrix, rhs):
    """Small, pivoted linear solve; no optional scientific dependency."""
    augmented = [list(row) + [value] for row, value in zip(matrix, rhs)]
    for column in range(len(rhs)):
        pivot = max(range(column, len(rhs)), key=lambda i: abs(augmented[i][column]))
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        scale = augmented[column][column]
        if abs(scale) < 1e-12:
            raise ValueError("singular fit")
        augmented[column] = [value / scale for value in augmented[column]]
        for index in range(len(rhs)):
            if index == column:
                continue
            weight = augmented[index][column]
            augmented[index] = [a - weight * b for a, b in zip(augmented[index], augmented[column])]
    return [row[-1] for row in augmented]


def fit(rows, penalty, with_market=False):
    xs = [vector(row, with_market) for row in rows]
    count, width = len(xs), len(xs[0])
    means = [sum(x[i] for x in xs) / count for i in range(width)]
    scales = [max(math.sqrt(sum((x[i] - means[i]) ** 2 for x in xs) / count), 1e-6) for i in range(width)]
    design = [[1] + [(x[i] - means[i]) / scales[i] for i in range(width)] for x in xs]
    ys = [row["future"] / row["prior"] - 1 for row in rows]
    gram = [[sum(x[i] * x[j] for x in design) + (penalty if i == j and i > 0 else 0)
             for j in range(width + 1)] for i in range(width + 1)]
    rhs = [sum(x[i] * y for x, y in zip(design, ys)) for i in range(width + 1)]
    return {"means": means, "scales": scales, "weights": solve(gram, rhs), "with_market": with_market}


def predict(model, row):
    values = vector(row, model["with_market"])
    z = [1] + [(x - mean) / scale for x, mean, scale in zip(values, model["means"], model["scales"])]
    return max(0, row["prior"] * (1 + sum(a * b for a, b in zip(z, model["weights"]))))


def split(rows, fraction):
    """Calendar-day boundary, one observation per match, no same-day leakage."""
    ordered = sorted(rows, key=lambda row: (row["at"], row["match_id"]))
    cutoff = ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]["at"].replace(hour=0, minute=0, second=0, microsecond=0)
    # A training label must have been observed before the test starts.
    return ([r for r in ordered if r["at"] < cutoff and r["label_at"] < cutoff],
            [r for r in ordered if r["at"] >= cutoff], cutoff)


def sign(value):
    return 1 if value > 1e-9 else -1 if value < -1e-9 else 0


def wilson(wins, count):
    if not count:
        return None
    z = 1.96
    p = wins / count
    denominator = 1 + z * z / count
    center = (p + z * z / (2 * count)) / denominator
    radius = z * math.sqrt(p * (1 - p) / count + z * z / (4 * count * count)) / denominator
    return [round(max(0, center - radius), 4), round(min(1, center + radius), 4)]


def metrics(rows, predictions):
    errors = [abs(pred - row["future"]) for row, pred in zip(rows, predictions)]
    actual = [sign(row["future"] - row["current"]) for row in rows]
    guessed = [sign(pred - row["current"]) for row, pred in zip(rows, predictions)]
    correct = sum(a == p for a, p in zip(actual, guessed))
    result = {"matches": len(rows), "rate_mae": round(sum(errors) / len(errors), 4),
            "speed_direction_correct": correct, "speed_direction_accuracy": round(correct / len(rows), 4),
            "accuracy_95_interval": wilson(correct, len(rows)),
            "predicted_acceleration": guessed.count(1), "predicted_deceleration": guessed.count(-1),
            "predicted_unchanged": guessed.count(0)}
    def meaningful_direction(rate, current):
        return sign(rate-current) if abs(rate-current) >= .10*current else 0
    actual_material = [meaningful_direction(r["future"],r["current"]) for r in rows]
    predicted_material = [meaningful_direction(p,r["current"]) for r,p in zip(rows,predictions)]
    material_correct = sum(a == p for a,p in zip(actual_material,predicted_material))
    result.update(material_speed_change_threshold_pct=10, material_speed_direction_correct=material_correct,
                  material_speed_direction_accuracy=round(material_correct/len(rows),4),
                  actual_materially_unchanged=actual_material.count(0),
                  predicted_materially_unchanged=predicted_material.count(0))
    recent = [(r, p) for r, p in zip(rows, predictions) if r["recent"] is not None]
    if recent:
        recent_correct = sum(sign(r["future"]-r["recent"]) == sign(p-r["recent"]) for r, p in recent)
        result.update(recent_speed_matches=len(recent), recent_speed_direction_correct=recent_correct,
                      recent_speed_direction_accuracy=round(recent_correct/len(recent),4))
    if rows[0].get("final_total") is not None:
        totals = [row["score"] + row["remaining"] * pred for row, pred in zip(rows, predictions)]
        actual = [sign(row["final_total"] - row["line"]) for row in rows]
        guesses = [sign(total - row["line"]) for row, total in zip(rows, totals)]
        wins = sum(a == p and a != 0 for a, p in zip(actual, guesses))
        pushes = actual.count(0)
        no_direction = sum(p == 0 and a != 0 for a, p in zip(actual, guesses))
        result.update(final_total_mae=round(sum(abs(total - row["final_total"]) for row, total in zip(rows, totals)) / len(rows), 4),
                      bet_direction_wins=wins, bet_direction_losses=len(rows)-wins-pushes-no_direction,
                      bet_pushes=pushes, bet_no_direction=no_direction,
                      market_final_total_mae=round(sum(abs(row["line"]-row["final_total"]) for row in rows)/len(rows),4))
    return result


def pattern(rows, subset):
    selected = [r for r in rows if subset(r)]
    slower = sum(r["future"] < r["current"] for r in selected)
    faster = sum(r["future"] > r["current"] for r in selected)
    return {"matches": len(selected), "later_slower": slower, "later_faster": faster,
            "average_current_ppm": round(sum(r["current"] for r in selected) / len(selected), 3) if selected else None,
            "average_future_ppm": round(sum(r["future"] for r in selected) / len(selected), 3) if selected else None}


def evaluate(rows):
    train, test, cutoff = split(rows, .70)
    report = {"eligible_matches": len(rows), "train_matches": len(train), "test_matches": len(test),
              "training_labels_purged_at_test_boundary": len(rows)-len(train)-len(test),
              "test_from_utc": cutoff.isoformat(),
              "target": "final_points_per_remaining_regulation_minute_including_unseparated_overtime" if rows[0].get("final_total") is not None else "scoring_points_per_game_minute",
              "fast_start_10pct": pattern(rows, lambda r: r["current"] > r["prior"] * 1.10),
              "slow_start_10pct": pattern(rows, lambda r: r["current"] < r["prior"] * .90)}
    if len(train) < 40 or len(test) < 20:
        report["status"] = "not_enough_for_chronological_fit"
        return report
    inner_train, inner_test, _ = split(train, .75)
    if not inner_train or not inner_test:
        report["status"] = "no_inner_temporal_split"
        return report
    models = {}
    baselines = {
        "current_persistence": lambda r: r["current"],
        "pregame_return": lambda r: r["prior"],
        "v7_whole_game_shrinkage": lambda r: (r["score"] + 10 * r["prior"]) / (r["elapsed"] + 10),
        "recent_persistence": lambda r: r["recent"] if r["recent"] is not None else r["current"],
        "market_remaining_rate": lambda r: (r["line"] - r["score"]) / r["remaining"],
    }
    majority = 1 if sum(r["future"] > r["current"] for r in train) >= sum(r["future"] < r["current"] for r in train) else -1
    report["training_majority_speed_direction"] = "acceleration" if majority == 1 else "deceleration"
    report["holdout_majority_direction_correct"] = sum(sign(r["future"]-r["current"]) == majority for r in test)
    report["holdout"] = {name: metrics(test, [fn(r) for r in test]) for name, fn in baselines.items()}
    for with_market in (False, True):
        name = "history_ridge" if not with_market else "history_and_market_ridge"
        candidates = []
        for penalty in (1, 10, 100):
            model = fit(inner_train, penalty, with_market)
            error = sum(abs(predict(model, r) - r["future"]) for r in inner_test) / len(inner_test)
            candidates.append((error, penalty))
        _, penalty = min(candidates)
        models[name] = fit(train, penalty, with_market)
        report["holdout"][name] = metrics(test, [predict(models[name], r) for r in test])
        report["holdout"][name]["penalty_selected_in_training"] = penalty
        model = models[name]
        names = FEATURE_NAMES + (("market_remaining_deviation",) if with_market else ())
        slopes = [w/s for w,s in zip(model["weights"][1:],model["scales"])]
        report["holdout"][name]["training_fit"] = {
            "target": "future_ppm / pregame_ppm - 1",
            "intercept": round(model["weights"][0]-sum(w*m for w,m in zip(slopes,model["means"])),6),
            "coefficients": {name: round(w,6) for name,w in zip(names,slopes)},
        }
    report["test_fast_start_10pct"] = pattern(test, lambda r: r["current"] > r["prior"] * 1.10)
    report["test_slow_start_10pct"] = pattern(test, lambda r: r["current"] < r["prior"] * .90)
    report["recent_available_in_test"] = sum(r["recent"] is not None for r in test)
    report["test_actual_acceleration"] = sum(r["future"] > r["current"] for r in test)
    report["test_actual_deceleration"] = sum(r["future"] < r["current"] for r in test)
    report["average_target_minutes_in_test"] = round(sum(r["target_minutes"] for r in test)/len(test),3)
    report["trend_available_in_test"] = sum(r["trend"] is not None for r in test)
    report["legacy_source_unverified_in_test"] = sum(not r["source_verified_at_recording"] for r in test)
    verified_test = [r for r in test if r["source_verified_at_recording"]]
    report["historical_source_verified_test_subset"] = {
        "matches": len(verified_test),
        "metrics": {name: metrics(verified_test,[fn(r) for r in verified_test]) for name,fn in baselines.items()}
        if verified_test else {},
    }
    report["legacy_clock_missing_in_test"] = sum(r["legacy_clock_missing"] for r in test)
    report["test_patterns_by_period"] = {
        str(period): {"all": pattern(test, lambda r,p=period: r["period"] == p),
                      "fast_at_signal_10pct": pattern(test, lambda r,p=period: r["period"] == p and r["current"]>r["prior"]*1.10),
                      "slow_at_signal_10pct": pattern(test, lambda r,p=period: r["period"] == p and r["current"]<r["prior"]*.90)}
        for period in sorted({r["period"] for r in test})}
    report["trend_continuation_at_recent_speed"] = {
        "test_matches": sum(r["recent"] is not None and r["trend"] is not None for r in test),
        "correct": sum(sign(r["future"]-r["recent"]) == sign(r["trend"])
                       for r in test if r["recent"] is not None and r["trend"] is not None),
    }
    report["status"] = "retrospective_temporal_holdout_not_live_validation"
    return report


def audit(db_path):
    with sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        alerts = [dict(r) for r in connection.execute("SELECT id,match_id,match_name,tournament,status,score,opening,prematch,live,alerted_at,prediction_context_json,reversal_features_json,market_provenance_json,final_total,result_source,settled_at,direction,quality_version,quality_score FROM alerts ORDER BY alerted_at,id")]
        snapshots = defaultdict(list)
        for row in connection.execute("SELECT match_id,recorded_at,period,game_clock,elapsed_game_seconds,remaining_minutes,total_score FROM match_live_snapshots ORDER BY recorded_at,id"):
            snapshots[row["match_id"]].append(dict(row))
    selected = first_signals(alerts)
    excluded = Counter()
    datasets = {"next_five_minutes": [], "remaining_regulation_to_last_minute": [], "final_total_including_unseparated_overtime": []}
    for alert in selected:
        state, error = observation(alert, snapshots[alert["match_id"]])
        if error:
            excluded[error] += 1
            continue
        for name, rows in datasets.items():
            if name == "final_total_including_unseparated_overtime":
                settled = timestamp(alert.get("settled_at"))
                final = alert.get("final_total")
                label = ({"future": (final-state["score"])/state["remaining"],
                          "target_minutes": state["remaining"], "label_at": settled, "final_total": final}
                         if alert.get("result_source") == "automatic_final_score" and settled
                         and settled > state["at"] and final is not None and final >= state["score"] else None)
            else:
                label = future_endpoint(state, snapshots[state["match_id"]], remaining_game=name != "next_five_minutes")
            if label:
                rows.append({**state, **label})
            else:
                excluded[name + "_no_clean_endpoint"] += 1
    return {"schema_version": 1, "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "alert_records": len(alerts), "first_signal_matches": len(selected),
            "snapshot_records": sum(map(len, snapshots.values())), "excluded": dict(excluded),
            "evaluations": {name: evaluate(rows) if rows else {"eligible_matches": 0} for name, rows in datasets.items()},
            "limitations": ["One first signal per match; published-signal selection, not all basketball games.",
                            "Legacy market proof is not retroactively verified.",
                            "Legacy absent clock text is checked against period, elapsed and remaining; exposed separately.",
                            "Five-minute endpoints allow 30 seconds tolerance; full remainder ends in the last regulation minute.",
                            "Scoring rate is not possession pace; snapshot labels exclude overtime. Final-total cohort includes unseparated overtime and is not a regulation speed label.",
                            "Fixed feature sets; penalty selected inside training; no holdout tuning."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite path; opened read-only")
    args = parser.parse_args()
    if args.db is None:
        from config import Config
        args.db = Config.DB_PATH
    print(json.dumps(audit(args.db), ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
