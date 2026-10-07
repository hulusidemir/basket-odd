"""Read-only ALT/UST outcomes and chronological over-bias experiments.

Emits aggregates only. Historical replay is not a deployed calibrated model.
"""

import argparse
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

from config import Config
from forecast_tracking import signal_tracking_summary
from forward_validation import report
from reversal_audit import first_signals, observation, split, timestamp, wilson


def alert_threshold(state, rules):
    if (state["elapsed"] < rules.MIN_ELAPSED_MINUTES
            or state["remaining"] < rules.MIN_REMAINING_MINUTES
            or state["score"] >= state["line"]):
        return math.inf
    threshold = max(rules.MIN_EDGE_POINTS, state["line"] * rules.MIN_EDGE_RATIO)
    margin = state["margin"] * state["score"]
    if state["elapsed"] >= state["duration"] / 2:
        if margin >= rules.EXTREME_BLOWOUT_MARGIN:
            threshold *= rules.EXTREME_BLOWOUT_EDGE_MULTIPLIER
        elif margin >= rules.BLOWOUT_MARGIN:
            threshold *= rules.BLOWOUT_EDGE_MULTIPLIER
    reference = state["prior"] * state["duration"]
    if abs(state["line"] / reference - 1) >= rules.LARGE_REPRICE_RATIO:
        threshold *= rules.LARGE_REPRICE_EDGE_MULTIPLIER
    return threshold


def prediction_metrics(rows, *, correction=0):
    counts = Counter()
    errors = []
    for row in rows:
        center = max(row["score"], row["center"] - correction)
        direction = "ALT" if center < row["line"] else "ÜST" if center > row["line"] else "EŞİT"
        counts[direction] += 1
        if row["final"] == row["line"]:
            counts["pushes"] += 1
        elif direction == "EŞİT":
            counts["no_direction"] += 1
        elif (center < row["line"]) == (row["final"] < row["line"]):
            counts["wins"] += 1
        else:
            counts["losses"] += 1
        errors.append(abs(center - row["final"]))
    decided = counts["wins"] + counts["losses"]
    return {"matches": len(rows), **{name: counts[name] for name in
            ("wins", "losses", "pushes", "no_direction", "ALT", "ÜST", "EŞİT")},
            "success_rate": round(100 * counts["wins"] / decided, 2) if decided else None,
            "wilson_95": wilson(counts["wins"], decided),
            "model_mae": round(mean(errors), 3) if errors else None}


def calibration_experiment(rows, *, train_fraction=.6):
    if not 0 < train_fraction < 1:
        raise ValueError("train_fraction must be between zero and one")
    if not rows:
        return {"eligible": 0, "status": "no_eligible_data"}
    train, test, cutoff = split(rows, train_fraction)
    upper_train = [row for row in train if row["center"] - row["line"] >= row["threshold"]]
    if not upper_train:
        return {"eligible": len(rows), "training": len(train), "test": len(test),
                "status": "no_upper_training_data", "cutoff_utc": cutoff.isoformat()}
    # One predeclared parameter; never fit it on the test's outcomes or subgroup.
    correction = max(0, mean(row["center"] - row["final"] for row in upper_train))

    def compare(sample):
        upper = [row for row in sample if row["center"] > row["line"]]
        original_alerts = [row for row in upper if row["center"] - row["line"] >= row["threshold"]]
        filtered = [row for row in original_alerts
                    if row["center"] - row["line"] - correction >= row["threshold"]]
        return {"upper_predictions_before": prediction_metrics(upper),
                "upper_predictions_bias_corrected": prediction_metrics(upper, correction=correction),
                "upper_alerts_before": prediction_metrics(original_alerts),
                # Remaining bets still have their original OVER direction/line.
                "upper_alerts_with_correction_margin": prediction_metrics(filtered),
                "retained_alert_fraction": round(len(filtered) / len(original_alerts), 4)
                                          if original_alerts else None,
                "under_predictions_unchanged": prediction_metrics(
                    [row for row in sample if row["center"] < row["line"]])}

    return {"status": "research_only", "eligible": len(rows), "training": len(train), "test": len(test),
            "cutoff_utc": cutoff.isoformat(), "upper_training": len(upper_train),
            "over_correction_points": round(correction, 6),
            "training_labels_before_test": all(row["label_at"] < cutoff for row in train),
            "training_test_match_overlap": len({row["match_id"] for row in train}
                                              & {row["match_id"] for row in test}),
            "holdout": compare(test),
            "historically_verified_source_holdout": compare(
                [row for row in test if row["source_verified_at_recording"]])}


def audit(db_path, *, train_fraction=.6):
    with sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        alerts = [dict(row) for row in connection.execute("""
            SELECT id, match_id, match_name, tournament, status, score, opening, prematch, live,
                alerted_at, prediction_context_json, reversal_features_json, market_provenance_json,
                final_total, result_source, settled_at, direction, quality_version, quality_score
            FROM alerts ORDER BY alerted_at, id
        """)]
        snapshots = defaultdict(list)
        for row in connection.execute("""
            SELECT match_id, recorded_at, period, game_clock, elapsed_game_seconds,
                remaining_minutes, total_score FROM match_live_snapshots ORDER BY recorded_at, id
        """):
            snapshots[row["match_id"]].append(dict(row))
    rules = Config()
    selected = first_signals(alerts)
    dataset = []
    excluded = Counter()
    as_of = datetime.now(timezone.utc)
    for alert in selected:
        state, error = observation(alert, snapshots[alert["match_id"]])
        if error:
            excluded[error] += 1
            continue
        settled = timestamp(alert.get("settled_at"))
        total = alert.get("final_total")
        if (alert.get("result_source") != "automatic_final_score" or settled is None
                or not state["at"] < settled <= as_of
                or isinstance(total, bool) or not isinstance(total, (int, float))
                or not math.isfinite(total) or total < state["score"]):
            excluded["no_eligible_automatic_final"] += 1
            continue
        center = state["score"] + state["remaining"] * (
            state["score"] + rules.PRIOR_EQUIV_MINUTES * state["prior"]
        ) / (state["elapsed"] + rules.PRIOR_EQUIV_MINUTES)
        dataset.append({**state, "label_at": settled, "final": total, "center": center,
                        "threshold": alert_threshold(state, rules)})
    verified = report(db_path, as_of=as_of)
    return {"as_of_utc": as_of.isoformat(), "recorded_signals": signal_tracking_summary(selected, as_of=as_of),
            "verified_saved_policy_cohorts": [
                {"first_prediction_utc": value["first_prediction_utc"],
                 "by_direction": value["by_direction"]}
                for value in sorted(verified["cohorts"].values(), key=lambda c: c["first_prediction_utc"])],
            "replay": {"method": "v7_whole_game_pregame_shrinkage_reconstructed_at_first_signal",
                       "prior_equivalent_minutes": rules.PRIOR_EQUIV_MINUTES,
                       "rules": {name: getattr(rules, name) for name in (
                           "MIN_EDGE_POINTS", "MIN_EDGE_RATIO", "MIN_ELAPSED_MINUTES", "MIN_REMAINING_MINUTES",
                           "BLOWOUT_MARGIN", "BLOWOUT_EDGE_MULTIPLIER", "EXTREME_BLOWOUT_MARGIN",
                           "EXTREME_BLOWOUT_EDGE_MULTIPLIER", "LARGE_REPRICE_RATIO", "LARGE_REPRICE_EDGE_MULTIPLIER")},
                       "excluded": dict(excluded),
                       **calibration_experiment(dataset, train_fraction=train_fraction)},
            "limitations": [
                "Recorded outcomes mix old policies; source-verified saved policy cohorts are separate.",
                "Replay uses observations selected by old signal engines; not all games or live v7 success.",
                "Source verification at historical capture does not make an old prediction a deployed v7 forecast.",
                "This dataset has already been inspected; chronological holdout is retrospective, not a new prospective test.",
                "The correction may flip predictions; filtering retains original OVER bets and reports lost coverage.",
                "Final totals include unseparated overtime; they are not clean regulation labels.",
                "Win rate is not a calibrated probability or profit. No live setting or record was changed."]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=Config.DB_PATH)
    parser.add_argument("--train-fraction", type=float, default=.6)
    args = parser.parse_args()
    print(json.dumps(audit(args.db, train_fraction=args.train_fraction), ensure_ascii=False, indent=2))
