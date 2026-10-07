"""Read-only paired v7/v8 replay at the first recorded signal; aggregates only."""

import argparse
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

from config import Config
from directional_audit import alert_threshold, prediction_metrics
from live_signals import direction_for_total
from pace_calculator import get_over_continuation
from reversal_audit import first_signals, object_json, observation, split, timestamp


def compare(rows):
    """Keep the original upper subset fixed, and report alert coverage separately."""
    upper = [r for r in rows if r["center"] > r["line"]]

    def paired(sample):
        changed = [{**r, "center": r["adjusted"]} for r in sample]
        return {"before": prediction_metrics(sample), "after": prediction_metrics(changed),
                "changed_direction": sum(direction_for_total(r["center"], r["line"])
                                         != direction_for_total(r["adjusted"], r["line"])
                                         for r in sample),
                "upper_alerts_before": prediction_metrics(
                    [r for r in sample if r["center"] - r["line"] >= r["threshold"]]),
                "upper_alerts_after": prediction_metrics(
                    [r for r in changed if r["center"] - r["line"] >= r["threshold"]]),
                "mean_correction_points": round(mean(r["center"] - r["adjusted"]
                                                     for r in sample), 3) if sample else None,
                "continuation_sources": dict(Counter(r["continuation_source"] for r in sample))}

    return {"all_predictions": paired(rows), "original_upper_predictions": paired(upper),
            "source_verified_original_upper": paired(
                [r for r in upper if r["source_verified_at_recording"]])}


def replay_dataset(db_path):
    with sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        alerts = [dict(r) for r in conn.execute("""
            SELECT id,match_id,match_name,tournament,status,score,opening,prematch,live,
                   alerted_at,prediction_context_json,reversal_features_json,market_provenance_json,
                   final_total,result_source,settled_at,direction,quality_version,quality_score
            FROM alerts ORDER BY alerted_at,id
        """)]
        snapshots = defaultdict(list)
        for r in conn.execute("""
            SELECT match_id,recorded_at,period,game_clock,elapsed_game_seconds,remaining_minutes,
                   total_score,market_provenance_json FROM match_live_snapshots ORDER BY recorded_at,id
        """):
            snapshots[r["match_id"]].append(dict(r))
    config = Config()
    config.OVER_CONTINUATION_ENABLED = True
    as_of = datetime.now(timezone.utc)
    dataset, excluded = [], Counter()
    for alert in first_signals(alerts):
        state, error = observation(alert, snapshots[alert["match_id"]])
        if error:
            excluded[error] += 1
            continue
        final, settled = alert.get("final_total"), timestamp(alert.get("settled_at"))
        if (alert.get("result_source") != "automatic_final_score" or settled is None
                or not state["at"] < settled <= as_of or isinstance(final, bool)
                or not isinstance(final, (int, float)) or not math.isfinite(final) or final < state["score"]):
            excluded["no_eligible_automatic_final"] += 1
            continue
        future = (state["score"] + config.PRIOR_EQUIV_MINUTES * state["prior"]) / (
            state["elapsed"] + config.PRIOR_EQUIV_MINUTES)
        history = snapshots[alert["match_id"]]
        if state["source_verified_at_recording"]:
            # Match the worker's verified-history requirement in the strong-source subgroup.
            history = [r for r in history if
                       (proof := object_json(r.get("market_provenance_json"))).get("verified") is True
                       and proof.get("version") in {"bet365_history_v1", "aiscore_history_v2", "aiscore_live_v2"}
                       and proof.get("market") == "bs"]
        captured = timestamp(object_json(alert.get("prediction_context_json")).get("market_captured_at"))
        continuation = get_over_continuation(history, {
            "elapsed_game_seconds": round(state["elapsed"] * 60), "total_score": state["score"],
            "period": state["period"], "quarter_length": state["quarter_length"],
            "period_count": state["period_count"], "observed_at": (captured or state["at"]).isoformat(),
        }, state["prior"], future, config)
        dataset.append({**state, "label_at": settled, "final": final,
                        "center": state["score"] + state["remaining"] * future,
                        "adjusted": state["score"] + state["remaining"] * continuation["future_ppm"],
                        "continuation_source": continuation["source"],
                        "continuation": continuation,
                        "threshold": alert_threshold(state, config)})
    return dataset, as_of, excluded


def audit(db_path):
    dataset, as_of, excluded = replay_dataset(db_path)
    if not dataset:
        return {"eligible": 0, "excluded": dict(excluded)}
    training, later, cutoff = split(dataset, .6)
    return {"method": "paired_v7_v8_hot_start_continuation", "eligible": len(dataset),
            "as_of_utc": as_of.isoformat(), "excluded": dict(excluded),
            "training": len(training), "later_test": len(later), "cutoff_utc": cutoff.isoformat(),
            "training_test_match_overlap": len({r["match_id"] for r in training}
                                              & {r["match_id"] for r in later}),
            "training_labels_before_test": all(r["label_at"] < cutoff for r in training),
            "training_comparison": compare(training), "later_comparison": compare(later),
            "limitations": [
                "Already inspected retrospective data, not prospective v8 results or calibrated probabilities.",
                "Old signal engines selected this sample; coverage does not represent all live games.",
                "Legacy observations have weaker source provenance; strong-source subgroup is separate.",
                "Final totals contain unseparated overtime; not clean regulation outcomes.",
                "No odds/stakes are used; prediction accuracy is not profit.",
                "Audit is read-only and never rewrites frozen forecasts or outcomes."]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=Config.DB_PATH)
    args = parser.parse_args()
    print(json.dumps(audit(args.db), ensure_ascii=False, indent=2))
