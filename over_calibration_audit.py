"""Reproduce frozen hot-excess fitting and paired v8/v9 tests; read-only aggregates."""

import argparse
import json
from statistics import mean

from config import Config
from over_calibration import MODEL, MODEL_SHA256, calibration_applies, calibrate_over_continuation, correction_factor
from over_signal_audit import compare, replay_dataset
from reversal_audit import split
from directional_audit import prediction_metrics


METHODS = ("none", "point", "rate", "relative", "excess")


def fit_bias(rows, method, config):
    active = [r for r in rows if calibration_applies(
        r, r["continuation"], r["prior"], config.PRIOR_EQUIV_MINUTES)]
    if method == "none":
        return 0.0, len(active)
    xs = [correction_factor(method, r, r["continuation"], r["prior"]) for r in active]
    denominator = sum(x * x for x in xs)
    value = sum(x * (r["adjusted"] - r["final"]) for r, x in zip(active, xs))
    return max(0.0, value / denominator) if denominator else 0.0, len(active)


def adjusted(row, method, coefficient, config):
    candidate = {**MODEL, "method": method, "coefficient": coefficient}
    result = calibrate_over_continuation(row["continuation"], row, row["prior"], config, model=candidate)
    return row["score"] + row["remaining"] * result["future_ppm"]


def select_training_model(training, cutoff, config):
    """Three nonoverlapping validation blocks; label timestamps precede each fit cutoff."""
    candidates = {}
    for method in METHODS:
        errors, folds = [], []
        for start_fraction, end_fraction in ((.4, .6), (.6, .8), (.8, 1.0)):
            prefix, later, start = split(training, start_fraction)
            end = split(training, end_fraction)[2] if end_fraction < 1 else cutoff
            block = [r for r in later if r["at"] < end]
            coefficient, active_count = fit_bias(prefix, method, config)
            fold_errors = [abs(adjusted(r, method, coefficient, config) - r["final"]) for r in block]
            errors.extend(fold_errors)
            folds.append({"training": len(prefix), "active_training": active_count,
                          "validation": len(block), "coefficient": coefficient,
                          "mae": mean(fold_errors) if fold_errors else None,
                          "training_labels_before_validation": all(r["label_at"] < start for r in prefix)})
        candidates[method] = {"mae": mean(errors) if errors else None, "folds": folds}
    eligible = [m for m in METHODS if candidates[m]["mae"] is not None]
    if not eligible:
        return {"status": "insufficient_training"}
    selected = min(eligible, key=lambda m: candidates[m]["mae"])
    coefficient, active = fit_bias(training, selected, config)
    return {"status": "fitted", "candidates": candidates, "selected": selected,
            "coefficient": coefficient, "active_training": active}


def audit(db_path):
    rows, as_of, excluded = replay_dataset(db_path)
    if not rows:
        return {"eligible": 0, "excluded": dict(excluded)}
    config = Config()
    config.OVER_CALIBRATION_ENABLED = True
    training, later, cutoff = split(rows, .6)
    selection = select_training_model(training, cutoff, config)
    # The production comparison always uses the checked-in frozen model,
    # never today's refit or a coefficient selected on later test outcomes.
    changed = [{**r, "adjusted": adjusted(r, MODEL["method"], MODEL["coefficient"], config)} for r in later]
    v8 = compare(later)
    v9 = compare(changed)
    def alerts(sample, new):
        mapped = [{**r, "center": r["adjusted"] if new else r["center"]} for r in sample]
        return prediction_metrics([r for r in mapped if abs(r["center"] - r["line"]) >= r["threshold"]])
    return {"eligible": len(rows), "training": len(training), "later_test": len(later),
            "as_of_utc": as_of.isoformat(), "cutoff_utc": cutoff.isoformat(),
            "excluded": dict(excluded), "training_selection": selection,
            "deployed_model": MODEL, "model_sha256": MODEL_SHA256,
            "training_coefficient_reproduces_frozen": selection.get("selected") == MODEL["method"]
                 and abs(selection.get("coefficient", 0) - MODEL["coefficient"]) < 1e-9,
            "v8_comparison": v8, "v9_comparison": v9,
            "all_alerts_v7": alerts(later, False), "all_alerts_v8": alerts(later, True),
            "all_alerts_v9": alerts(changed, True),
            "training_test_overlap": len({r["match_id"] for r in training} & {r["match_id"] for r in later}),
            "limitations": ["Previously inspected retrospective, old-engine selected data; not prospective results.",
                            "Final totals have unseparated overtime; not clean regulation labels.",
                            "Direction changes remain in the original upper subset; upper alerts reported separately.",
                            "No raw observations are exported; audit never changes records or live settings."]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=Config.DB_PATH)
    args = parser.parse_args()
    print(json.dumps(audit(args.db), ensure_ascii=False, indent=2))
