"""Fit/validate saved v9 forecast errors chronologically; print aggregates only."""

import argparse
import hashlib
import json
import math
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev

from config import Config
from forward_validation import _timestamp
from live_market import verify_current_market, verify_market
from live_signals import direction_for_total, forecast_live_total
from match_state import confirmed_12_minute_quarters, game_clock, quarter_clock_seconds
from win_probability import MODEL, outcome_probabilities, frozen_probability_direction, direction_for_probabilities


MIN_TRAINING = 60
MIN_VALIDATION_PER_DIRECTION = 25
MAX_CALIBRATION_ERROR = .10


def read_observations(path):
    first, excluded = {}, Counter()
    now = datetime.now(timezone.utc)
    with sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        connection.row_factory = sqlite3.Row
        rows = connection.execute("""
            SELECT s.*, f.final_total, f.result_source, f.settled_at
            FROM match_live_snapshots s
            LEFT JOIN forecast_match_results f ON f.match_id=s.match_id
            WHERE s.forecast_json IS NOT NULL ORDER BY s.recorded_at,s.id
        """)
        for raw in rows:
            row = dict(raw)
            try:
                context = json.loads(row["forecast_json"])
                forecast = context["forecast"]
                if (forecast.get("base_engine", forecast["engine"]) != MODEL["engine"]
                        or forecast["elapsed_minutes"] < 12 or forecast["remaining_minutes"] < 5
                        or forecast["prior_equivalent_minutes"] != 10
                        or forecast.get("over_continuation", {}).get("enabled") is not True
                        or forecast.get("over_calibration", {}).get("enabled") is not True):
                    continue
                # Select the first eligible observation before looking at its label.
                if row["match_id"] in first:
                    continue
                first[row["match_id"]] = None
                captured = _timestamp(context["market_captured_at"])
                recorded = _timestamp(row["recorded_at"])
                if (captured is None or recorded is None or not captured <= now or recorded > now
                        or (captured - recorded).total_seconds() > 1):
                    raise ValueError("invalid_capture")
                proof = json.loads(row["market_provenance_json"])
                if proof["version"] == "aiscore_live_v2":
                    verified, reason = verify_current_market(proof["source"], row["match_id"],
                        context["status"], context["score"], now=captured.timestamp())
                else:
                    verified, reason = verify_market(proof["source"], [proof["history_latest"]],
                        row["match_id"], context["status"], context["score"], now=captured.timestamp())
                if (reason or verified["live"] != forecast["line"] or forecast["line"] != row["live_total"]
                        or verified["score"] != f"{row['home_score']} - {row['away_score']}"
                        or verified["bookmaker_id"] != context["bookmaker_id"]
                        or forecast["direction"] != (frozen_probability_direction(forecast["win_probability"])
                            if forecast["engine"] == "future_pace_v10"
                            else direction_for_total(forecast["predicted_total"], forecast["line"]))):
                    raise ValueError("invalid_source")
                if row["result_source"] != "automatic_final_score" or row["final_total"] is None:
                    excluded["pending_first_observation"] += 1
                    continue
                settled = _timestamp(row["settled_at"])
                if settled is None or not recorded <= settled <= now:
                    raise ValueError("invalid_label_time")
                final = row["final_total"]
                if isinstance(final, bool) or not isinstance(final, int) or final < 0:
                    raise ValueError("invalid_final")
                first[row["match_id"]] = {"forecast": forecast, "final": final,
                    "captured": captured, "settled": settled,
                    "tournament": context.get("tournament") or "Unknown"}
            except (KeyError, TypeError, ValueError, ZeroDivisionError):
                excluded["invalid_first_observation"] += 1
    return sorted((row for row in first.values() if row), key=lambda row: row["captured"]), dict(excluded)


def read_reconstructed_observations(path):
    """Replay today's base model on proven past observations; never edit archives.

    Older snapshots predate saved forecasts. Their source proof, clock, score,
    opening/prematch totals and preceding observations can reconstruct a base
    estimate. Automatic archive finals supplement the newer forecast finals.
    This is retrospective reconstruction, not historical forward validation.
    """
    from main import _snapshot_has_verified_market

    now = datetime.now(timezone.utc)
    config = Config()
    history, first, confirmed = {}, {}, set()
    excluded, labels, metadata = Counter(), {}, {}
    with sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        for raw in connection.execute("SELECT match_id,match_name,tournament FROM alerts ORDER BY id"):
            metadata.setdefault(raw["match_id"], dict(raw))
        final_rows = connection.execute("""
            SELECT match_id,final_total,result_source,settled_at FROM forecast_match_results
            UNION ALL
            SELECT match_id,final_total,result_source,settled_at FROM alerts
            WHERE final_total IS NOT NULL
        """)
        conflicting = set()
        for raw in final_rows:
            final, settled = raw["final_total"], _timestamp(raw["settled_at"])
            if (raw["result_source"] != "automatic_final_score" or settled is None
                    or settled > now or type(final) is not int or final < 0):
                continue
            previous = labels.get(raw["match_id"])
            if previous and previous[0] != final:
                conflicting.add(raw["match_id"])
            elif previous is None or settled < previous[1]:
                labels[raw["match_id"]] = (final, settled)
        for match_id in conflicting:
            labels.pop(match_id, None)
        excluded["conflicting_final_matches"] = len(conflicting)
        rows = connection.execute("""
            SELECT * FROM match_live_snapshots
            WHERE length(market_provenance_json)>2 ORDER BY recorded_at,id
        """)
        for raw in rows:
            row = dict(raw)
            is_first = False
            if not _snapshot_has_verified_market(row):
                continue
            match_id = row["match_id"]
            try:
                proof = json.loads(row["market_provenance_json"])
                context = json.loads(row["forecast_json"] or "{}")
                info = context if context.get("match_name") else metadata.get(match_id)
                if info is None:
                    continue
                status = f"Q{row['period']} {row['game_clock']}"
                match = {"match_id": match_id, "match_name": info["match_name"],
                    "tournament": info.get("tournament") or "Unknown", "status": status,
                    "score": f"{row['home_score']} - {row['away_score']}",
                    "opening_total": proof["opening"], "prematch_total": proof.get("prematch"),
                    "inplay_total": row["live_total"], "market_provenance": proof,
                    "market_captured_at": proof["captured_at"]}
                configured = game_clock("", tournament=match["tournament"])
                seconds = quarter_clock_seconds(status)
                if configured["period_count"] == 4 and seconds is not None and seconds > 600:
                    confirmed.add(match_id)
                with confirmed_12_minute_quarters(match_id in confirmed):
                    forecast = forecast_live_total(match, config, history.get(match_id, []))
                eligible = (forecast is not None and forecast["elapsed_minutes"] >= 12
                            and forecast["remaining_minutes"] >= 5)
                is_first = eligible and match_id not in first
                if is_first:
                    first[match_id] = None
                captured, recorded = _timestamp(proof["captured_at"]), _timestamp(row["recorded_at"])
                if (captured is None or recorded is None or captured > now or recorded > now
                        or (captured - recorded).total_seconds() > 1):
                    raise ValueError("invalid capture")
                if proof["version"] == "aiscore_live_v2":
                    verified, reason = verify_current_market(proof["source"], match_id,
                        status, match["score"], now=captured.timestamp())
                else:
                    verified, reason = verify_market(proof["source"], [proof["history_latest"]],
                        match_id, status, match["score"], now=captured.timestamp())
                if (reason or verified["live"] != row["live_total"]
                        or verified["opening"] != match["opening_total"]
                        or verified.get("prematch") != match["prematch_total"]
                        or verified["bookmaker_id"] != proof["bookmaker_id"]):
                    raise ValueError("invalid source")
                history.setdefault(match_id, []).append(row)
                if not is_first:
                    continue
                label = labels.get(match_id)
                if label is None:
                    excluded["pending_first_observation"] += 1
                    continue
                final, settled = label
                if settled < recorded or final < row["total_score"]:
                    raise ValueError("invalid final time or score")
                first[match_id] = {"forecast": forecast, "final": final,
                    "captured": captured, "settled": settled,
                    "tournament": match["tournament"],
                    "reconstructed": not bool(context.get("forecast")),
                    "saved_base_engine": context.get("forecast", {}).get("base_engine",
                        context.get("forecast", {}).get("engine"))}
            except (KeyError, TypeError, ValueError, ZeroDivisionError):
                if is_first:
                    excluded["invalid_first_observation"] += 1
    return sorted((row for row in first.values() if row), key=lambda row: row["captured"]), dict(excluded)


def fit_for_future_matches(rows):
    """Fit all finals known now; preserve a separate chronological method check."""
    check = fit_and_validate(rows)
    model = json.loads(json.dumps(check))
    model.pop("chronological_validation", None)
    model["chronological_validation"] = {key: check.get(key) for key in (
        "training_matches", "validation_matches", "training_cutoff_utc",
        "mean_normalized_error", "normalized_error_scale", "validation")}
    errors = [(row["final"] - row["forecast"].get("base_predicted_total",
               row["forecast"]["predicted_total"])) / math.sqrt(row["forecast"]["remaining_minutes"])
              for row in rows]
    if len(errors) < 2 or pstdev(errors) <= 0:
        raise ValueError("insufficient production fit data")
    fitted_at = datetime.now(timezone.utc).isoformat()
    model.update(training_matches=len(rows), mean_normalized_error=mean(errors),
        normalized_error_scale=pstdev(errors), fitted_at_utc=fitted_at, training_cutoff_utc=fitted_at,
        data_scope="first_source_verified_current_base_reconstruction_per_match",
        reconstructed_matches=sum(row.get("reconstructed") is True for row in rows),
        prospective_validated=False)
    model["training_data_sha256"] = hashlib.sha256(json.dumps([
        [row["captured"].isoformat(), row["settled"].isoformat(),
         row["forecast"].get("base_predicted_total", row["forecast"]["predicted_total"]),
         row["forecast"]["remaining_minutes"],
         row["forecast"]["line"], row["final"], row.get("tournament")]
        for row in rows], separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    model["validation"] = {direction: {"accepted": False,
        "matches": check["validation"][direction]["matches"],
        "reason": "production_refit_requires_forward_validation"} for direction in ("ALT", "ÜST")}
    return model


def league_summary(rows):
    groups = {}
    for row in rows:
        groups.setdefault(row.get("tournament") or "Unknown", []).append(row)
    result = []
    for league, samples in groups.items():
        errors = [(row["final"] - row["forecast"].get("base_predicted_total",
            row["forecast"]["predicted_total"])) / math.sqrt(row["forecast"]["remaining_minutes"])
            for row in samples]
        result.append({"league": league, "matches": len(samples),
            "mean_normalized_error": mean(errors),
            "normalized_error_scale": pstdev(errors) if len(errors) > 1 else None})
    return sorted(result, key=lambda row: (-row["matches"], row["league"]))


def fit_and_validate(rows):
    model = json.loads(json.dumps(MODEL))
    for key in ("chronological_validation", "training_data_sha256", "fitted_at_utc", "reconstructed_matches"):
        model.pop(key, None)
    model["training_matches"] = 0
    model["validation"] = {direction: {"accepted": False, "matches": 0,
        "reason": "insufficient_validated_data"} for direction in ("ALT", "ÜST")}
    if len(rows) < 2:
        return model
    cutoff = rows[int(len(rows) * .7)]["captured"]
    training = [row for row in rows if row["captured"] < cutoff and row["settled"] < cutoff]
    validation = [row for row in rows if row["captured"] >= cutoff]
    errors = [(row["final"] - row["forecast"].get("base_predicted_total", row["forecast"]["predicted_total"]))
              / math.sqrt(row["forecast"]["remaining_minutes"]) for row in training]
    model.update(training_matches=len(training), validation_matches=len(validation),
                 training_cutoff_utc=cutoff.isoformat(),
                 data_scope="first_eligible_source_verified_v9_forecast_per_match",
                 prospective_validated=False)
    if len(training) < 2:
        return model
    mu, scale = mean(errors), pstdev(errors)
    if scale <= 0:
        return model
    model.update(mean_normalized_error=mu, normalized_error_scale=scale)
    def estimate(row):
        return outcome_probabilities(row["forecast"].get("base_predicted_total", row["forecast"]["predicted_total"]),
            row["forecast"]["line"], row["forecast"]["remaining_minutes"], mu, scale,
            score=row["forecast"].get("score_total"), training_matches=len(training))
    training_values = [(row, estimate(row)) for row in training]
    validation_values = [(row, estimate(row)) for row in validation]
    for direction in ("ALT", "ÜST"):
        chosen = [(row, values) for row, values in validation_values
                  if direction_for_probabilities(values) == direction]
        samples = [row for row, _ in chosen]
        predictions = [values[direction] for _, values in chosen]
        outcomes = [int(row["final"] < row["forecast"]["line"] if direction == "ALT"
            else row["final"] > row["forecast"]["line"]) for row in samples]
        train_direction = [row for row, values in training_values
                           if direction_for_probabilities(values) == direction]
        train_wins = sum(row["final"] < row["forecast"]["line"] if direction == "ALT"
            else row["final"] > row["forecast"]["line"] for row in train_direction)
        baseline = (train_wins + 1) / (len(train_direction) + 2)
        check = model["validation"][direction]
        check.update(matches=len(samples), training_matches=len(train_direction))
        if not samples:
            continue
        brier = mean((p - outcome) ** 2 for p, outcome in zip(predictions, outcomes))
        baseline_brier = mean((baseline - outcome) ** 2 for outcome in outcomes)
        gap = abs(mean(predictions) - mean(outcomes))
        bins = []
        for bucket in range(10):
            indices = [index for index, p in enumerate(predictions) if min(9, int(p * 10)) == bucket]
            if indices:
                bins.append({"matches": len(indices),
                    "average_probability": mean(predictions[index] for index in indices),
                    "observed_win_rate": mean(outcomes[index] for index in indices)})
        bin_error = sum(group["matches"] * abs(group["average_probability"] - group["observed_win_rate"])
                        for group in bins) / len(samples)
        check.update(brier=brier, baseline_brier=baseline_brier,
                     average_probability=mean(predictions), observed_win_rate=mean(outcomes),
                     calibration_error=gap,
                     calibration_bins=bins, binned_calibration_error=bin_error,
                     probability_range=[min(predictions), max(predictions)])
        if len(training) < MIN_TRAINING:
            check["reason"] = "insufficient_validated_data"
        elif len(samples) < MIN_VALIDATION_PER_DIRECTION or len(train_direction) < MIN_VALIDATION_PER_DIRECTION:
            check["reason"] = "insufficient_directional_validation"
        elif max(gap, bin_error) > MAX_CALIBRATION_ERROR or brier >= baseline_brier:
            check["reason"] = "probability_validation_failed"
        else:
            check.update(accepted=True, reason="retrospective_validation_passed")
    return model


def audit(path, *, reconstruct=False):
    rows, excluded = (read_reconstructed_observations(path) if reconstruct else read_observations(path))
    model = fit_and_validate(rows)
    return {"eligible_final_matches": len(rows), "excluded": excluded, "candidate_model": model,
            "leagues": league_summary(rows),
            **({"production_refit_model": fit_for_future_matches(rows)} if reconstruct else {}),
            "limitations": ["Previously observed retrospective data; not new prospective validation.",
                "Square-root remaining-time error scaling and normal tails are model assumptions.",
                "Validation does not account for league/day correlation.",
                "Estimated probabilities and validation acceptance are separate; prices and profit are not used."]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--reconstruct", action="store_true",
                        help="Replay current base on proven legacy snapshots and include automatic archive finals")
    args = parser.parse_args()
    print(json.dumps(audit(args.db, reconstruct=args.reconstruct), ensure_ascii=False, indent=2))
