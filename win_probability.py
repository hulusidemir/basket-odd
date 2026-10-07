"""Frozen outcome-error probabilities, independent of bookmaker prices.

The distribution uses errors from saved, source-verified forecasts. An
estimated probability and its validation status are separate outputs.
It never trains or reads SQLite at runtime.
"""

import hashlib
import json
import math
from pathlib import Path


VERSION = "win_probability_v2"
MODEL_PATH = Path(__file__).resolve().parent / "models" / "win_probability_v1.json"
MODEL_BYTES = MODEL_PATH.read_bytes()
MODEL = json.loads(MODEL_BYTES)
MODEL_SHA256 = hashlib.sha256(MODEL_BYTES).hexdigest()


def outcome_probabilities(center, line, remaining, mean_error, error_scale, *, score=None,
                          training_matches=0):
    """Normal residual model with integer final scores and a separate push mass."""
    values = (center, line, remaining, mean_error, error_scale)
    if any(isinstance(value, bool) or not isinstance(value, (int, float))
           or not math.isfinite(value) for value in values):
        raise ValueError("invalid probability input")
    if remaining <= 0 or error_scale <= 0:
        raise ValueError("invalid probability scale")
    root_minutes = math.sqrt(remaining)
    expected = center + root_minutes * mean_error
    # Include uncertainty in the learned error mean, rather than treating a
    # finite training sample as a known population mean.
    uncertainty = math.sqrt(1 + 1 / training_matches) if training_matches > 0 else 1
    standard_deviation = root_minutes * error_scale * uncertainty

    def cdf(boundary):
        return (1 + math.erf((boundary - expected) / (standard_deviation * math.sqrt(2)))) / 2

    if score is not None and (isinstance(score, bool) or not isinstance(score, int) or score < 0):
        raise ValueError("invalid scored total")
    floor_mass = cdf(score - .5) if score is not None else 0
    surviving_mass = 1 - floor_mass
    if surviving_mass <= 1e-12:
        raise ValueError("invalid distribution support")

    def supported_cdf(boundary):
        return max(0, min(1, (cdf(boundary) - floor_mass) / surviving_mass))

    under = supported_cdf(math.ceil(line) - .5)
    over = 1 - supported_cdf(math.floor(line) + .5)
    return {"ALT": under, "ÜST": over, "push": max(0.0, 1 - under - over)}


def direction_for_probabilities(values):
    difference = values["ÜST"] - values["ALT"]
    return "EŞİT" if abs(difference) <= 1e-12 else "ÜST" if difference > 0 else "ALT"


def frozen_probability_direction(estimate):
    """Check the saved distribution without recalculating from today's model."""
    values = {"ALT": estimate["under_probability"], "ÜST": estimate["over_probability"],
              "push": estimate["push_probability"]}
    if (any(isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(value) or not 0 <= value <= 1 for value in values.values())
            or abs(sum(values.values()) - 1) > 1e-6):
        raise ValueError("invalid frozen probabilities")
    direction = direction_for_probabilities(values)
    if direction != estimate["preferred_direction"]:
        raise ValueError("inconsistent frozen probability direction")
    return direction


def assess_forecast(forecast, *, model=None):
    model_hash = MODEL_SHA256 if model is None else hashlib.sha256(
        json.dumps(model, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    model = MODEL if model is None else model
    direction = forecast.get("direction")
    validation = model.get("validation", {}).get(direction, {})
    result = {"version": VERSION, "model_sha256": model_hash,
              "probability": None, "push_probability": None, "validated": False,
              "prospective_validated": model.get("prospective_validated") is True,
              "reason": "insufficient_validated_data", "direction": direction,
              "training_matches": model.get("training_matches", 0),
              "under_probability": None, "over_probability": None,
              "validation_matches": validation.get("matches", 0)}
    if direction not in ("ALT", "ÜST", "EŞİT"):
        result["reason"] = "no_direction"
        return result
    elapsed, remaining = forecast.get("elapsed_minutes"), forecast.get("remaining_minutes")
    if any(isinstance(value, bool) or not isinstance(value, (int, float))
           or not math.isfinite(value) for value in (elapsed, remaining)):
        result["reason"] = "invalid_model_input"
        return result
    regime = model["regime"]
    if (forecast.get("base_engine", forecast.get("engine")) != model["engine"]
            or forecast.get("prior_equivalent_minutes") != regime["prior_equivalent_minutes"]
            or forecast.get("over_continuation", {}).get("enabled") is not True
            or forecast.get("over_calibration", {}).get("enabled") is not True
            or elapsed < 0 or remaining <= 0
            or round(elapsed + remaining, 6)
               not in regime["durations"]):
        result["reason"] = "outside_model_regime"
        return result
    try:
        values = outcome_probabilities(forecast.get("base_predicted_total", forecast["predicted_total"]),
            forecast["line"], remaining, model["mean_normalized_error"], model["normalized_error_scale"],
            score=forecast.get("score_total"), training_matches=model.get("training_matches", 0))
    except (KeyError, ValueError, TypeError):
        result["reason"] = "invalid_model_input"
        return result
    lower, upper = validation.get("probability_range", [0.0, 1.0])
    in_regime = elapsed >= regime["min_elapsed_minutes"] and remaining >= regime["min_remaining_minutes"]
    probability = values[direction] if direction in values else max(values["ALT"], values["ÜST"])
    validated = validation.get("accepted") is True and in_regime and lower <= probability <= upper
    result.update(probability=probability, push_probability=values["push"],
                  under_probability=values["ALT"], over_probability=values["ÜST"],
                  preferred_direction=direction_for_probabilities(values),
                  validated=validated,
                  reason="chronologically_validated_estimate" if validated else "model_estimate",
                  validation_reason=validation.get("reason", "not_validated"),
                  expected_total=forecast.get("base_predicted_total", forecast["predicted_total"])
                    + math.sqrt(remaining) * model["mean_normalized_error"],
                  error_standard_deviation=math.sqrt(remaining) * model["normalized_error_scale"]
                    * math.sqrt(1 + 1 / model["training_matches"]) if model.get("training_matches", 0) > 0
                    else math.sqrt(remaining) * model["normalized_error_scale"])
    return result
