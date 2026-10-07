"""Frozen empirical hot-scoring correction. No database access or live fitting."""

import hashlib
import json
import math
from pathlib import Path


_MODEL_BYTES = (Path(__file__).resolve().parent / "models" / "over_calibration_v1.json").read_bytes()
MODEL = json.loads(_MODEL_BYTES)
MODEL_SHA256 = hashlib.sha256(_MODEL_BYTES).hexdigest()


def calibration_applies(state, continuation, prior, prior_minutes):
    recent = continuation.get("recent_window")
    return (prior_minutes == MODEL["prior_equivalent_minutes"]
            and state["duration"] in MODEL["durations"]
            and state["elapsed"] >= MODEL["min_elapsed_minutes"]
            and state["remaining"] >= MODEL["min_remaining_minutes"]
            and continuation.get("enabled") is True
            and continuation["raw_future_ppm"] > prior
            and recent is not None and recent["future_ppm"] >= prior)


def correction_factor(method, state, continuation, prior):
    if method == "none":
        return 0.0
    if method == "point":
        return 1.0
    if method == "rate":
        return state["remaining"]
    if method == "relative":
        return state["remaining"] * prior
    if method == "excess":
        return state["remaining"] * max(0.0, continuation["future_ppm"] - prior)
    raise ValueError("Unknown calibration method")


def calibrate_over_continuation(continuation, state, prior, config, *, model=None):
    model = MODEL if model is None else model
    enabled = getattr(config, "OVER_CALIBRATION_ENABLED", True)
    ppm = continuation["future_ppm"]
    model_hash = (MODEL_SHA256 if model is MODEL else hashlib.sha256(
        json.dumps(model, sort_keys=True, separators=(",", ":")).encode()).hexdigest())
    result = {"version": model["version"], "model_sha256": model_hash,
              "enabled": enabled, "applied": False, "future_ppm": ppm,
              "coefficient": model["coefficient"], "method": model["method"],
              "correction_points": 0.0, "reason": "outside_calibration_regime"}
    if not enabled:
        result["reason"] = "disabled"
        return result
    if not calibration_applies(state, continuation, prior, config.PRIOR_EQUIV_MINUTES):
        return result
    amount = model["coefficient"] * correction_factor(model["method"], state, continuation, prior)
    if not math.isfinite(amount) or amount < 0:
        raise ValueError("Invalid calibration correction")
    # A residual learned from final totals may cross the pregame rate. It
    # cannot subtract scored points or create a negative future scoring rate.
    amount = min(amount, state["remaining"] * ppm)
    result.update(applied=amount > 0, future_ppm=ppm - amount / state["remaining"],
                  correction_points=amount, reason="empirical_hot_excess")
    return result
