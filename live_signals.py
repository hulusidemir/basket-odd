"""Continuous total forecasts, with alert eligibility separate from direction."""

import math
import re
from dataclasses import dataclass

from match_state import game_clock, parse_score
from pace_calculator import get_future_paces, get_over_continuation
from over_calibration import calibrate_over_continuation
from win_probability import assess_forecast


ENGINE_VERSION = "future_pace_v10"


def valid_total(value) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and 0 < number <= 1000 else None


@dataclass(frozen=True)
class SignalDecision:
    reference_used: str
    reference_total: float
    diff: float
    effective_threshold: float
    direction: str
    period: int | None
    skip_reason: str = ""

    # v5 Extended fields
    market_implied_pace: float | None = None
    pace_lower_bound: float | None = None
    pace_upper_bound: float | None = None
    line_move_ratio: float | None = None
    edge_points: float | None = None
    pregame_ppm: float | None = None
    sustainable_projection_center: float | None = None
    over_continuation: dict | None = None
    over_calibration: dict | None = None
    win_probability: dict | None = None
    required_edge_points: float | None = None
    base_predicted_total: float | None = None


def direction_for_total(center: float, line: float) -> str:
    """An equal estimate carries no directional advantage."""
    return "ÜST" if center > line else "ALT" if center < line else "EŞİT"


def forecast_live_total(match: dict, config, snapshots: list[dict] | None = None) -> dict | None:
    """Keep one total for every line; hot scoring requires continuation support."""
    reference = valid_total(match.get("prematch_total")) or valid_total(match.get("opening_total"))
    line = valid_total(match.get("inplay_total"))
    clock = game_clock(match.get("status", ""), match.get("match_name", ""), match.get("tournament", ""))
    home, away = parse_score(match.get("score", ""))
    if (reference is None or line is None or home is None or away is None
            or clock.get("period") is None or clock.get("remaining_min") is None
            or not clock.get("quarter_length") or not clock.get("period_count")):
        return None
    duration = clock["quarter_length"] * clock["period_count"]
    elapsed = (clock["period"] - 1) * clock["quarter_length"] + clock["quarter_length"] - clock["remaining_min"]
    if not 0 <= elapsed <= duration:
        return None
    prior_minutes = float(config.PRIOR_EQUIV_MINUTES)
    if elapsed + prior_minutes <= 0:
        return None
    score = home + away
    pregame_ppm = reference / duration
    future_ppm = (score + prior_minutes * pregame_ppm) / (elapsed + prior_minutes)
    remaining = duration - elapsed
    continuation = get_over_continuation(snapshots, {
        "elapsed_game_seconds": round(elapsed * 60), "total_score": score,
        "period": clock["period"], "quarter_length": clock["quarter_length"],
        "period_count": clock["period_count"],
        "observed_at": match.get("market_captured_at") or (match.get("market_provenance") or {}).get("captured_at"),
    }, pregame_ppm, future_ppm, config)
    raw_center = score + future_ppm * remaining
    calibration = calibrate_over_continuation(continuation, {
        "duration": duration, "elapsed": elapsed, "remaining": remaining,
    }, pregame_ppm, config)
    future_ppm = calibration["future_ppm"]
    center = score + future_ppm * remaining
    continuation.update(raw_predicted_total=raw_center,
                        correction_points=raw_center - score - continuation["future_ppm"] * remaining)
    forecast = {
        "engine": ENGINE_VERSION, "base_engine": "future_pace_v9", "method": "outcome_distribution",
        "base_predicted_total": center, "score_total": score,
        "predicted_total": center, "line": line,
        "direction": direction_for_total(center, line), "signed_edge_points": center - line,
        "future_ppm": future_ppm, "pregame_ppm": pregame_ppm,
        "prior_equivalent_minutes": prior_minutes,
        "elapsed_minutes": elapsed, "remaining_minutes": remaining,
        "scope": "regulation", "probability_calibrated": False,
        "over_continuation": continuation,
        "over_calibration": calibration,
    }
    forecast["win_probability"] = assess_forecast(forecast)
    probability = forecast["win_probability"]
    if probability.get("probability") is not None:
        # The point total and direction use the same error-corrected outcome
        # distribution. Stored v9 totals remain the base, preventing double bias.
        forecast["predicted_total"] = max(score, probability["expected_total"])
        forecast["future_ppm"] = (forecast["predicted_total"] - score) / remaining
        forecast["signed_edge_points"] = forecast["predicted_total"] - line
        forecast["direction"] = probability["preferred_direction"]
        forecast["win_probability"] = assess_forecast(forecast)
    return forecast


def evaluate_live_signal(match: dict, snapshots: list[dict], config) -> SignalDecision:
    """Keep a point forecast even when its advantage is too small for an alert."""
    prematch = valid_total(match.get("prematch_total"))
    reference_used = "prematch" if prematch is not None else "opening"
    reference = valid_total(match.get("opening_total")) if prematch is None else prematch

    inplay_total = valid_total(match.get("inplay_total"))

    if reference is None or inplay_total is None:
        return SignalDecision("opening", 0, 0, 0.0, "PAS", None, "missing_totals")

    diff = inplay_total - reference
    line_move_ratio = (inplay_total / reference) - 1 if reference > 0 else 0

    clock = game_clock(match["status"], match["match_name"], match["tournament"])
    period = clock["period"]

    if re.search(r"\b(?:OT\d*|\d+OT|overtime|uzatma)\b", match["status"], re.IGNORECASE):
        return SignalDecision(reference_used, reference, diff, 0.0, "PAS", period, "overtime_disabled")

    if period is None:
        return SignalDecision(reference_used, reference, diff, 0.0, "PAS", period, "unknown_period")

    quarter_length = clock.get("quarter_length")
    period_count = clock.get("period_count")
    remaining_min = clock.get("remaining_min")

    if not quarter_length or not period_count or remaining_min is None:
        return SignalDecision(reference_used, reference, diff, 0.0, "PAS", period, "missing_clock_data")

    regulation_minutes = period_count * quarter_length
    pregame_ppm = reference / regulation_minutes

    elapsed_minutes = (period - 1) * quarter_length + (quarter_length - remaining_min)
    remaining_total_minutes = regulation_minutes - elapsed_minutes

    if elapsed_minutes < getattr(config, "MIN_ELAPSED_MINUTES", 12.0):
        return SignalDecision(reference_used, reference, diff, 0.0, "PAS", period, "PAS_EARLY_GAME")

    if remaining_total_minutes < getattr(config, "MIN_REMAINING_MINUTES", 5.0) or remaining_total_minutes <= 0:
        return SignalDecision(reference_used, reference, diff, 0.0, "PAS", period, "PAS_LATE_GAME")

    home_score, away_score = parse_score(match.get("score", ""))
    if home_score is None or away_score is None:
        return SignalDecision(reference_used, reference, diff, 0.0, "PAS", period, "missing_score")

    current_score = home_score + away_score
    if current_score >= inplay_total:
        return SignalDecision(reference_used, reference, diff, 0.0, "PAS", period, "PAS_STALE_DATA_SCORE_HIGH")

    market_future_pace = (inplay_total - current_score) / remaining_total_minutes

    current_state = {
        "elapsed_game_seconds": round(elapsed_minutes * 60),
        "total_score": current_score,
        "period": period,
        "observed_at": match.get("market_captured_at") or (match.get("market_provenance") or {}).get("captured_at"),
    }

    future_paces = get_future_paces(snapshots, current_state, pregame_ppm, config)

    forecast = forecast_live_total(match, config, snapshots)
    if forecast is None:
        return SignalDecision(reference_used, reference, diff, 0.0, "PAS", period, "missing_forecast")
    # These intervals describe sensitivity, not a unanimous voting/veto system.
    future_pace_lower = min(future_paces, default=forecast["future_ppm"])
    future_pace_upper = max(future_paces, default=forecast["future_ppm"])

    required_edge_points = max(config.MIN_EDGE_POINTS, inplay_total * config.MIN_EDGE_RATIO)

    score_diff = abs(home_score - away_score)
    if elapsed_minutes >= (regulation_minutes / 2.0):
        if score_diff >= config.EXTREME_BLOWOUT_MARGIN:
            required_edge_points *= config.EXTREME_BLOWOUT_EDGE_MULTIPLIER
        elif score_diff >= config.BLOWOUT_MARGIN:
            required_edge_points *= config.BLOWOUT_EDGE_MULTIPLIER

    if abs(line_move_ratio) >= config.LARGE_REPRICE_RATIO:
        required_edge_points *= config.LARGE_REPRICE_EDGE_MULTIPLIER

    signed_edge = forecast["signed_edge_points"]

    direction = "PAS"
    reason = "PAS_NO_EDGE"
    edge = 0.0

    if forecast["direction"] == "ÜST" and signed_edge >= required_edge_points:
        direction = "ÜST"
        edge = signed_edge
        reason = ""
    elif forecast["direction"] == "ALT" and -signed_edge >= required_edge_points:
        direction = "ALT"
        edge = -signed_edge
        reason = ""

    sustainable_projection = forecast["predicted_total"]

    return SignalDecision(
        reference_used=reference_used,
        reference_total=reference,
        diff=diff,
        effective_threshold=0.0,
        direction=direction,
        period=period,
        skip_reason=reason if direction == "PAS" else "",
        market_implied_pace=round(market_future_pace, 2),
        pace_lower_bound=round(future_pace_lower, 2),
        pace_upper_bound=round(future_pace_upper, 2),
        line_move_ratio=round(line_move_ratio, 3),
        edge_points=round(edge, 1),
        pregame_ppm=round(pregame_ppm, 2),
        sustainable_projection_center=round(sustainable_projection, 1),
        over_continuation=forecast["over_continuation"],
        over_calibration=forecast["over_calibration"],
        win_probability=forecast["win_probability"],
        required_edge_points=required_edge_points,
        base_predicted_total=forecast["base_predicted_total"],
    )
