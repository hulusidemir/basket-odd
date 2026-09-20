"""Future Pace Engine v5 for live total-line signals."""

import math
import re
import statistics
from dataclasses import dataclass

from match_state import game_clock, parse_score
from pace_calculator import get_future_paces


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


def evaluate_live_signal(match: dict, snapshots: list[dict], config) -> SignalDecision:
    """Evaluate Future Pace Engine v5 rules."""
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
        "elapsed_game_seconds": int(elapsed_minutes * 60),
        "total_score": current_score,
        "period": period,
    }

    future_paces = get_future_paces(snapshots, current_state, pregame_ppm, config)

    if len(future_paces) < config.MIN_VALID_FUTURE_PACES:
        return SignalDecision(reference_used, reference, diff, 0.0, "PAS", period, "PAS_INSUFFICIENT_FUTURE_PACE")

    future_pace_lower = min(future_paces)
    future_pace_upper = max(future_paces)

    future_band_width = future_pace_upper - future_pace_lower
    if future_band_width > config.MAX_FUTURE_BAND_WIDTH:
        return SignalDecision(reference_used, reference, diff, 0.0, "PAS", period, "PAS_VOLATILE_REGIME")

    if future_pace_lower <= market_future_pace <= future_pace_upper:
        return SignalDecision(reference_used, reference, diff, 0.0, "PAS", period, "PAS_MARKET_INSIDE_PACE_BAND")

    required_edge_points = max(config.MIN_EDGE_POINTS, inplay_total * config.MIN_EDGE_RATIO)

    score_diff = abs(home_score - away_score)
    if elapsed_minutes >= (regulation_minutes / 2.0):
        if score_diff >= config.EXTREME_BLOWOUT_MARGIN:
            required_edge_points *= config.EXTREME_BLOWOUT_EDGE_MULTIPLIER
        elif score_diff >= config.BLOWOUT_MARGIN:
            required_edge_points *= config.BLOWOUT_EDGE_MULTIPLIER

    if abs(line_move_ratio) >= config.LARGE_REPRICE_RATIO:
        required_edge_points *= config.LARGE_REPRICE_EDGE_MULTIPLIER

    over_edge_points = (future_pace_lower - market_future_pace) * remaining_total_minutes
    under_edge_points = (market_future_pace - future_pace_upper) * remaining_total_minutes

    direction = "PAS"
    reason = "PAS_NO_EDGE"
    edge = 0.0

    if market_future_pace < future_pace_lower and over_edge_points >= required_edge_points:
        direction = "ÜST"
        edge = over_edge_points
        reason = ""
    elif market_future_pace > future_pace_upper and under_edge_points >= required_edge_points:
        direction = "ALT"
        edge = under_edge_points
        reason = ""

    future_pace_center = statistics.median(future_paces)
    sustainable_projection = current_score + (future_pace_center * remaining_total_minutes)

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
        sustainable_projection_center=round(sustainable_projection, 1)
    )
