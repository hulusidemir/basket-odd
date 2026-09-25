"""Deterministic, signal-time Quality v1 score; not a win probability."""

from live_signals import SignalDecision, valid_total
from match_state import game_clock, parse_score


FAIR_EDGE_MAX_POINTS = 15.0
PACE_SUPPORT_MAX_RATIO = 0.20
MARKET_MOVE_MAX_POINTS = 15.0


def _clamp(value: float, maximum: float) -> float:
    return max(0.0, min(maximum, value))


def quality_label(score: int) -> str:
    if score < 40:
        return "PAS"
    if score < 55:
        return "DÜŞÜK"
    if score < 70:
        return "ORTA"
    if score < 85:
        return "YÜKSEK"
    return "ÇOK YÜKSEK"


def score_signal_quality(
    match: dict, decision: SignalDecision, config, *, repeated: bool = False,
    observation_age_seconds: float | None = None,
) -> tuple[int, str, dict]:
    clock = game_clock(match["status"], match["match_name"], match["tournament"])
    home, away = parse_score(match.get("score", ""))
    elapsed = None
    remaining = None
    if (clock["period"] and clock.get("quarter_length") and clock.get("period_count")
            and clock.get("remaining_min") is not None):
        elapsed = (clock["period"] - 1) * clock["quarter_length"] + clock["quarter_length"] - clock["remaining_min"]
        remaining = clock["period_count"] * clock["quarter_length"] - elapsed

    live = valid_total(match.get("inplay_total"))
    fair = decision.sustainable_projection_center
    edge = ((live - fair) if decision.direction == "ALT" else (fair - live)) if live is not None and fair is not None else 0
    if decision.direction == "ÜST":
        minimum_edge = config.OVER_MIN_FAIR_EDGE_POINTS
        fair_edge = round(30 * _clamp(
            (edge - minimum_edge) / max(FAIR_EDGE_MAX_POINTS - minimum_edge, 0.001), 1,
        ))
    else:
        fair_edge = round(30 * _clamp(edge / FAIR_EDGE_MAX_POINTS, 1))

    required = decision.market_implied_pace
    current = (home + away) / elapsed if home is not None and away is not None and elapsed and elapsed > 0 else None
    pace_ratio = 0.0
    if required is not None and required > 0 and current is not None:
        pace_ratio = ((required - current) if decision.direction == "ALT" else (current - required)) / required
    if decision.direction == "ÜST":
        minimum_margin = (config.OVER_Q2_MIN_PACE_MARGIN_RATIO
                          if clock["period"] == 2 and clock.get("period_count") == 4
                          else config.OVER_MIN_PACE_MARGIN_RATIO)
        pace_support = round(25 * _clamp(
            (pace_ratio - minimum_margin) / max(PACE_SUPPORT_MAX_RATIO - minimum_margin, 0.001), 1,
        ))
    else:
        pace_support = round(25 * _clamp(pace_ratio / PACE_SUPPORT_MAX_RATIO, 1))

    move = live - decision.reference_total if live is not None else 0
    if decision.direction == "ALT":
        negative_limit = config.UNDER_MAX_NEGATIVE_LINE_MOVE
        if move <= -negative_limit:
            market_move = -45 - round(15 * _clamp((-move - negative_limit) / MARKET_MOVE_MAX_POINTS, 1))
        else:
            market_move = round(20 * _clamp((move + negative_limit) / (MARKET_MOVE_MAX_POINTS + negative_limit), 1))
    else:
        market_move = round(20 * (1 - _clamp(move / MARKET_MOVE_MAX_POINTS, 1)))

    game_state = 0
    if elapsed is not None and remaining is not None:
        duration = elapsed + remaining
        game_state = 7 if elapsed < duration * 0.35 else 12 if elapsed < duration * 0.75 else 15
        score_diff = abs(home - away) if home is not None and away is not None else 0
        if decision.direction == "ALT" and remaining <= 5 and score_diff <= 8:
            game_state -= 6
        if decision.direction == "ÜST" and clock["period"] == 4 and score_diff >= config.BLOWOUT_MARGIN:
            game_state -= 6
    game_state = int(_clamp(game_state, 15))

    data_quality = 10 if live is not None and home is not None and away is not None and elapsed is not None else 0
    if observation_age_seconds is not None and observation_age_seconds > 10:
        data_quality = max(0, data_quality - 3)
    penalty = -config.REPEAT_SIGNAL_QUALITY_PENALTY if repeated else 0
    factors = {
        "fair_edge": fair_edge,
        "pace_support": pace_support,
        "market_move": market_move,
        "game_state": game_state,
        "data_quality": data_quality,
        "repeat_penalty": penalty,
    }
    score = round(_clamp(sum(factors.values()), 100))
    return score, quality_label(score), factors
