from dataclasses import asdict, dataclass

from projection import calculate_projected_total, game_clock, parse_score


@dataclass(frozen=True)
class SignalFeatures:
    opening_total: float
    pregame_total: float | None
    current_live_total: float
    reference_total: float
    reference_label: str
    line_delta_vs_open: float
    line_delta_vs_pregame: float | None
    line_delta_vs_reference: float
    abs_line_delta_vs_reference: float
    market_move_direction: str
    continuation_direction: str | None
    contrarian_direction: str | None
    current_total_score: int | None
    current_points_per_minute: float | None
    projected_final_total_naive: float | None
    projection_gap_vs_live_line: float | None
    projection_gap_vs_open: float | None
    recent_points_per_minute: float | None
    recent_sample_minutes: float | None
    recent_points_delta: int | None
    recent_pace_trend: str
    live_line_velocity_per_minute: float | None
    elapsed_minutes: float | None
    remaining_minutes: float | None
    quarter: int | None
    score_diff: int | None
    blowout_risk: float
    foul_game_risk: float
    comeback_risk: float
    volatility_score: float
    possible_market_continuation_flag: bool
    possible_mean_reversion_flag: bool

    def to_dict(self) -> dict:
        return asdict(self)


def _bounded(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def _float_or_none(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_signal_features(
    match: dict,
    *,
    reference_total: float,
    reference_label: str,
    recent_snapshots: list[dict] | None = None,
    blowout_margin_threshold: int = 15,
    late_game_minutes_threshold: float = 6.0,
    foul_game_score_diff_threshold: int = 8,
) -> SignalFeatures:
    opening = float(match["opening_total"])
    live = float(match["inplay_total"])
    pregame = match.get("prematch_total")
    pregame_value = float(pregame) if pregame is not None else None
    status = match.get("status", "")
    score = match.get("score", "")
    match_name = match.get("match_name", "")
    tournament = match.get("tournament", "")

    line_delta_vs_open = round(live - opening, 1)
    line_delta_vs_pregame = round(live - pregame_value, 1) if pregame_value is not None else None
    line_delta_vs_reference = round(live - float(reference_total), 1)
    abs_delta = abs(line_delta_vs_reference)

    if line_delta_vs_reference > 0:
        market_move_direction = "UP"
        continuation_direction = "ÜST"
        contrarian_direction = "ALT"
    elif line_delta_vs_reference < 0:
        market_move_direction = "DOWN"
        continuation_direction = "ALT"
        contrarian_direction = "ÜST"
    else:
        market_move_direction = "FLAT"
        continuation_direction = None
        contrarian_direction = None

    clock = game_clock(status, match_name, tournament)
    quarter = clock["period"]
    remaining_in_period = clock["remaining_min"]
    quarter_length = clock["quarter_length"]
    total_game_min = clock["total_game_min"]

    elapsed = None
    remaining_total = None
    if quarter is not None and remaining_in_period is not None:
        elapsed = round((quarter - 1) * quarter_length + (quarter_length - remaining_in_period), 2)
        remaining_total = round(max(total_game_min - elapsed, 0.0), 2)

    home_score, away_score = parse_score(score)
    current_total_score = None
    score_diff = None
    points_per_minute = None
    if home_score is not None and away_score is not None:
        current_total_score = home_score + away_score
        score_diff = abs(home_score - away_score)
        if elapsed and elapsed > 0:
            points_per_minute = round(current_total_score / elapsed, 3)

    projected = calculate_projected_total(score, status, match_name, tournament)
    projection_gap_vs_live = round(projected - live, 1) if projected is not None else None
    projection_gap_vs_open = round(projected - opening, 1) if projected is not None else None

    recent_ppm = None
    recent_sample = None
    recent_points_delta = None
    recent_pace_trend = "unknown"
    line_velocity = None
    if elapsed is not None and current_total_score is not None:
        candidates = []
        for raw in recent_snapshots or []:
            snap_elapsed = _float_or_none(raw.get("elapsed_minutes"))
            snap_total = _int_or_none(raw.get("total_score"))
            if snap_elapsed is None or snap_total is None:
                continue
            sample_minutes = elapsed - snap_elapsed
            if sample_minutes <= 0.25:
                continue
            if snap_total > current_total_score:
                continue
            candidates.append((sample_minutes, snap_elapsed, snap_total, raw))

        if candidates:
            # Prefer at least a 90-second sample; otherwise use the widest available window.
            wide = [item for item in candidates if item[0] >= 1.5]
            sample_minutes, _, snap_total, snap = max(wide or candidates, key=lambda item: item[0])
            points_delta = current_total_score - snap_total
            if points_delta >= 0:
                recent_sample = round(sample_minutes, 2)
                recent_points_delta = points_delta
                recent_ppm = round(points_delta / sample_minutes, 3)

                previous_live = _float_or_none(snap.get("live"))
                if previous_live is not None:
                    line_velocity = round((live - previous_live) / sample_minutes, 3)

                if points_per_minute is None:
                    recent_pace_trend = "unknown"
                elif recent_ppm >= points_per_minute + 0.65 or recent_ppm >= 4.8:
                    recent_pace_trend = "heating"
                elif recent_ppm <= points_per_minute - 0.65 or recent_ppm <= 2.6:
                    recent_pace_trend = "cooling"
                else:
                    recent_pace_trend = "steady"

    blowout_risk = 0.0
    if score_diff is not None:
        blowout_risk = _bounded(score_diff / max(float(blowout_margin_threshold), 1.0))

    foul_game_risk = 0.0
    if quarter == 4 and remaining_in_period is not None and remaining_in_period <= late_game_minutes_threshold:
        closeness = 1.0
        if score_diff is not None:
            closeness = 1.0 - _bounded(score_diff / max(float(foul_game_score_diff_threshold), 1.0))
        time_pressure = 1.0 - _bounded(remaining_in_period / max(late_game_minutes_threshold, 0.1))
        foul_game_risk = _bounded((closeness * 0.7) + (time_pressure * 0.3))

    comeback_risk = 0.0
    if quarter is not None and quarter >= 3 and score_diff is not None:
        comeback_risk = _bounded((1.0 - _bounded(score_diff / 14.0)) * 0.8 + (0.2 if quarter == 4 else 0.0))

    volatility_score = 0.0
    if elapsed is None or elapsed < 8:
        volatility_score += 0.35
    if projection_gap_vs_live is not None:
        volatility_score += min(abs(projection_gap_vs_live) / 30.0, 0.35)
    if recent_ppm is not None and points_per_minute is not None:
        volatility_score += min(abs(recent_ppm - points_per_minute) / 4.0, 0.25)
    if foul_game_risk:
        volatility_score += foul_game_risk * 0.2
    volatility_score = _bounded(volatility_score)

    projection_abs = abs(projection_gap_vs_live) if projection_gap_vs_live is not None else 0.0
    possible_market_continuation_flag = False
    possible_mean_reversion_flag = False
    if continuation_direction and projection_gap_vs_live is not None:
        if continuation_direction == "ÜST":
            possible_market_continuation_flag = projection_gap_vs_live >= 3
            possible_mean_reversion_flag = projection_gap_vs_live <= -4
        else:
            possible_market_continuation_flag = projection_gap_vs_live <= -3
            possible_mean_reversion_flag = projection_gap_vs_live >= 4
    if projection_abs < 2:
        possible_market_continuation_flag = False
        possible_mean_reversion_flag = False

    return SignalFeatures(
        opening_total=opening,
        pregame_total=pregame_value,
        current_live_total=live,
        reference_total=float(reference_total),
        reference_label=reference_label,
        line_delta_vs_open=line_delta_vs_open,
        line_delta_vs_pregame=line_delta_vs_pregame,
        line_delta_vs_reference=line_delta_vs_reference,
        abs_line_delta_vs_reference=round(abs_delta, 1),
        market_move_direction=market_move_direction,
        continuation_direction=continuation_direction,
        contrarian_direction=contrarian_direction,
        current_total_score=current_total_score,
        current_points_per_minute=points_per_minute,
        projected_final_total_naive=projected,
        projection_gap_vs_live_line=projection_gap_vs_live,
        projection_gap_vs_open=projection_gap_vs_open,
        recent_points_per_minute=recent_ppm,
        recent_sample_minutes=recent_sample,
        recent_points_delta=recent_points_delta,
        recent_pace_trend=recent_pace_trend,
        live_line_velocity_per_minute=line_velocity,
        elapsed_minutes=elapsed,
        remaining_minutes=remaining_total,
        quarter=quarter,
        score_diff=score_diff,
        blowout_risk=round(blowout_risk, 3),
        foul_game_risk=round(foul_game_risk, 3),
        comeback_risk=round(comeback_risk, 3),
        volatility_score=round(volatility_score, 3),
        possible_market_continuation_flag=possible_market_continuation_flag,
        possible_mean_reversion_flag=possible_mean_reversion_flag,
    )
