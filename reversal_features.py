"""Freeze observable tempo and market facts without changing signal decisions."""

from datetime import datetime, timedelta, timezone

from live_signals import valid_total
from match_state import game_clock, parse_score, quarter_clock_seconds
from pace_calculator import chronological_snapshots


MAX_SNAPSHOT_AGE = timedelta(minutes=20)


def _recorded_at(value) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    return numerator / denominator if numerator is not None and denominator is not None and denominator > 0 else None


def _trusted_snapshots(
    snapshots: list[dict], current_state: dict, quarter_length: int,
    period_count: int, as_of: datetime,
) -> list[dict]:
    eligible = []
    for row in snapshots:
        recorded = _recorded_at(row.get("recorded_at"))
        if recorded is None or recorded > as_of or as_of - recorded > MAX_SNAPSHOT_AGE:
            continue
        period = row.get("period")
        clock_text = row.get("game_clock")
        if period_count == 4 and period in (1, 2, 3, 4) and clock_text:
            remaining_seconds = quarter_clock_seconds(f"Q{period} {clock_text}")
            if remaining_seconds is None or remaining_seconds > quarter_length * 60:
                continue
            expected = (period - 1) * quarter_length * 60 + quarter_length * 60 - remaining_seconds
            if abs(expected - row["elapsed_game_seconds"]) > 1:
                continue
        eligible.append(row)
    return chronological_snapshots(eligible, current_state)


def _anchor(rows: list[dict], endpoint_second: int, low: int, high: int, target: int) -> dict | None:
    candidates = [row for row in rows if low <= endpoint_second - row["elapsed_game_seconds"] <= high]
    return min(candidates, key=lambda row: abs(endpoint_second - row["elapsed_game_seconds"] - target)) if candidates else None


def _window(start: dict, end_second: int, end_score: int, rows: list[dict]) -> tuple[float, int, float, int]:
    seconds = end_second - start["elapsed_game_seconds"]
    points = end_score - start["total_score"]
    minutes = seconds / 60
    count = sum(start["elapsed_game_seconds"] <= row["elapsed_game_seconds"] <= end_second for row in rows)
    return minutes, points, points / minutes, count


def collect_reversal_features(
    match: dict, snapshots: list[dict], decision, *, as_of: datetime | None = None,
    observation_age_seconds: float | None = None,
) -> dict:
    """Build signal-time features; missing history stays null and never changes direction."""
    as_of = as_of or datetime.now(timezone.utc)
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)
    else:
        as_of = as_of.astimezone(timezone.utc)

    opening = valid_total(match.get("opening_total"))
    prematch = valid_total(match.get("prematch_total"))
    live = valid_total(match.get("inplay_total"))
    pregame = prematch if prematch is not None else opening
    clock = game_clock(match.get("status", ""), match.get("match_name", ""), match.get("tournament", ""))
    quarter_length = clock.get("quarter_length")
    period_count = clock.get("period_count")
    period = clock.get("period")
    remaining_period = clock.get("remaining_min")
    regulation = quarter_length * period_count if quarter_length and period_count else None
    elapsed = ((period - 1) * quarter_length + quarter_length - remaining_period
               if period and quarter_length and remaining_period is not None else None)
    remaining = regulation - elapsed if regulation is not None and elapsed is not None else None
    home, away = parse_score(match.get("score", ""))
    score = home + away if home is not None and away is not None else None
    current_ppm = _safe_ratio(score, elapsed)
    pregame_ppm = _safe_ratio(pregame, regulation)
    required_ppm = _safe_ratio(live - score, remaining) if live is not None and score is not None else None
    current_gap = current_ppm - required_ppm if current_ppm is not None and required_ppm is not None else None
    fair = decision.sustainable_projection_center
    market_captured = _recorded_at(match.get("market_captured_at"))

    result = {
        "opening_to_prematch_move": prematch - opening if prematch is not None and opening is not None else None,
        "prematch_to_live_move": live - prematch if live is not None and prematch is not None else None,
        "opening_to_live_move": live - opening if live is not None and opening is not None else None,
        "pregame_source": "prematch" if prematch is not None else "opening" if opening is not None else None,
        "pregame_ppm": pregame_ppm,
        "current_ppm": current_ppm,
        "current_vs_pregame_ppm": current_ppm - pregame_ppm if current_ppm is not None and pregame_ppm is not None else None,
        "current_vs_pregame_pct": current_ppm / pregame_ppm - 1 if current_ppm is not None and pregame_ppm else None,
        "recent_ppm": None,
        "recent_window_minutes": None,
        "recent_window_points": None,
        "recent_snapshot_count": None,
        "previous_ppm": None,
        "previous_window_minutes": None,
        "previous_window_points": None,
        "previous_snapshot_count": None,
        "pace_delta": None,
        "pace_delta_pct": None,
        "required_ppm": required_ppm,
        "current_required_ppm_gap": current_gap,
        "current_required_ppm_gap_pct": current_gap / required_ppm if current_gap is not None and required_ppm and required_ppm > 0 else None,
        "fair_edge": fair - live if fair is not None and live is not None else None,
        "projection": current_ppm * regulation if current_ppm is not None and regulation is not None else None,
        "regulation_minutes": regulation,
        "period": period,
        "elapsed_minutes": elapsed,
        "remaining_minutes": remaining,
        "elapsed_fraction": elapsed / regulation if elapsed is not None and regulation and regulation > 0 else None,
        "reversal_feature_status": "INSUFFICIENT_HISTORY",
        "trusted_snapshot_count": 0,
        "observation_age_seconds": observation_age_seconds,
        "market_captured_at": market_captured.isoformat() if market_captured else None,
        "feature_frozen_at": as_of.isoformat(),
    }

    if score is None or elapsed is None or elapsed <= 0 or remaining is None or remaining <= 0:
        return result
    now_second = round(elapsed * 60)
    state = {"elapsed_game_seconds": now_second, "total_score": score, "period": period}
    trusted = _trusted_snapshots(snapshots, state, quarter_length, period_count, as_of)
    result["trusted_snapshot_count"] = len(trusted)
    recent = _anchor(trusted, now_second, 120, 180, 150)
    if recent is None or recent["total_score"] > score:
        return result
    recent_minutes, recent_points, recent_ppm, recent_count = _window(recent, now_second, score, trusted)
    result.update(recent_ppm=recent_ppm, recent_window_minutes=recent_minutes,
                  recent_window_points=recent_points, recent_snapshot_count=recent_count,
                  reversal_feature_status="RECENT_ONLY")

    previous = _anchor(trusted, recent["elapsed_game_seconds"], 180, 300, 240)
    if previous is None or previous["total_score"] > recent["total_score"]:
        return result
    previous_minutes, previous_points, previous_ppm, previous_count = _window(
        previous, recent["elapsed_game_seconds"], recent["total_score"], trusted,
    )
    result.update(previous_ppm=previous_ppm, previous_window_minutes=previous_minutes,
                  previous_window_points=previous_points, previous_snapshot_count=previous_count,
                  pace_delta=recent_ppm - previous_ppm,
                  pace_delta_pct=recent_ppm / previous_ppm - 1 if previous_ppm > 0 else None,
                  reversal_feature_status="FULL")
    return result
