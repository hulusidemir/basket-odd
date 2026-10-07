"""Pure math module for calculating valid pace windows and shrinking them to pregame priors."""

from datetime import datetime, timezone


def _utc_timestamp(value) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def chronological_snapshots(snapshots: list[dict], current_state: dict) -> list[dict]:
    """Exclude clock rollback, transient score dips and pre-correction anchors."""
    current_elapsed = current_state["elapsed_game_seconds"]
    current_score = current_state["total_score"]
    as_of = _utc_timestamp(current_state.get("observed_at"))
    if as_of is not None:
        snapshots = [row for row in snapshots
                     if (recorded := _utc_timestamp(row.get("recorded_at"))) is not None
                     and recorded <= as_of]
    segment_start = 0
    peak_score = -1
    peak_elapsed = -1
    for index, snapshot in enumerate(snapshots):
        elapsed = snapshot["elapsed_game_seconds"]
        score = snapshot["total_score"]
        if elapsed < peak_elapsed or elapsed > current_elapsed:
            continue
        if score < peak_score:
            confirmed = next((later for later in snapshots[index + 1:]
                              if later["elapsed_game_seconds"] > elapsed
                              and later["elapsed_game_seconds"] <= current_elapsed
                              and later["total_score"] >= score), None)
            if confirmed and confirmed["total_score"] < peak_score:
                segment_start = index
                peak_score = score
        else:
            peak_score = score
        peak_elapsed = elapsed

    accepted = []
    for snapshot in snapshots[segment_start:]:
        elapsed = snapshot["elapsed_game_seconds"]
        score = snapshot["total_score"]
        if elapsed > current_elapsed or score > current_score:
            continue
        if accepted and (elapsed < accepted[-1]["elapsed_game_seconds"] or score < accepted[-1]["total_score"]):
            continue
        accepted.append(snapshot)
    return accepted


def get_future_pace_windows(snapshots: list[dict], current_state: dict,
                           pregame_ppm: float, config) -> list[dict]:
    """Describe distinct observed intervals; overlapping intervals are not independent."""
    if not snapshots:
        return []

    windows = {}

    # Extract current state vars
    now_elapsed = current_state.get("elapsed_game_seconds", 0)
    now_score = current_state.get("total_score", 0)
    current_period = current_state.get("period")
    snapshots = chronological_snapshots(snapshots, current_state)
    if not snapshots:
        return []

    def add_window(label, start_second, start_score):
        seconds = now_elapsed - start_second
        points = now_score - start_score
        if seconds <= 0 or points < 0:
            return
        key = (start_second, start_score, now_elapsed, now_score)
        if key in windows:
            windows[key]["labels"].append(label)
            return
        window_minutes = seconds / 60.0
        observed_pace = points / window_minutes
        weight = window_minutes / (window_minutes + config.PRIOR_EQUIV_MINUTES)
        future_pace = pregame_ppm + weight * (observed_pace - pregame_ppm)
        windows[key] = {"labels": [label], "start_second": start_second,
                        "end_second": now_elapsed, "start_score": start_score,
                        "end_score": now_score, "window_minutes": window_minutes,
                        "observed_ppm": observed_pace, "future_ppm": future_pace}

    # WHOLE GAME PACE
    if now_elapsed > 0:
        add_window("whole_game", 0, 0)

    def get_anchor(target_delta_sec: int) -> dict | None:
        target_elapsed = now_elapsed - target_delta_sec
        min_elapsed = now_elapsed - (target_delta_sec * config.ANCHOR_TOLERANCE_MAX_PCT)
        max_elapsed = now_elapsed - (target_delta_sec * config.ANCHOR_TOLERANCE_MIN_PCT)

        best_snapshot = None
        best_diff = float("inf")

        for snap in reversed(snapshots):
            snap_elapsed = snap["elapsed_game_seconds"]
            if min_elapsed <= snap_elapsed <= max_elapsed:
                diff = abs(snap_elapsed - target_elapsed)
                if diff < best_diff:
                    best_diff = diff
                    best_snapshot = snap

        if best_snapshot:
            delta_score = now_score - best_snapshot["total_score"]
            delta_time = now_elapsed - best_snapshot["elapsed_game_seconds"]
            if delta_time > 0 and delta_score >= 0:
                return best_snapshot
        return None

    # RECENT 2M PACE
    p2 = get_anchor(120)
    if p2:
        add_window("recent_2m", p2["elapsed_game_seconds"], p2["total_score"])

    # RECENT 5M PACE
    p5 = get_anchor(300)
    if p5:
        add_window("recent_5m", p5["elapsed_game_seconds"], p5["total_score"])

    # CURRENT QUARTER PACE
    if current_period is not None:
        q_start_snap = None
        for snap in snapshots:
            if snap["period"] == current_period:
                q_start_snap = snap
                break

        if q_start_snap:
            delta_time = now_elapsed - q_start_snap["elapsed_game_seconds"]
            if delta_time >= config.MIN_QUARTER_ELAPSED_SEC:
                delta_score = now_score - q_start_snap["total_score"]
                if delta_score >= 0:
                    add_window("observed_period_segment", q_start_snap["elapsed_game_seconds"],
                               q_start_snap["total_score"])

    return list(windows.values())


def get_future_paces(snapshots: list[dict], current_state: dict, pregame_ppm: float, config) -> list[float]:
    """Count each source interval once, even when it serves multiple windows."""
    return [window["future_ppm"] for window in
            get_future_pace_windows(snapshots, current_state, pregame_ppm, config)]
