"""Pure math module for calculating valid pace windows and shrinking them to pregame priors."""


def chronological_snapshots(snapshots: list[dict], current_state: dict) -> list[dict]:
    """Exclude clock rollback, transient score dips and pre-correction anchors."""
    current_elapsed = current_state["elapsed_game_seconds"]
    current_score = current_state["total_score"]
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


def get_future_paces(snapshots: list[dict], current_state: dict, pregame_ppm: float, config) -> list[float]:
    """Calculate all valid future paces: recent 2m, recent 5m, current quarter, whole game shrunk to prior."""
    if not snapshots:
        return []

    future_paces = []

    # Extract current state vars
    now_elapsed = current_state.get("elapsed_game_seconds", 0)
    now_score = current_state.get("total_score", 0)
    current_period = current_state.get("period")
    snapshots = chronological_snapshots(snapshots, current_state)

    def add_future_pace(observed_pace: float, window_minutes: float):
        weight = window_minutes / (window_minutes + config.PRIOR_EQUIV_MINUTES)
        future_pace = pregame_ppm + weight * (observed_pace - pregame_ppm)
        future_paces.append(future_pace)

    # WHOLE GAME PACE
    if now_elapsed > 0:
        window_min = now_elapsed / 60.0
        whole_game_pace = now_score / window_min
        add_future_pace(whole_game_pace, window_min)

    def get_anchor_pace(target_delta_sec: int) -> tuple[float, float] | None:
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
                return (delta_score / (delta_time / 60.0), delta_time / 60.0)
        return None

    # RECENT 2M PACE
    p2 = get_anchor_pace(120)
    if p2:
        add_future_pace(p2[0], p2[1])

    # RECENT 5M PACE
    p5 = get_anchor_pace(300)
    if p5:
        add_future_pace(p5[0], p5[1])

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
                    add_future_pace(delta_score / (delta_time / 60.0), delta_time / 60.0)

    return future_paces
