def live_total_delta(current_live, previous_live) -> float | None:
    try:
        current = float(current_live)
        previous = float(previous_live)
    except (TypeError, ValueError):
        return None

    return abs(current - previous)
