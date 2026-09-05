"""Basketbol skor ve periyot metinlerini operasyonel amaçlarla ayrıştırır."""

import re


def parse_score(score: str) -> tuple[int | None, int | None]:
    match = re.search(r"(\d+)\s*[-–]\s*(\d+)", score or "")
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def normalize_quarter_scores(value, score: str = "") -> dict:
    """Return a bounded, score-consistent AiScore period-score snapshot."""
    if not isinstance(value, dict):
        return {}
    raw_home = value.get("home")
    raw_away = value.get("away")
    if not isinstance(raw_home, list) or not isinstance(raw_away, list):
        return {}
    if not raw_home or len(raw_home) != len(raw_away):
        return {}

    home: list[int] = []
    away: list[int] = []
    for raw_home_score, raw_away_score in zip(raw_home[:4], raw_away[:4]):
        if isinstance(raw_home_score, bool) or isinstance(raw_away_score, bool):
            return {}
        try:
            home_score = int(raw_home_score)
            away_score = int(raw_away_score)
        except (TypeError, ValueError):
            return {}
        if not 0 <= home_score <= 100 or not 0 <= away_score <= 100:
            return {}
        home.append(home_score)
        away.append(away_score)

    current_home, current_away = parse_score(score)
    if current_home is not None and current_away is not None:
        # The scoreboard cells and headline can update a few seconds apart.
        # Larger differences indicate that an unrelated numeric row was read.
        if abs(sum(home) - current_home) > 4 or abs(sum(away) - current_away) > 4:
            return {}

    try:
        quality = max(0, min(100, int(value.get("quality") or 0)))
    except (TypeError, ValueError):
        quality = 0
    return {
        "home": home,
        "away": away,
        "source": str(value.get("source") or "")[:80],
        "quality": quality,
    }


def game_clock(status: str, match_name: str = "", tournament: str = "") -> dict:
    """Return period and remaining minutes parsed from the raw match status."""
    status_clean = (status or "").strip()
    uses_halves = _uses_halves(match_name, tournament)
    quarter_length = 20 if uses_halves else (12 if _game_minutes(match_name, tournament) == 48 else 10)
    period_count = 2 if uses_halves else 4

    if not status_clean or re.match(r"^OT", status_clean, re.IGNORECASE):
        return {
            "period": None,
            "remaining_min": None,
            "quarter_length": quarter_length,
            "period_count": period_count,
        }

    period = None
    remaining_min = None
    if re.match(r"^HT$", status_clean, re.IGNORECASE):
        period = 1 if uses_halves else 2
        remaining_min = 0.0

    if period is None:
        ended = re.match(r"^(?:Q(\d)|(\d)Q)\s*[-\s]?\s*Ended$", status_clean, re.IGNORECASE)
        if ended:
            period = int(ended.group(1) or ended.group(2))
            remaining_min = 0.0

    if period is None:
        quarter = re.match(
            r"(?:Q(\d)|(\d)Q)\s*[-:\s]?\s*(\d{1,2}):(\d{2})",
            status_clean,
            re.IGNORECASE,
        )
        if quarter:
            period = int(quarter.group(1) or quarter.group(2))
            remaining_min = int(quarter.group(3)) + int(quarter.group(4)) / 60.0

    if period is None:
        ordinal = re.match(
            r"(\d)(?:st|nd|rd|th)\s*[-:\s]?\s*(?:(\d{1,2}):(\d{2}))?",
            status_clean,
            re.IGNORECASE,
        )
        if ordinal:
            period = int(ordinal.group(1))
            if ordinal.group(2) and ordinal.group(3):
                remaining_min = int(ordinal.group(2)) + int(ordinal.group(3)) / 60.0

    if period is None:
        quarter_only = re.match(r"^(?:Q(\d)|(\d)Q)$", status_clean, re.IGNORECASE)
        if quarter_only:
            period = int(quarter_only.group(1) or quarter_only.group(2))

    if period is None:
        half = re.match(r"^(\d)H(?:[-:\s]+(\d{1,2}):(\d{2}))?$", status_clean, re.IGNORECASE)
        if half:
            value = int(half.group(1))
            period = value if uses_halves else (2 if value == 1 else 4)
            if half.group(2) and half.group(3):
                remaining_min = int(half.group(2)) + int(half.group(3)) / 60.0

    if period is None:
        numeric = re.match(r"^([1-4])$", status_clean)
        if numeric:
            period = int(numeric.group(1))

    if period is None:
        numeric_time = re.match(r"([1-4])[-:\s]+(\d{1,2}):(\d{2})", status_clean)
        if numeric_time:
            period = int(numeric_time.group(1))
            remaining_min = int(numeric_time.group(2)) + int(numeric_time.group(3)) / 60.0

    if period is not None and not 1 <= period <= period_count:
        period = None
        remaining_min = None
    elif remaining_min is not None and not 0 <= remaining_min <= quarter_length:
        period = None
        remaining_min = None

    return {
        "period": period,
        "remaining_min": remaining_min,
        "quarter_length": quarter_length,
        "period_count": period_count,
    }


def current_pace_projection(
    score: str,
    status: str,
    match_name: str = "",
    tournament: str = "",
    *,
    quarter_scores: dict | None = None,
) -> dict:
    """Project regulation pace and expose frozen per-period pace details."""
    home_score, away_score = parse_score(score)
    clock = game_clock(status, match_name, tournament)
    period = clock.get("period")
    remaining_min = clock.get("remaining_min")
    quarter_length = clock.get("quarter_length")
    period_count = clock.get("period_count")
    if (
        home_score is None
        or away_score is None
        or period is None
        or remaining_min is None
        or not quarter_length
        or not period_count
    ):
        return {
            "total": None,
            "ppm": None,
            "score_total": None,
            "elapsed_minutes": None,
            "game_minutes": quarter_length * period_count if quarter_length and period_count else None,
            "periods": [],
        }

    elapsed_minutes = (period - 1) * quarter_length + (quarter_length - remaining_min)
    game_minutes = quarter_length * period_count
    score_total = home_score + away_score
    if elapsed_minutes <= 0 or elapsed_minutes > game_minutes:
        projected_total = None
        average_ppm = None
    else:
        raw_average_ppm = score_total / elapsed_minutes
        average_ppm = round(raw_average_ppm, 2)
        projected_total = round(raw_average_ppm * game_minutes, 1)

    normalized_scores = normalize_quarter_scores(quarter_scores, score)
    period_rows = []
    if normalized_scores and average_ppm is not None:
        home_periods = normalized_scores["home"]
        away_periods = normalized_scores["away"]
        visible_period_count = min(period, period_count, len(home_periods), len(away_periods))
        remaining_game_minutes = max(0.0, game_minutes - elapsed_minutes)
        uses_halves = period_count == 2
        for index in range(1, visible_period_count + 1):
            is_current = index == period
            if is_current:
                played_minutes = quarter_length - remaining_min
            else:
                played_minutes = float(quarter_length)
            if played_minutes <= 0:
                ppm = None
                continuation_total = None
            else:
                period_total = home_periods[index - 1] + away_periods[index - 1]
                raw_period_ppm = period_total / played_minutes
                ppm = round(raw_period_ppm, 2)
                continuation_total = round(
                    score_total + raw_period_ppm * remaining_game_minutes,
                    1,
                )
            period_rows.append({
                "period": index,
                "label": f"H{index}" if uses_halves else f"Q{index}",
                "home": home_periods[index - 1],
                "away": away_periods[index - 1],
                "total": home_periods[index - 1] + away_periods[index - 1],
                "played_minutes": round(played_minutes, 2),
                "ppm": ppm,
                "continuation_total": continuation_total,
                "is_current": is_current and remaining_min > 0,
            })
    return {
        "total": projected_total,
        "ppm": average_ppm,
        "score_total": score_total,
        "elapsed_minutes": round(elapsed_minutes, 2),
        "game_minutes": game_minutes,
        "periods": period_rows,
    }


def _game_minutes(match_name: str, tournament: str) -> int:
    text = f"{match_name} {tournament}".upper()
    if "NBA SUMMER LEAGUE" in text:
        return 40
    is_women = any(token in text for token in ("WNBA", "WOMEN", "WOMEN'S", "WOMAN", "KADIN"))
    is_nba = bool(re.search(r"\bNBA\b", text) or "NATIONAL BASKETBALL ASSOCIATION" in text)
    return 48 if is_nba and not is_women else 40


def _uses_halves(match_name: str, tournament: str) -> bool:
    text = f"{match_name} {tournament}".upper()
    is_ncaa = "NCAA" in text
    is_women = any(token in text for token in ("WNBA", "WOMEN", "WOMEN'S", "WOMAN", "KADIN"))
    return is_ncaa and not is_women
