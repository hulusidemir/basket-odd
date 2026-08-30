"""Basketbol skor ve periyot metinlerini operasyonel amaçlarla ayrıştırır."""

import re


def parse_score(score: str) -> tuple[int | None, int | None]:
    match = re.search(r"(\d+)\s*[-–]\s*(\d+)", score or "")
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


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
) -> dict:
    """Project the regulation total if the current scoring pace continues."""
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
            "score_total": None,
            "elapsed_minutes": None,
            "game_minutes": quarter_length * period_count if quarter_length and period_count else None,
        }

    elapsed_minutes = (period - 1) * quarter_length + (quarter_length - remaining_min)
    game_minutes = quarter_length * period_count
    score_total = home_score + away_score
    if elapsed_minutes <= 0 or elapsed_minutes > game_minutes:
        projected_total = None
    else:
        projected_total = round(score_total / elapsed_minutes * game_minutes, 1)
    return {
        "total": projected_total,
        "score_total": score_total,
        "elapsed_minutes": round(elapsed_minutes, 2),
        "game_minutes": game_minutes,
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
