import re


def parse_score(score: str) -> tuple[int | None, int | None]:
    match = re.search(r"(\d+)\s*[-–]\s*(\d+)", score or "")
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def game_clock(status: str, match_name: str = "", tournament: str = "") -> dict:
    status_clean = (status or "").strip()
    total_game_min = game_minutes(match_name, tournament)

    if not status_clean or re.match(r"^OT", status_clean, re.IGNORECASE):
        return {
            "period": None,
            "remaining_min": None,
            "quarter_length": total_game_min / 4,
            "total_game_min": total_game_min,
        }

    quarter_length = 20 if total_game_min == 40 and _is_ncaa(match_name, tournament) else (
        12 if total_game_min == 48 else 10
    )

    period = None
    remaining_min = None

    if re.match(r"^HT$", status_clean, re.IGNORECASE):
        period = 1 if quarter_length == 20 else 2
        remaining_min = 0.0

    if period is None:
        q_match = re.match(
            r"(?:Q(\d)|(\d)Q)\s*[-:\s]?\s*(\d{1,2}):(\d{2})",
            status_clean,
            re.IGNORECASE,
        )
        if q_match:
            period = int(q_match.group(1) or q_match.group(2))
            remaining_min = int(q_match.group(3)) + int(q_match.group(4)) / 60.0

    if period is None:
        ord_match = re.match(
            r"(\d)(?:st|nd|rd|th)\s*[-:\s]?\s*(?:(\d{1,2}):(\d{2}))?",
            status_clean,
            re.IGNORECASE,
        )
        if ord_match:
            period = int(ord_match.group(1))
            if ord_match.group(2) and ord_match.group(3):
                remaining_min = int(ord_match.group(2)) + int(ord_match.group(3)) / 60.0
            else:
                remaining_min = quarter_length / 2

    if period is None:
        q_only = re.match(r"^(?:Q(\d)|(\d)Q)$", status_clean, re.IGNORECASE)
        if q_only:
            period = int(q_only.group(1) or q_only.group(2))
            remaining_min = quarter_length / 2

    if period is None:
        h_match = re.match(r"^(\d)H(?:[-:\s]+(\d{1,2}):(\d{2}))?$", status_clean, re.IGNORECASE)
        if h_match:
            half = int(h_match.group(1))
            period = half if quarter_length == 20 else (2 if half == 1 else 4)
            if h_match.group(2) and h_match.group(3):
                remaining_min = int(h_match.group(2)) + int(h_match.group(3)) / 60.0
            else:
                remaining_min = quarter_length / 2

    if period is None:
        d_match = re.match(r"^([1-4])$", status_clean)
        if d_match:
            period = int(d_match.group(1))
            remaining_min = quarter_length / 2

    if period is None:
        nt_match = re.match(r"([1-4])[-:\s]+(\d{1,2}):(\d{2})", status_clean)
        if nt_match:
            period = int(nt_match.group(1))
            remaining_min = int(nt_match.group(2)) + int(nt_match.group(3)) / 60.0

    return {
        "period": period,
        "remaining_min": remaining_min,
        "quarter_length": quarter_length,
        "total_game_min": total_game_min,
    }


def game_minutes(match_name: str = "", tournament: str = "") -> int:
    text_to_check = f"{match_name} {tournament}".upper()
    if "NBA" in text_to_check:
        return 48
    return 40


def _is_ncaa(match_name: str = "", tournament: str = "") -> bool:
    return "NCAA" in f"{match_name} {tournament}".upper()


def calculate_projected_total(score: str, status: str, match_name: str = "", tournament: str = "") -> float | None:
    home_score, away_score = parse_score(score)
    if home_score is None or away_score is None:
        return None

    clock = game_clock(status, match_name, tournament)
    period = clock["period"]
    remaining_min = clock["remaining_min"]
    quarter_length = clock["quarter_length"]
    total_game_min = clock["total_game_min"]

    if period is None or remaining_min is None:
        return None

    elapsed_min = (period - 1) * quarter_length + (quarter_length - remaining_min)
    if elapsed_min <= 1:
        return None

    return round(((home_score + away_score) / elapsed_min) * total_game_min, 1)
