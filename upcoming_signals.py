"""Independent pre-game analysis; never writes live alerts or Telegram messages."""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import unicodedata


HISTORY_LIMIT = 10


def normalize_name(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def _number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        return None
    return result if result.is_finite() else None


def team_attack_average(team: str, matches: list[dict], *, before: str,
                        current_match_id: str = "", limit: int = HISTORY_LIMIT) -> dict:
    """Pick disjoint low-three/high-two games from recent verified finals."""
    valid = []
    seen = set()
    for match in matches:
        match_id = str(match.get("match_id") or "")
        played = str(match.get("date") or "")[:10].replace("/", "-")
        try:
            datetime.strptime(played, "%Y-%m-%d")
        except ValueError:
            continue
        try:
            cutoff = datetime.fromisoformat(before)
            if match.get("played_at"):
                started = datetime.fromisoformat(match["played_at"])
                if started.tzinfo is None:
                    continue
                cutoff = cutoff.replace(tzinfo=started.tzinfo) if cutoff.tzinfo is None else cutoff
                if started >= cutoff or started > datetime.now(timezone.utc):
                    continue
            # Date-only source cannot establish ordering for a same-day match.
            elif played >= cutoff.date().isoformat() or played > datetime.now(timezone.utc).date().isoformat():
                continue
        except (TypeError, ValueError):
            continue
        if not match_id or match_id in seen or match_id == current_match_id:
            continue
        if match.get("status") != "FT":
            continue
        home = _number(match.get("home_score"))
        away = _number(match.get("away_score"))
        if (home is None or away is None or min(home, away) < 0
                or home + away <= 0 or home != int(home) or away != int(away)):
            continue
        key = normalize_name(team)
        if not key:
            continue
        if key == normalize_name(match.get("home_team") or ""):
            points = home
        elif key == normalize_name(match.get("away_team") or ""):
            points = away
        else:
            continue
        seen.add(match_id)
        valid.append({**match, "date": played, "points": int(points)})
    recent = sorted(valid, key=lambda row: (row["date"], row.get("played_at") or "", row["match_id"]),
                    reverse=True)[:limit]
    result = {"team": team, "sample_count": len(recent), "average": None,
              "selected_matches": [], "status": "insufficient_history"}
    if len(recent) < 5:
        return result
    ranked = sorted(recent, key=lambda row: (row["points"], row["date"], row["match_id"]))
    selected = [{**row, "selection": "low"} for row in ranked[:3]]
    selected += [{**row, "selection": "high"} for row in ranked[-2:]]
    average = Decimal(sum(row["points"] for row in selected)) / 5
    return {**result, "average": float(average), "selected_matches": selected, "status": "ready"}


def analyze_upcoming(match: dict, histories: dict, *, limit: int = HISTORY_LIMIT) -> dict:
    """Compare the selected attacks to the opening, without pre-match fallback."""
    teams = [team_attack_average(match.get(f"{side}_team") or "", histories.get(side) or [],
                                 before=match.get("kickoff") or "",
                                 current_match_id=match.get("match_id") or "", limit=limit)
             for side in ("home", "away")]
    result = {"version": 1, "history_limit": limit,
              "calculated_at": datetime.now(timezone.utc).isoformat(),
              "home": teams[0], "away": teams[1], "estimated_total": None,
              "opening_total": None, "edge": None, "signal": None,
              "status": "insufficient_history"}
    opening = _number(match.get("opening_total"))
    if opening is not None and opening > 0:
        result["opening_total"] = float(opening)
    if any(team["average"] is None for team in teams):
        return result
    total = sum(Decimal(str(team["average"])) for team in teams)
    result["estimated_total"] = float(total)
    if opening is None or opening <= 0:
        return {**result, "status": "missing_opening"}
    edge = total - opening
    return {**result, "edge": float(edge), "signal": "ALT" if edge < 0 else "ÜST" if edge > 0 else None,
            "status": "ready" if edge else "equal"}
