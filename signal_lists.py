from db import Database
from projection import game_clock, parse_score


def _normalize_direction(value) -> str:
    text = str(value or "").strip().upper().replace("UST", "ÜST")
    return text if text in {"ALT", "ÜST"} else (text or "-")


def _safe_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _is_blacklisted_league(alert: dict, list_profile: dict | None) -> bool:
    if not isinstance(list_profile, dict):
        return False
    tournament = str(alert.get("tournament") or "").strip()
    normalized = Database.normalize_signal_list_value(tournament)
    return bool(normalized and normalized in ((list_profile.get("sets") or {}).get("black", {}).get("league") or set()))


def _early_tempo_tag(alert: dict, list_profile: dict | None) -> dict | None:
    if _normalize_direction(alert.get("direction")) != "ALT":
        return None
    opening = _safe_float(alert.get("opening"))
    live = _safe_float(alert.get("live"))
    fair = _safe_float(alert.get("fair_line"))
    projected = _safe_float(alert.get("projected_total"))
    if projected is None:
        projected = _safe_float(alert.get("projected"))
    if opening is None or live is None or fair is None:
        return None
    home, away = parse_score(str(alert.get("score") or ""))
    if home is None or away is None:
        return None
    clock = game_clock(
        str(alert.get("status") or ""),
        str(alert.get("match_name") or ""),
        str(alert.get("tournament") or ""),
    )
    period = clock.get("period")
    remaining = clock.get("remaining_min")
    quarter_length = clock.get("quarter_length")
    total_game_min = clock.get("total_game_min")
    if None in (period, remaining, quarter_length, total_game_min):
        return None
    elapsed = (period * quarter_length) - remaining
    if elapsed < 3:
        return {"label": "PAS", "tone": "neutral", "title": "Erken dakika.", "rank": 1}
    if period == 1 and elapsed < 6:
        return None
    instant_pace = ((home + away) / elapsed) * total_game_min
    if period not in {1, 2} or instant_pace <= opening + 15 or _is_blacklisted_league(alert, list_profile):
        return None
    if abs(live - fair) > 12:
        return {"label": "PAS", "tone": "neutral", "title": "Canlı-adil farkı 12 puandan büyük.", "rank": 1}
    if projected is None or projected > live:
        return None
    return {"label": "★", "tone": "star", "title": "Erken tempo şişmesi ALT onayı.", "rank": 4}


def build_quality_tag(alert: dict, list_profile: dict | None = None) -> dict:
    direction = _normalize_direction(alert.get("direction"))
    opening = _safe_float(alert.get("opening"))
    live = _safe_float(alert.get("live"))
    fair = _safe_float(alert.get("fair_line"))
    empty = {"label": "-", "tone": "empty", "title": "Kalite kuralı uygulanmadı.", "rank": 0}
    if opening is None or live is None or fair is None:
        return empty
    early = _early_tempo_tag(alert, list_profile)
    if early is not None:
        return early
    if opening < 170 and direction == "ÜST":
        if fair < live:
            return {"label": "FADE", "tone": "fade", "title": "Açılış <170 ve ÜST; adil barem canlının altında.", "rank": 3}
        return {"label": "PAS", "tone": "neutral", "title": "Açılış <170 ve ÜST.", "rank": 1}
    if opening > 180 and direction == "ALT":
        gap = live - fair
        if live > fair + 8:
            return {"label": "FADE", "tone": "fade", "title": "Canlı barem adil baremin 8+ üstünde.", "rank": 3}
        if 0 <= gap <= 8:
            return {"label": "İZLE", "tone": "neutral", "title": "Canlı-adil farkı 0-8 aralığında.", "rank": 1}
        if live < fair:
            return {"label": "✓", "tone": "red-check", "title": "Canlı barem adil baremin altında.", "rank": 2}
    return empty


def build_signal_list_profile(entries: list[dict]) -> dict:
    profile = {
        "black": {"team": set(), "league": set()},
        "white": {"team": set(), "league": set()},
    }
    labels = {
        "black": {"team": {}, "league": {}},
        "white": {"team": {}, "league": {}},
    }
    for row in entries or []:
        list_type = str(row.get("list_type") or "").strip().lower()
        scope = str(row.get("scope") or "").strip().lower()
        normalized = str(row.get("normalized_value") or "").strip()
        value = str(row.get("value") or "").strip()
        if list_type not in profile or scope not in profile[list_type] or not normalized:
            continue
        profile[list_type][scope].add(normalized)
        labels[list_type][scope][normalized] = value or normalized
    return {"sets": profile, "labels": labels}


def split_match_teams(match_name: str) -> tuple[str, str]:
    parts = [part.strip() for part in str(match_name or "").split(" - ", 1)]
    if len(parts) == 2:
        return parts[0], parts[1]
    return str(match_name or "").strip(), ""


def build_signal_list_markers(alert: dict, list_profile: dict | None) -> list[dict]:
    if not isinstance(list_profile, dict):
        return []
    sets = list_profile.get("sets") or {}
    labels = list_profile.get("labels") or {}
    home, away = split_match_teams(alert.get("match_name") or "")
    candidates = {
        "team": [team for team in (home, away) if team],
        "league": [str(alert.get("tournament") or "").strip()],
    }
    markers: list[dict] = []
    for list_type, marker_label, tone in (
        ("black", "Kara liste", "blacklist"),
        ("white", "Beyaz liste", "whitelist"),
    ):
        for scope, values in candidates.items():
            for value in values:
                normalized = Database.normalize_signal_list_value(value)
                if normalized and normalized in (sets.get(list_type, {}).get(scope) or set()):
                    display = labels.get(list_type, {}).get(scope, {}).get(normalized) or value
                    scope_label = "Takım" if scope == "team" else "Lig"
                    markers.append({
                        "type": list_type,
                        "tone": tone,
                        "scope": scope,
                        "value": display,
                        "symbol": "*",
                        "title": f"{marker_label} - {scope_label}: {display}",
                    })
    return markers
