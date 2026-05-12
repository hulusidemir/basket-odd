from db import Database
from projection import game_clock, parse_score


def normalize_direction(value) -> str:
    text = str(value or "").strip().upper().replace("UST", "ÜST")
    if text in {"ALT", "ÜST"}:
        return text
    return text or "-"


def safe_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def split_match_teams(match_name: str) -> tuple[str, str]:
    parts = [part.strip() for part in str(match_name or "").split(" - ", 1)]
    if len(parts) == 2:
        return parts[0], parts[1]
    return str(match_name or "").strip(), ""


def _is_blacklisted_league(alert: dict, list_profile: dict | None) -> bool:
    if not isinstance(list_profile, dict):
        return False
    tournament = str(alert.get("tournament") or "").strip()
    if not tournament:
        return False
    normalized = Database.normalize_signal_list_value(tournament)
    sets = list_profile.get("sets") or {}
    return normalized in (sets.get("black", {}).get("league") or set())


def _early_tempo_star(alert: dict, list_profile: dict | None) -> dict | None:
    direction = normalize_direction(alert.get("direction"))
    if direction != "ALT":
        return None

    opening = safe_float(alert.get("opening"), None)
    live = safe_float(alert.get("live"), None)
    fair = safe_float(alert.get("fair_line"), None)
    projected = safe_float(alert.get("projected_total"), None)
    if projected is None:
        projected = safe_float(alert.get("projected"), None)
    if opening is None or live is None or fair is None:
        return None

    home_score, away_score = parse_score(str(alert.get("score") or ""))
    if home_score is None or away_score is None:
        return None

    match_name = str(alert.get("match_name") or "")
    tournament = str(alert.get("tournament") or "")
    clock = game_clock(str(alert.get("status") or ""), match_name, tournament)
    period = clock.get("period")
    remaining_min = clock.get("remaining_min")
    quarter_length = clock.get("quarter_length")
    total_game_min = clock.get("total_game_min")
    if period is None or remaining_min is None or quarter_length is None or total_game_min is None:
        return None

    elapsed_min = (period * quarter_length) - remaining_min
    if elapsed_min < 3:
        return {
            "label": "PAS",
            "tone": "neutral",
            "title": "Erken dakika: 3 dakika dolmadan tempo projeksiyonu yapılmadı.",
            "rank": 1,
        }
    if period == 1 and elapsed_min < 6:
        return None

    instant_total = home_score + away_score
    instant_pace = (instant_total / elapsed_min) * total_game_min
    if period not in {1, 2} or instant_pace <= opening + 15:
        return None

    if _is_blacklisted_league(alert, list_profile):
        return None

    if abs(live - fair) > 12:
        return {
            "label": "PAS",
            "tone": "neutral",
            "title": "Uçurum filtresi: canlı barem ile adil barem farkı 12 puandan büyük.",
            "rank": 1,
        }

    if projected is None or projected > live:
        return None

    return {
        "label": "★",
        "tone": "star",
        "title": (
            "Erken tempo şişmesi: Q1/Q2 anlık pace açılış bareminin 15+ üstünde; "
            "projeksiyon canlı baremin altında kaldığı için ALT sinyali onaylandı."
        ),
        "rank": 4,
    }


def build_quality_tag(alert: dict, list_profile: dict | None = None) -> dict:
    direction = normalize_direction(alert.get("direction"))
    opening = safe_float(alert.get("opening"), None)
    live = safe_float(alert.get("live"), None)
    fair = safe_float(alert.get("fair_line"), None)

    empty = {
        "label": "-",
        "tone": "empty",
        "title": "Kalite kuralı uygulanmadı.",
        "rank": 0,
    }
    if opening is None or live is None or fair is None:
        return empty

    early_tempo = _early_tempo_star(alert, list_profile)
    if early_tempo is not None:
        return early_tempo

    if opening < 170 and direction == "ÜST":
        if fair < live:
            return {
                "label": "FADE",
                "tone": "fade",
                "title": "Açılış < 170 ve ÜST: adil barem canlı baremin altında.",
                "rank": 3,
            }
        return {
            "label": "PAS",
            "tone": "neutral",
            "title": "Açılış < 170 ve ÜST: adil barem canlı baremi karşılıyor, ama kural pas geç diyor.",
            "rank": 1,
        }

    if opening > 180 and direction == "ALT":
        gap = live - fair
        if live > fair + 8:
            return {
                "label": "FADE",
                "tone": "fade",
                "title": "Açılış > 180 ve ALT: canlı barem adil baremin 8+ üstünde.",
                "rank": 3,
            }
        if 0 <= gap <= 8:
            return {
                "label": "İZLE",
                "tone": "neutral",
                "title": "Açılış > 180 ve ALT: canlı-adil farkı 0-8 aralığında.",
                "rank": 1,
            }
        if live < fair:
            return {
                "label": "✓",
                "tone": "red-check",
                "title": "Açılış > 180 ve ALT: canlı barem adil baremin altında, oynanır.",
                "rank": 2,
            }

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
                    display = (
                        labels.get(list_type, {}).get(scope, {}).get(normalized)
                        or value
                    )
                    scope_label = "Takım" if scope == "team" else "Lig"
                    markers.append({
                        "type": list_type,
                        "tone": tone,
                        "scope": scope,
                        "value": display,
                        "symbol": "●",
                        "title": f"{marker_label} — {scope_label}: {display}",
                    })
    return markers
