from db import Database


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
