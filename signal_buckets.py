import json
import math
import re
from copy import deepcopy


RESEARCH_WARNING = (
    "Bu kova geçmiş silinen sinyallerde bulundu. Örneklem küçük olduğu için "
    "tek başına bahis kararı değil, yüksek öncelikli inceleme etiketi olarak kullanılmalıdır."
)


def _safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", ".").strip())
    except (TypeError, ValueError):
        match = re.search(r"-?\d+(?:\.\d+)?", str(value).replace(",", "."))
        return float(match.group(0)) if match else None


def _normalize_direction(value) -> str:
    text = str(value or "").strip().upper().replace("UST", "ÜST")
    return text if text in {"ALT", "ÜST"} else ""


def _parse_json_dict(value) -> dict:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _wilson_lower(success: int, resolved: int, z: float = 1.96) -> float | None:
    if resolved <= 0:
        return None
    rate = success / resolved
    denominator = 1 + z * z / resolved
    center = rate + z * z / (2 * resolved)
    adjustment = z * math.sqrt((rate * (1 - rate) + z * z / (4 * resolved)) / resolved)
    return ((center - adjustment) / denominator) * 100


def _rate(success: int, resolved: int) -> float | None:
    return round((success / resolved) * 100, 1) if resolved else None


def _bucket_values(alert: dict, analysis: dict | None = None) -> dict | None:
    analysis = analysis if isinstance(analysis, dict) else {}
    live = _safe_float(alert.get("live") or alert.get("inplay_total"))
    fair = _safe_float(alert.get("fair_line") or analysis.get("fair_line"))
    projected = _safe_float(
        alert.get("projected")
        or alert.get("projected_total")
        or analysis.get("projected_total")
        or analysis.get("projected")
    )
    if live is None or fair is None or projected is None:
        return None
    direction = _normalize_direction(
        alert.get("final_direction")
        or analysis.get("final_direction")
        or analysis.get("direction")
        or alert.get("direction")
    )
    return {
        "direction": direction,
        "live": live,
        "fair": fair,
        "projected": projected,
        "fair_edge": fair - live,
        "projected_gap": projected - live,
        "fair_projected_gap": fair - projected,
    }


BUCKET_DEFINITIONS = [
    {
        "id": "alt_160_170_fair_proj_below",
        "label": "ALT Prime",
        "direction": "ALT",
        "headline": "Canlı 160-170 bandında piyasa şişmesi",
        "rule": (
            "Canlı barem 160-170 aralığında; adil barem < projeksiyon < canlı barem; "
            "adil barem projeksiyondan 4-8 puan düşük."
        ),
        "analysis_note": (
            "Geçmişte bu yapı ALT tarafında çok temiz çalıştı. Model, canlı baremin hem "
            "adil çizginin hem projeksiyonun üstüne kaçtığını söylüyor."
        ),
        "predicate": lambda v: (
            v["direction"] == "ALT"
            and 160 <= v["live"] < 170
            and v["fair"] < v["projected"] < v["live"]
            and -8 <= v["fair_projected_gap"] < -4
        ),
    },
    {
        "id": "ust_projection_breakout",
        "label": "ÜST Baskı",
        "direction": "ÜST",
        "headline": "Projeksiyon canlı baremin çok üstünde",
        "rule": (
            "Sinyal yönü ÜST; projeksiyon canlı baremden 20+ puan yüksek; "
            "adil barem de canlı baremin üstünde."
        ),
        "analysis_note": (
            "Bu kova tempoda güçlü yukarı baskı gösterir. Büyük projeksiyon farkları "
            "oynak olabilir; bu yüzden maç scripti ve period mutlaka kontrol edilmelidir."
        ),
        "predicate": lambda v: (
            v["direction"] == "ÜST"
            and v["projected_gap"] > 20
            and v["fair_edge"] > 0
        ),
    },
    {
        "id": "ust_cross_pressure",
        "label": "ÜST Çapraz",
        "direction": "ÜST",
        "headline": "Adil hafif aşağıda, projeksiyon yukarıda",
        "rule": (
            "Sinyal yönü ÜST; adil barem < canlı barem < projeksiyon; "
            "projeksiyon canlıdan en az 4 puan yüksek; adil-canlı farkı 3-4.5 puan."
        ),
        "analysis_note": (
            "Bu yapı piyasa çizgisiyle tempo projeksiyonunun ayrıştığı ince bir bölge. "
            "Geçmişte seçici ama pozitif sonuç verdi."
        ),
        "predicate": lambda v: (
            v["direction"] == "ÜST"
            and v["fair"] < v["live"] < v["projected"]
            and v["projected_gap"] >= 4
            and 3 <= abs(v["fair_edge"]) < 4.5
        ),
    },
]


def _public_definition(definition: dict) -> dict:
    return {
        key: value
        for key, value in definition.items()
        if key != "predicate"
    }


def _row_payload(row: dict) -> tuple[dict, dict]:
    snapshot = _parse_json_dict(row.get("display_snapshot"))
    payload = dict(snapshot) if snapshot else dict(row)
    analysis = payload.get("analysis") if isinstance(payload.get("analysis"), dict) else _parse_json_dict(row.get("ai_analysis"))
    return payload, analysis


def matching_signal_buckets(
    alert: dict,
    analysis: dict | None = None,
    profile: dict | None = None,
) -> list[dict]:
    values = _bucket_values(alert, analysis)
    if not values:
        return []
    matches: list[dict] = []
    for definition in BUCKET_DEFINITIONS:
        if not definition["predicate"](values):
            continue
        item = _public_definition(definition)
        item["values"] = {
            "live": round(values["live"], 1),
            "fair": round(values["fair"], 1),
            "projected": round(values["projected"], 1),
            "fair_edge": round(values["fair_edge"], 1),
            "projected_gap": round(values["projected_gap"], 1),
            "fair_projected_gap": round(values["fair_projected_gap"], 1),
        }
        item["warning"] = RESEARCH_WARNING
        if isinstance(profile, dict):
            item["history"] = deepcopy(profile.get(definition["id"]) or {})
        matches.append(item)
    return matches


def build_signal_bucket_profile(rows: list[dict]) -> dict:
    grouped: dict[str, list[dict]] = {definition["id"]: [] for definition in BUCKET_DEFINITIONS}
    for row in rows or []:
        result = str(row.get("result") or "").strip()
        if result not in {"Başarılı", "Başarısız"}:
            continue
        payload, analysis = _row_payload(row)
        for match in matching_signal_buckets(payload, analysis):
            grouped.setdefault(match["id"], []).append({
                "success": result == "Başarılı",
                "date": str(row.get("alerted_at") or row.get("deleted_at") or ""),
                "id": int(row.get("id") or 0),
            })

    profile = {}
    for definition in BUCKET_DEFINITIONS:
        bucket_rows = sorted(grouped.get(definition["id"]) or [], key=lambda item: (item["date"], item["id"]))
        resolved = len(bucket_rows)
        success = sum(1 for item in bucket_rows if item["success"])
        midpoint = resolved // 2
        first_half = bucket_rows[:midpoint]
        second_half = bucket_rows[midpoint:]
        first_success = sum(1 for item in first_half if item["success"])
        second_success = sum(1 for item in second_half if item["success"])
        profile[definition["id"]] = {
            "resolved": resolved,
            "success": success,
            "fail": resolved - success,
            "rate": _rate(success, resolved),
            "wilson_low": round(_wilson_lower(success, resolved), 1) if resolved else None,
            "first_half": {
                "resolved": len(first_half),
                "success": first_success,
                "rate": _rate(first_success, len(first_half)),
            },
            "second_half": {
                "resolved": len(second_half),
                "success": second_success,
                "rate": _rate(second_success, len(second_half)),
            },
        }
    return profile
