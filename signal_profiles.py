import re


def _num(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def _first_num(*values) -> float | None:
    for value in values:
        number = _num(value)
        if number is not None:
            return number
    return None


def _normalize_direction(value) -> str:
    direction = str(value or "").strip().upper().replace("UST", "ÜST")
    if direction == "ALT":
        return "ALT"
    if direction == "ÜST":
        return "ÜST"
    return ""


def _score_parts(score: str) -> tuple[float, float] | None:
    match = re.search(r"(\d{1,3})\s*[-–]\s*(\d{1,3})", str(score or ""))
    if not match:
        return None
    return float(match.group(1)), float(match.group(2))


def evaluate_hundred_profile(alert: dict, analysis: dict | None = None) -> dict:
    analysis = analysis if isinstance(analysis, dict) else {}
    direction = _normalize_direction(
        alert.get("direction") or analysis.get("direction") or analysis.get("final_direction")
    )
    fair_edge = _first_num(alert.get("fair_edge"), analysis.get("fair_edge"))
    diff = abs(_num(alert.get("diff")) or 0)
    parts = _score_parts(alert.get("score"))
    total = sum(parts) if parts else None
    margin = abs(parts[0] - parts[1]) if parts else None
    components = analysis.get("projection_components")
    components = components if isinstance(components, dict) else {}
    ppm = _first_num(analysis.get("match_ppm"), components.get("current_pace_per_min"))
    projected = _first_num(alert.get("projected"), analysis.get("projected_total"))
    live = _num(alert.get("live"))
    projected_gap = _first_num(alert.get("projected_gap"), analysis.get("projected_gap"))
    if projected_gap is None and projected is not None and live is not None:
        projected_gap = projected - live
    quality = str(alert.get("quality_label") or analysis.get("quality_label") or "-").strip() or "-"

    rules = []
    if direction == "ALT":
        if total is not None and ppm is not None and 140 <= total < 170 and ppm >= 5 and quality == "-":
            rules.append("alt_total_140_170_ppm_5_quality_empty")
        if total is not None and 110 <= total < 140 and 12 <= diff < 14:
            rules.append("alt_total_110_140_diff_12_14")
        if (
            fair_edge is not None
            and total is not None
            and ppm is not None
            and -3 < fair_edge < 0
            and 140 <= total < 170
            and 4.2 <= ppm < 4.6
        ):
            rules.append("alt_edge_minus3_0_total_140_170_ppm_4_2_4_6")

    if direction == "ÜST":
        if (
            total is not None
            and ppm is not None
            and projected_gap is not None
            and total >= 170
            and 3.8 <= ppm < 4.2
            and -5 < projected_gap < 0
        ):
            rules.append("ust_total_170_ppm_3_8_4_2_gap_minus5_0")
        if (
            fair_edge is not None
            and total is not None
            and total >= 170
            and 10 <= diff < 12
            and 0 < fair_edge < 3
        ):
            rules.append("ust_total_170_diff_10_12_edge_0_3")
        if (
            margin is not None
            and total is not None
            and projected_gap is not None
            and 4 <= margin <= 7
            and total >= 170
            and projected_gap <= -10
        ):
            rules.append("ust_margin_4_7_total_170_gap_lte_minus10")

    return {
        "hundred_profile": bool(rules),
        "hundred_profile_rule": rules[0] if rules else "",
    }
