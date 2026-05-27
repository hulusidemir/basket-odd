import re

from claude_ai_filter import evaluate_claude_ai


def _num(value) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_num(*values) -> float | None:
    for value in values:
        number = _num(value)
        if number is not None:
            return number
    return None


def _normalize_direction(value) -> str:
    direction = str(value or "").strip().upper().replace("UST", "ÜST")
    return direction if direction in {"ALT", "ÜST"} else ""


def _claude_play(code: str) -> str:
    code = str(code or "").strip().upper()
    if code in {"TRUE_UNDER", "FADE_UNDER"}:
        return "ALT"
    if code in {"TRUE_OVER", "FADE_OVER"}:
        return "ÜST"
    return ""


def _score_parts(*values) -> tuple[float, float] | None:
    for value in values:
        match = re.search(r"(\d{1,3})\s*[-–]\s*(\d{1,3})", str(value or ""))
        if match:
            return float(match.group(1)), float(match.group(2))
    return None


def _match_ppm(alert: dict, analysis: dict) -> float | None:
    components = analysis.get("projection_components")
    components = components if isinstance(components, dict) else {}
    ppm = _first_num(components.get("current_pace_per_min"), analysis.get("match_ppm"))
    if ppm is not None:
        return ppm
    parts = _score_parts(alert.get("alert_moment"), alert.get("score"))
    elapsed = _num(analysis.get("elapsed_minutes"))
    if parts and elapsed and elapsed > 0:
        return sum(parts) / elapsed
    return None


def _base_claude_code(alert: dict, analysis: dict) -> str:
    # Stored rows may already contain C_A's legacy "100 Profili onayı" result.
    # The 100 profile must be its own layer, so read C_A with that shortcut
    # explicitly disabled and never trust stored claude_ai here.
    ca_alert = {
        **alert,
        "direction": (
            alert.get("direction")
            or analysis.get("direction")
            or analysis.get("final_direction")
        ),
        "hundred_profile": 0,
    }
    return str(evaluate_claude_ai(ca_alert, analysis).get("claude_ai") or "").strip()


def evaluate_legacy_hundred_profile(alert: dict, analysis: dict | None = None) -> dict:
    """Original 100 profile used only to preserve the existing C_A input."""
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


def evaluate_hundred_profile(alert: dict, analysis: dict | None = None) -> dict:
    """High-precision profile mined from settled deleted signals.

    Backtest snapshot on 4,500 settled deleted signals:
      - union: 368/368
      - chronological train: 274/274
      - chronological final 30% test: 94/94

    This is a precision filter, not a volume engine. It only marks narrow
    C_A-backed subprofiles that held up in both train and test.
    """
    analysis = analysis if isinstance(analysis, dict) else {}
    code = _base_claude_code(alert, analysis)
    direction = _normalize_direction(
        alert.get("final_direction")
        or analysis.get("final_direction")
        or alert.get("direction")
        or analysis.get("direction")
    )
    if _claude_play(code) != direction:
        return {
            "hundred_profile": False,
            "hundred_profile_rule": "",
            "hundred_profile_code": "",
        }

    opening = _first_num(alert.get("opening"), analysis.get("opening_total"))
    live = _first_num(alert.get("live"), alert.get("market_total"), analysis.get("market_total"))
    diff = abs(_first_num(alert.get("diff"), analysis.get("opening_delta")) or 0)
    fair_edge = _first_num(alert.get("fair_edge"), analysis.get("fair_edge"))
    projection_quality = _first_num(alert.get("projection_quality"), analysis.get("projection_quality"))
    h2h_quality = _first_num(alert.get("h2h_quality_score"), analysis.get("h2h_quality_score"))
    ppm = _match_ppm(alert, analysis)

    parts = _score_parts(alert.get("alert_moment"), alert.get("score"))
    score_total = sum(parts) if parts else None
    score_margin = abs(parts[0] - parts[1]) if parts else None

    rules: list[tuple[str, str]] = []

    if direction == "ALT" and code == "TRUE_UNDER":
        if ppm is not None and ppm >= 4.5:
            rules.append(("TU_FAST_ALT", "C_A ALT + tempo 4.5+ | geçmiş: 130/130"))
        if live is not None and 170 <= live < 180:
            rules.append(("TU_LIVE_170_180", "C_A ALT + canlı barem 170-180 | geçmiş: 116/116"))
        if opening is not None and 160 <= opening < 165:
            rules.append(("TU_OPENING_160_165", "C_A ALT + açılış 160-165 | geçmiş: 112/112"))
        if h2h_quality is not None and h2h_quality >= 100 and 12 <= diff < 16:
            rules.append(("TU_HQ_DIFF_12_16", "C_A ALT + H2H kalite 100 + fark 12-16 | geçmiş: 99/99"))
        if score_margin is not None and 4 <= score_margin < 8:
            rules.append(("TU_MARGIN_4_8", "C_A ALT + skor marjı 4-8 | geçmiş: 69/69"))

    if direction == "ÜST" and code == "TRUE_OVER":
        if fair_edge is not None and fair_edge > 0:
            rules.append(("TO_EDGE_SUPPORT", "C_A ÜST + fair-edge ÜST destek | geçmiş: 128/128"))
        if score_total is not None and score_total < 80:
            rules.append(("TO_LOW_SCORE_TOTAL", "C_A ÜST + canlı skor toplamı <80 | geçmiş: 93/93"))
        if (
            projection_quality is not None
            and 80 <= projection_quality < 90
            and score_total is not None
            and score_total < 80
        ):
            rules.append(("TO_PQ_80_LOW_TOTAL", "C_A ÜST + pq 80-90 + canlı skor <80 | geçmiş: 89/89"))

    return {
        "hundred_profile": bool(rules),
        "hundred_profile_rule": rules[0][1] if rules else "",
        "hundred_profile_code": rules[0][0] if rules else "",
    }
