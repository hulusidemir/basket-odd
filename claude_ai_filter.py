"""Historical C_A profile filter restored for live-dashboard classification."""

import re
from typing import Any


SCENARIOS = {
    "TRUE_UNDER": {"label": "Güçlü Alt", "play": "ALT", "tooltip": "Güçlü Alt - geçmişte %100 tutan örüntü"},
    "TRUE_OVER": {"label": "Güçlü Üst", "play": "ÜST", "tooltip": "Güçlü Üst - geçmişte %100 tutan örüntü"},
    "FADE_OVER": {"label": "Tersine Üst", "play": "ÜST", "tooltip": "Tersine Üst - ALT sinyalini tersle, ÜST oyna"},
    "FADE_UNDER": {"label": "Tersine Alt", "play": "ALT", "tooltip": "Tersine Alt - ÜST sinyalini tersle, ALT oyna"},
}

_SCORE_RE = re.compile(r"(\d{1,3})\s*[-–]\s*(\d{1,3})")


def _f(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _live_total(score: Any) -> int | None:
    match = _SCORE_RE.search(str(score or ""))
    if not match:
        return None
    return int(match.group(1)) + int(match.group(2))


def evaluate_claude_ai(alert: dict, analysis: dict | None) -> dict:
    direction = str(alert.get("direction") or "").strip().upper().replace("UST", "ÜST")
    if direction not in ("ALT", "ÜST"):
        return {"claude_ai": "", "claude_ai_rule": ""}

    data = analysis if isinstance(analysis, dict) else {}
    if int(alert.get("hundred_profile") or 0) == 1:
        return {
            "claude_ai": "TRUE_UNDER" if direction == "ALT" else "TRUE_OVER",
            "claude_ai_rule": "100 Profili onayı",
        }

    opening = _f(alert.get("opening"))
    live = _f(alert.get("live"))
    period = int(alert.get("alert_period") or 0)
    live_total = _live_total(alert.get("score"))
    fair_edge = _f(data.get("fair_edge"))
    projected_total = _f(data.get("projected_total"))
    h2h_total = _f(data.get("h2h_total"))
    projection_quality = _f(data.get("projection_quality"))
    components = data.get("projection_components") or {}
    ppm = _f(components.get("current_pace_per_min")) or _f(data.get("match_ppm"))
    projected_gap = projected_total - live if projected_total is not None and live is not None else None
    h2h_gap = h2h_total - live if h2h_total is not None and live is not None else None

    if direction == "ALT":
        if opening is not None and live_total is not None and fair_edge is not None and 160 <= opening < 170 and 150 <= live_total < 170 and -8 <= fair_edge <= -3:
            return {"claude_ai": "TRUE_UNDER", "claude_ai_rule": "B1: açılış 160-170 + canlı 150-170 + fair ALT destek"}
        if opening is not None and live_total is not None and ppm is not None and 160 <= opening < 170 and 150 <= live_total < 170 and 4.5 <= ppm < 5.0:
            return {"claude_ai": "TRUE_UNDER", "claude_ai_rule": "B2: açılış 160-170 + canlı 150-170 + tempo 4.5-5.0"}
        if live_total is not None and projected_gap is not None and ppm is not None and 150 <= live_total < 170 and -5 < projected_gap < 5 and 4.5 <= ppm < 5.0:
            return {"claude_ai": "TRUE_UNDER", "claude_ai_rule": "B3: canlı 150-170 + projeksiyon nötr + tempo 4.5-5.0"}
        if live_total is not None and projected_gap is not None and projection_quality is not None and 120 <= live_total < 150 and 5 <= projected_gap < 15 and projection_quality >= 80:
            return {"claude_ai": "TRUE_UNDER", "claude_ai_rule": "B4: canlı 120-150 + projeksiyon yanıltıcı ÜST + pq>=80"}
        if opening is not None and live_total is not None and h2h_gap is not None and 160 <= opening < 170 and 150 <= live_total < 170 and -15 <= h2h_gap <= -5:
            return {"claude_ai": "TRUE_UNDER", "claude_ai_rule": "B5: açılış 160-170 + canlı 150-170 + H2H ALT destek"}

    if direction == "ÜST":
        if opening is not None and live_total is not None and ppm is not None and 150 <= opening < 160 and 150 <= live_total < 170 and ppm < 3.5:
            return {"claude_ai": "TRUE_OVER", "claude_ai_rule": "C1: açılış 150-160 + canlı 150-170 + tempo <3.5"}
        if opening is not None and live_total is not None and ppm is not None and 170 <= opening < 180 and live_total >= 170 and 3.5 <= ppm < 4.0:
            return {"claude_ai": "TRUE_OVER", "claude_ai_rule": "C2: açılış 170-180 + canlı >=170 + tempo 3.5-4.0"}
        if live_total is not None and projected_gap is not None and ppm is not None and live_total >= 170 and -5 < projected_gap < 5 and 3.5 <= ppm < 4.0:
            return {"claude_ai": "TRUE_OVER", "claude_ai_rule": "C3: canlı >=170 + projeksiyon nötr + tempo 3.5-4.0"}
        if live_total is not None and ppm is not None and live_total >= 170 and period == 3 and 3.5 <= ppm < 4.0:
            return {"claude_ai": "TRUE_OVER", "claude_ai_rule": "C4: canlı >=170 + P3 + tempo 3.5-4.0"}
        if opening is not None and live_total is not None and projected_gap is not None and 170 <= opening < 180 and live_total >= 170 and -5 < projected_gap < 5:
            return {"claude_ai": "TRUE_OVER", "claude_ai_rule": "C5: açılış 170-180 + canlı >=170 + projeksiyon nötr"}
        if opening is not None and live_total is not None and 160 <= opening < 170 and 120 <= live_total < 150 and 12 <= abs(_f(alert.get("diff")) or 0) < 16:
            return {"claude_ai": "FADE_UNDER", "claude_ai_rule": "F1: açılış 160-170 + canlı 120-150 + fark 12-16"}
        if opening is not None and live_total is not None and ppm is not None and 160 <= opening < 170 and 120 <= live_total < 150 and 3.5 <= ppm < 4.0:
            return {"claude_ai": "FADE_UNDER", "claude_ai_rule": "F2: açılış 160-170 + canlı 120-150 + tempo 3.5-4.0"}
        if live_total is not None and ppm is not None and 120 <= live_total < 150 and 12 <= abs(_f(alert.get("diff")) or 0) < 16 and 3.5 <= ppm < 4.0:
            return {"claude_ai": "FADE_UNDER", "claude_ai_rule": "F3: canlı 120-150 + fark 12-16 + tempo 3.5-4.0"}
        if live_total is not None and ppm is not None and h2h_gap is not None and live_total < 120 and h2h_gap >= 15 and ppm < 3.5:
            return {"claude_ai": "FADE_UNDER", "claude_ai_rule": "F4: canlı <120 + H2H yüksek + tempo <3.5"}
        if live_total is not None and ppm is not None and 120 <= live_total < 150 and period == 3 and 3.5 <= ppm < 4.0:
            return {"claude_ai": "FADE_UNDER", "claude_ai_rule": "F5: canlı 120-150 + P3 + tempo 3.5-4.0"}

    return {"claude_ai": "", "claude_ai_rule": ""}


def scenario_meta(code: str) -> dict:
    return SCENARIOS.get(code or "", {"label": "", "play": "", "tooltip": ""})


def scenario_play_direction(code: str) -> str:
    direction = str(scenario_meta(code).get("play") or "").strip().upper().replace("UST", "ÜST")
    return direction if direction in {"ALT", "ÜST"} else ""
