"""Sinyal skoru — geçmiş sonuçlara göre kalibre edilmiş tek güven puanı.

2026-05-20 kalibrasyonu, yerel DB'deki 3545 sonuçlanmış alert üzerinde yapıldı.
Önemli not: silinen/geçmiş kayıtlarda ``score`` final skoruna dönebildiği için
canlı sinyal anı skoru öncelikle ``alert_moment`` içinden okunur.

Kalibrasyon özeti:
  - C_A / 100 profil katmanı gerçek yüksek güven bölgesi: C_A toplamı ~%90+.
  - Normal sinyaller C_A yokken yaklaşık yazı-tura; bu yüzden 85+ skor verilmez.
  - Normal sinyallerde en iyi ayrışan alanlar: açılış bandı, anlık tempo (PPM),
    sinyal anı toplam sayı, fair edge ve projeksiyon gap'in yönle uyumu.
"""

from __future__ import annotations

import re
from typing import Any


_SCORE_RE = re.compile(r"(\d{1,3})\s*[-–]\s*(\d{1,3})")


def _num(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_score(score: Any) -> tuple[int | None, int | None]:
    m = _SCORE_RE.search(str(score or ""))
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def _label(score: int) -> str:
    if score >= 95:
        return "Çok Güçlü"
    if score >= 85:
        return "Güçlü"
    if score >= 70:
        return "Güvenilir"
    if score >= 60:
        return "Oynanabilir"
    if score >= 50:
        return "İzle"
    if score >= 40:
        return "Zayıf"
    return "Pas"


def _normalize_direction(value: Any) -> str:
    direction = str(value or "").strip().upper().replace("UST", "ÜST")
    return direction if direction in ("ALT", "ÜST") else ""


def _code_play(code: Any) -> str:
    code = str(code or "").strip().upper()
    if code in ("TRUE_UNDER", "FADE_UNDER"):
        return "ALT"
    if code in ("TRUE_OVER", "FADE_OVER"):
        return "ÜST"
    return ""


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "evet"}
    return bool(value)


def _parse_alert_score(alert: dict) -> tuple[int | None, int | None]:
    """Prefer signal-time score in alert_moment; fall back to current score."""
    for source in (alert.get("alert_moment"), alert.get("score")):
        matches = list(_SCORE_RE.finditer(str(source or "")))
        if matches:
            match = matches[-1]
            return int(match.group(1)), int(match.group(2))
    return None, None


def _period(alert: dict) -> int:
    try:
        value = int(alert.get("alert_period") or 0)
    except (TypeError, ValueError):
        value = 0
    if value:
        return value
    text = f"{alert.get('alert_moment') or ''} {alert.get('status') or ''}"
    for number in (1, 2, 3, 4):
        if re.search(rf"\bq{number}\b|\b{number}q\b|{number}\.\s*", text, re.IGNORECASE):
            return number
    return 0


def _add_opening_points(score: float, direction: str, opening: float | None) -> float:
    if opening is None:
        return score
    if direction == "ALT":
        if opening < 140:
            score += 2
        elif opening < 150:
            score -= 1
        elif opening < 160:
            score -= 2
        elif opening < 170:
            score += 4
        elif opening < 180:
            score += 2
        elif opening < 190:
            score -= 4
        else:
            score -= 8
    else:
        if opening < 140:
            score += 6
        elif opening < 150:
            score -= 7
        elif opening < 160:
            score -= 10
        elif opening < 170:
            score += 12
        elif opening < 180:
            score -= 4
        elif opening < 190:
            score -= 4
        else:
            score += 5
    return score


def _add_ppm_points(score: float, direction: str, ppm: float | None) -> float:
    if ppm is None:
        return score
    if direction == "ALT":
        if ppm < 3.5:
            score += 5
        elif ppm < 4.0:
            score += 4
        elif ppm < 4.5:
            score -= 2
        elif ppm < 5.0:
            score += 2
        elif ppm < 5.5:
            score -= 5
        else:
            score -= 3
    else:
        if ppm < 3.5:
            score += 0
        elif ppm < 4.0:
            score += 8
        elif ppm < 4.5:
            score -= 6
        elif ppm < 5.0:
            score -= 5
        elif ppm < 5.5:
            score -= 2
        else:
            score += 5
    return score


def _add_live_total_points(score: float, direction: str, live_total: int | None) -> float:
    if live_total is None:
        return score
    if direction == "ALT":
        if live_total < 80:
            score -= 2
        elif live_total < 100:
            score -= 1
        elif live_total < 140:
            score += 0
        elif live_total < 160:
            score += 5
        else:
            score -= 6
    else:
        if live_total < 80:
            score += 0
        elif live_total < 100:
            score += 6
        elif live_total < 120:
            score -= 1
        elif live_total < 140:
            score -= 5
        elif live_total < 160:
            score -= 8
    return score


def _add_fair_points(score: float, direction: str, fair_edge: float | None) -> float:
    if fair_edge is None:
        return score
    if direction == "ALT":
        if fair_edge < -10:
            score -= 6
        elif fair_edge < -6:
            score -= 2
        elif fair_edge < -3:
            score += 1
        elif 3 <= fair_edge < 6:
            score += 4
        elif fair_edge >= 6:
            score += 3
    else:
        if -3 <= fair_edge < 0:
            score -= 6
        elif 0 <= fair_edge < 6:
            score += 2
        elif 6 <= fair_edge < 10:
            score += 1
        elif fair_edge >= 10:
            score += 4
    return score


def _add_projection_points(score: float, direction: str, projected_gap: float | None) -> float:
    if projected_gap is None:
        return score
    if direction == "ALT":
        if 6 <= projected_gap < 10:
            score -= 4
        elif projected_gap >= 10:
            score -= 1
        elif projected_gap <= -6:
            score += 2
    else:
        if -10 <= projected_gap < -6:
            score -= 5
        elif 0 <= projected_gap < 3:
            score += 3
        elif 6 <= projected_gap < 10:
            score -= 4
        elif projected_gap >= 10:
            score += 3
    return score


def compute_signal_score(alert: dict, analysis: dict | None = None) -> dict[str, Any]:
    """Bir sinyal için 0-100 arası skor üretir.

    Dönüş: {"score": int 0-100, "label": str}
    """
    a = analysis if isinstance(analysis, dict) else {}
    # The dashboard recomputes C_A from current rules. Older alerts can still
    # carry stale C_A codes inside ai_analysis, so only fall back to analysis
    # when the alert itself has no claude_ai field at all.
    raw_code = alert.get("claude_ai") if "claude_ai" in alert else a.get("claude_ai")
    code = str(raw_code or "").strip()
    direction = _code_play(code) or _normalize_direction(
        alert.get("direction") or a.get("direction") or a.get("final_direction")
    )
    if direction not in ("ALT", "ÜST"):
        return {"score": 50, "label": "—"}

    if code in ("TRUE_OVER", "FADE_OVER"):
        score = 96
        return {"score": score, "label": _label(score)}
    if code in ("TRUE_UNDER", "FADE_UNDER"):
        score = 91
        return {"score": score, "label": _label(score)}

    if _truthy(alert.get("hundred_profile") or a.get("hundred_profile")):
        score = 84 if direction == "ALT" else 96
        return {"score": score, "label": _label(score)}

    opening = _num(alert.get("opening"))
    live = _num(alert.get("live"))
    period = _period(alert)
    fair_edge = _num(a.get("fair_edge"))
    projected = _num(a.get("projected_total"))
    proj_components = a.get("projection_components") or {}
    ppm = _num(proj_components.get("current_pace_per_min")) or _num(a.get("match_ppm"))
    elapsed = _num(a.get("elapsed_minutes"))

    home_s, away_s = _parse_alert_score(alert)
    live_total = (home_s + away_s) if home_s is not None else None

    # PPM elde yoksa anlık skor + elapsed_minutes'tan türet
    if ppm is None and live_total is not None and elapsed and elapsed > 0:
        ppm = live_total / elapsed

    score = 51.0 if direction == "ALT" else 50.0
    score = _add_opening_points(score, direction, opening)
    score = _add_ppm_points(score, direction, ppm)
    score = _add_live_total_points(score, direction, live_total)

    projected_gap = None
    if projected is not None and live is not None:
        projected_gap = projected - live
    score = _add_fair_points(score, direction, fair_edge)
    score = _add_projection_points(score, direction, projected_gap)

    diff = abs(_num(alert.get("diff")) or 0)
    if direction == "ALT":
        if period == 1:
            score -= 1
        elif period in (2, 4):
            score += 2
        if 12 <= diff < 16:
            score += 3
        elif 16 <= diff < 20:
            score += 2
        elif diff >= 25:
            score -= 1
    else:
        if period == 3:
            score += 3
        elif period == 2:
            score -= 2
        if 12 <= diff < 16:
            score += 4
        elif diff >= 25:
            score += 4
        elif 10 <= diff < 12:
            score -= 2

    backtest = a.get("backtest") if isinstance(a.get("backtest"), dict) else {}
    backtest_rate = _num(backtest.get("chosen_rate"))
    try:
        backtest_samples = int(backtest.get("chosen_samples") or 0)
    except (TypeError, ValueError):
        backtest_samples = 0
    if backtest_rate is not None and backtest_samples >= 40:
        if backtest_rate >= 65:
            score += 4
        elif backtest_rate >= 60:
            score += 2
        elif backtest_rate <= 40:
            score -= 4
        elif backtest_rate <= 45:
            score -= 2

    # Normal sinyaller geçmişte 85+ güveni taşımadı; o bölge C_A/100 profilindir.
    score = int(round(max(0.0, min(74.0, score))))
    return {"score": score, "label": _label(score)}
