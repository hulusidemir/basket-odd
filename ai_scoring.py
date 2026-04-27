"""
AI-style post signal scoring layer.

This module intentionally does not create or change signals. It only reviews an
already-created ALT/UST signal against live line, fair line, projection, period,
score gap, and repeat count.
"""

from projection import game_clock, parse_score


def _safe_float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clamp_score(value: float) -> int:
    return int(max(0, min(100, round(value))))


def _direction(value) -> str:
    normalized = str(value or "").strip().upper().replace("UST", "ÜST")
    return normalized if normalized in {"ALT", "ÜST"} else ""


def _side(value: float | None, live: float | None) -> str:
    if value is None or live is None:
        return ""
    if value > live:
        return "ÜST"
    if value < live:
        return "ALT"
    return ""


def _period(signal: dict, analysis: dict) -> int | None:
    for key in ("alert_period", "period"):
        period = _safe_int(signal.get(key))
        if period is not None:
            return period
    clock = game_clock(
        str(signal.get("status") or ""),
        str(signal.get("match_name") or ""),
        str(signal.get("tournament") or ""),
    )
    return clock.get("period")


def _score_gap(score: str) -> int | None:
    home, away = parse_score(score or "")
    if home is None or away is None:
        return None
    return abs(home - away)


def _format_delta(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.1f}".rstrip("0").rstrip(".")


def _make_reason(
    direction: str,
    *,
    opening_gap: float | None,
    fair_gap: float | None,
    projection_gap: float | None,
    fair_side: str,
    projection_side: str,
    period: int | None,
    score_gap: int | None,
    signal_count: int,
    is_ai_100: bool,
) -> str:
    repeat_note = ""
    if signal_count > 1:
        repeat_note = (
            f" Tekrar sinyal #{signal_count}; şartlar korunuyor."
            if is_ai_100
            else f" Tekrar sinyal #{signal_count} uyarısı var."
        )

    if is_ai_100 and direction == "ALT":
        return (
            f"ALT AI 100: Canlı barem açılıştan +{_format_delta(opening_gap)} yukarıda. "
            f"Adil barem canlıdan {_format_delta(fair_gap)} puan düşük. "
            f"Projeksiyon canlıdan {_format_delta(projection_gap)} puan düşük. "
            f"Adil ve projeksiyon ALT yönünü birlikte onaylıyor.{repeat_note}"
        )

    if is_ai_100 and direction == "ÜST":
        return (
            f"ÜST AI 100: Canlı barem açılıştan {_format_delta(abs(opening_gap or 0))} puan aşağıda. "
            f"Adil barem canlıdan {_format_delta(fair_gap)} puan yüksek. "
            f"Projeksiyon canlıdan {_format_delta(projection_gap)} puan yüksek. "
            f"ÜST yönü Q{period} içinde onaylanıyor.{repeat_note}"
        )

    if fair_side and projection_side and fair_side != projection_side:
        return f"Adil barem {fair_side}, projeksiyon {projection_side} gösteriyor. Çelişki var.{repeat_note}"

    if not fair_side or not projection_side:
        missing = []
        if not fair_side:
            missing.append("adil barem")
        if not projection_side:
            missing.append("projeksiyon")
        return f"{' ve '.join(missing).capitalize()} canlıya çok yakın veya eksik; net AI onayı yok.{repeat_note}"

    pieces = []
    if opening_gap is not None:
        pieces.append(f"canlı-açılış farkı {opening_gap:+.1f}")
    pieces.append(f"adil barem {fair_side}")
    pieces.append(f"projeksiyon {projection_side}")
    if period:
        pieces.append(f"periyot Q{period}")
    if score_gap is not None:
        pieces.append(f"skor farkı {score_gap}")
    verdict = "onay zayıf"
    if fair_side == direction and projection_side == direction:
        verdict = f"{direction} yönü destekleniyor"
    elif fair_side != direction or projection_side != direction:
        verdict = "sinyal yönü tam destek almıyor"
    return f"{'; '.join(pieces)}. {verdict}.{repeat_note}"


def calculate_ai_score(signal: dict, analysis: dict | None = None, raw_score: float | None = None) -> dict:
    """
    Return AI review fields for an already-created signal.

    raw_score is the old/current application score when available. When the
    caller has no old score (for example Telegram send-time), a neutral 50 is
    used as the starting point so the layer can still produce a readable review.
    """

    analysis = analysis or {}
    opening = _safe_float(signal.get("opening_total", signal.get("opening")))
    live = _safe_float(signal.get("inplay_total", signal.get("live")))
    fair_line = _safe_float(analysis.get("fair_line", signal.get("fair_line")))
    projection = _safe_float(
        analysis.get("projected_total", signal.get("projected", signal.get("projection")))
    )
    direction = _direction(signal.get("direction") or analysis.get("direction"))
    period = _period(signal, analysis)
    signal_count = max(_safe_int(signal.get("signal_count")) or 1, 1)
    gap = _score_gap(str(signal.get("score") or ""))

    raw = _safe_float(raw_score)
    if raw is None:
        raw = _safe_float(signal.get("raw_score"))
    if raw is None:
        raw = 50.0

    opening_gap = (live - opening) if live is not None and opening is not None else None
    fair_side = _side(fair_line, live)
    projection_side = _side(projection, live)
    fair_abs_gap = abs((fair_line or 0) - (live or 0)) if fair_line is not None and live is not None else None
    projection_abs_gap = abs((projection or 0) - (live or 0)) if projection is not None and live is not None else None

    if direction == "ALT":
        fair_gap = live - fair_line if live is not None and fair_line is not None else None
        projection_gap = live - projection if live is not None and projection is not None else None
        opening_strength = opening_gap if opening_gap is not None else None
        ai_100 = (
            opening_strength is not None
            and fair_gap is not None
            and projection_gap is not None
            and opening_strength >= 12
            and fair_gap >= 5
            and projection_gap >= 3
            and fair_side == "ALT"
            and projection_side == "ALT"
            and period in {1, 2, 3}
        )
    elif direction == "ÜST":
        fair_gap = fair_line - live if live is not None and fair_line is not None else None
        projection_gap = projection - live if live is not None and projection is not None else None
        opening_strength = -opening_gap if opening_gap is not None else None
        ai_100 = (
            opening_strength is not None
            and fair_gap is not None
            and projection_gap is not None
            and opening_strength >= 12
            and fair_gap >= 3
            and projection_gap >= 3
            and fair_side == "ÜST"
            and projection_side == "ÜST"
            and period in {3, 4}
            and not (period == 4 and gap is not None and gap >= 15)
        )
    else:
        fair_gap = None
        projection_gap = None
        opening_strength = None
        ai_100 = False

    if ai_100:
        label = f"AI 100 ONAY - {direction}"
        reason = _make_reason(
            direction,
            opening_gap=opening_gap,
            fair_gap=fair_gap,
            projection_gap=projection_gap,
            fair_side=fair_side,
            projection_side=projection_side,
            period=period,
            score_gap=gap,
            signal_count=signal_count,
            is_ai_100=True,
        )
        return {
            "raw_score": _clamp_score(raw),
            "ai_score": 100,
            "ai_label": label,
            "ai_reason": reason,
            "final_score": 100,
        }

    score = raw
    if fair_side == direction:
        score += 15
    elif fair_side:
        score -= 25
    if projection_side == direction:
        score += 15
    elif projection_side:
        score -= 25
    if fair_side and projection_side:
        if fair_side == projection_side:
            score += 10
        else:
            score -= 30

    if opening_strength is not None:
        if opening_strength >= 12:
            score += 10
        if opening_strength >= 18:
            score += 5

    if direction == "ALT":
        if fair_gap is not None and fair_gap >= 5:
            score += 10
        if projection_gap is not None and projection_gap >= 3:
            score += 10
        if period in {1, 2, 3}:
            score += 5
        elif period == 4:
            score -= 15
    elif direction == "ÜST":
        if fair_gap is not None and fair_gap >= 3:
            score += 10
        if projection_gap is not None and projection_gap >= 3:
            score += 10
        if period in {3, 4}:
            score += 10
        elif period in {1, 2}:
            score -= 25
        if period == 4 and gap is not None:
            if gap >= 15:
                score -= 20
            elif gap <= 10:
                score += 5

    if fair_abs_gap is not None and fair_abs_gap < 3:
        score -= 10
    if projection_abs_gap is not None and projection_abs_gap < 3:
        score -= 10
    if signal_count >= 3:
        score -= 10
    elif signal_count >= 2:
        score -= 5

    ai_score = _clamp_score(score)
    conflict = bool(fair_side and projection_side and fair_side != projection_side)
    weak_gap = opening_strength is not None and opening_strength < 12
    near_line = (
        (fair_abs_gap is not None and fair_abs_gap < 3)
        or (projection_abs_gap is not None and projection_abs_gap < 3)
    )

    if conflict or (direction and ((fair_side and fair_side != direction) or (projection_side and projection_side != direction))):
        label = "ÇELİŞKİLİ / PAS"
    elif weak_gap or near_line or ai_score < 40:
        label = "ZAYIF"
    elif ai_score >= 80:
        label = "GÜÇLÜ ADAY"
    elif ai_score >= 60:
        label = "ORTA"
    else:
        label = "ZAYIF"

    reason = _make_reason(
        direction or "Sinyal",
        opening_gap=opening_gap,
        fair_gap=fair_gap,
        projection_gap=projection_gap,
        fair_side=fair_side,
        projection_side=projection_side,
        period=period,
        score_gap=gap,
        signal_count=signal_count,
        is_ai_100=False,
    )
    return {
        "raw_score": _clamp_score(raw),
        "ai_score": ai_score,
        "ai_label": label,
        "ai_reason": reason,
        "final_score": ai_score,
    }
