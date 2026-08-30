"""Explainable signal-quality scoring from immutable signal-time facts.

This module never decides whether an alert should be created.  It only measures
how well the stored score, clock and totals support the alert's existing
direction for dashboard display.
"""

import math

from match_state import current_pace_projection, parse_score


def _number(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _direction(value) -> str:
    text = str(value or "").strip().upper().replace("UST", "ÜST")
    return text if text in {"ALT", "ÜST"} else ""


def _unavailable(reason: str) -> dict:
    return {
        "score": None,
        "label": "Hesaplanamadı",
        "tone": "unavailable",
        "verdict": reason,
        "factors": [],
        "note": "SKS mevcut sinyali değiştirmez; yalnızca eldeki verinin sinyali ne kadar desteklediğini gösterir.",
    }


def _factor_state(value: float, threshold: float) -> str:
    if value >= threshold:
        return "support"
    if value <= -threshold:
        return "against"
    return "neutral"


def calculate_signal_quality(alert: dict) -> dict:
    """Return a 0-100 explanatory score, not a win probability."""
    direction = _direction(alert.get("direction"))
    opening = _number(alert.get("opening"))
    live = _number(alert.get("live"))
    if not direction or opening is None or live is None:
        return _unavailable("Sinyal yönü veya barem bilgisi eksik olduğu için SKS hesaplanamadı.")

    score_text = str(alert.get("score") or "")
    status = str(alert.get("status") or "")
    match_name = str(alert.get("match_name") or "")
    tournament = str(alert.get("tournament") or "")
    pace = current_pace_projection(score_text, status, match_name, tournament)
    score_total = _number(pace.get("score_total"))
    elapsed = _number(pace.get("elapsed_minutes"))
    game_minutes = _number(pace.get("game_minutes"))
    pace_total = _number(pace.get("total"))
    if (
        score_total is None
        or elapsed is None
        or game_minutes is None
        or pace_total is None
        or elapsed <= 0
        or elapsed > game_minutes
    ):
        return _unavailable("Skor veya maç saati güvenilir okunamadığı için SKS hesaplanamadı.")

    remaining = game_minutes - elapsed
    state_fair_total = score_total + opening * (remaining / game_minutes)
    if direction == "ALT":
        state_support = live - state_fair_total
        pace_support = live - pace_total
    else:
        state_support = state_fair_total - live
        pace_support = pace_total - live

    # Score/time correction is the primary evidence.  Current scoring speed is
    # deliberately down-weighted early because a few baskets dominate it.
    state_component = _clamp(state_support * 4.0, -28.0, 28.0)
    pace_reliability = _clamp((elapsed - 2.0) / 18.0, 0.15, 1.0)
    pace_component = _clamp(pace_support * 2.0, -18.0, 18.0) * pace_reliability

    uncertainty_penalty = 0.0
    timing_text = "Maç saati değerlendirme için yeterli ilerlemiş."
    timing_state = "neutral"
    if elapsed < 5:
        uncertainty_penalty += 10.0
        timing_text = "Maç çok erken aşamada; birkaç basket görüntüyü kolayca değiştirebilir."
        timing_state = "against"
    elif elapsed < 10:
        uncertainty_penalty += 5.0
        timing_text = "Maçın erken bölümünde olduğu için değerlendirme temkinli tutuldu."

    home_score, away_score = parse_score(score_text)
    close_late_game = (
        remaining <= 4
        and home_score is not None
        and away_score is not None
        and abs(home_score - away_score) <= 8
    )
    if close_late_game:
        uncertainty_penalty += 9.0
        timing_text = "Maç sonu yakın ve skor yakın; taktik fauller toplamı hızlı değiştirebilir."
        timing_state = "against"

    score = int(round(_clamp(50.0 + state_component + pace_component - uncertainty_penalty, 0.0, 100.0)))
    if score >= 75:
        label, tone = "Güçlü", "strong"
    elif score >= 60:
        label, tone = "İyi", "good"
    elif score >= 45:
        label, tone = "Belirsiz", "uncertain"
    else:
        label, tone = "Zayıf", "weak"

    state_state = _factor_state(state_support, 2.5)
    if state_state == "support":
        state_text = "Skor ve kalan süreye göre canlı barem sinyal yönünde fazla hareket etmiş görünüyor."
    elif state_state == "against":
        state_text = "Skor ve kalan süre bookmaker hareketini büyük ölçüde açıklıyor."
    else:
        state_text = "Skor ve kalan süre belirgin bir bookmaker açığı göstermiyor."

    pace_state = _factor_state(pace_support, 4.0)
    if pace_state == "support":
        pace_text = "Maçın mevcut sayı hızı sinyal yönünü destekliyor."
    elif pace_state == "against":
        pace_text = "Maçın mevcut sayı hızı sinyalin ters yönünü destekliyor."
    else:
        pace_text = "Maçın mevcut sayı hızı iki yönden birini belirgin biçimde desteklemiyor."

    if score >= 75:
        verdict = f"Eldeki skor ve süre verisi {direction} sinyalini güçlü biçimde destekliyor."
    elif score >= 60:
        verdict = f"Eldeki veriler {direction} sinyalini destekliyor, ancak fark kesin değil."
    elif score >= 45:
        verdict = "Bookmaker hareketinin abartıldığına dair yeterince net kanıt yok."
    else:
        verdict = "Maçın mevcut gidişi bu sinyali desteklemiyor."

    return {
        "score": score,
        "label": label,
        "tone": tone,
        "verdict": verdict,
        "factors": [
            {"label": "Skor ve süre", "state": state_state, "text": state_text},
            {"label": "Maçın sayı hızı", "state": pace_state, "text": pace_text},
            {"label": "Zaman güveni", "state": timing_state, "text": timing_text},
        ],
        "note": "SKS kazanma olasılığı değildir. Şut ve pozisyon ayrıntıları mevcut veri akışında olmadığı için tek başına bahis kararı olarak kullanılmamalıdır.",
        "state_adjusted_total": round(state_fair_total, 1),
        "state_support_points": round(state_support, 1),
        "pace_support_points": round(pace_support, 1),
    }
