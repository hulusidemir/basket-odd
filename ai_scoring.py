"""
AI-style post signal scoring layer.

calculate_ai_score  — sinyal kalitesi skoru (0-100), etiket, açıklama
calculate_bet_recommendation — veri analizine dayalı bahis yönü önerisi
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


def calculate_ai_score(
    signal: dict, analysis: dict | None = None, raw_score: float | None = None
) -> dict:
    """
    Sinyal kalite skoru.  Aynı return arayüzü korunuyor:
    raw_score, ai_score, ai_label, ai_reason, final_score.

    Değişiklikler (eski sisteme göre):
    - ÜST base skoru 40'tan başlıyor (geçmiş %47.7 başarı oranını yansıtır)
    - Q1 ALT bonus'u kaldırıldı (Q1 projeksiyon MAE=36 — güvenilmez)
    - Anlık skor farkı (score_gap) ALT/ÜST her ikisi için kullanılıyor
    - fair_edge ağırlıkları gerçek veri analizine göre yeniden kalibre edildi
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

    opening_gap = (live - opening) if live is not None and opening is not None else None
    fair_side = _side(fair_line, live)
    projection_side = _side(projection, live)
    fair_abs_gap = (
        abs((fair_line or 0) - (live or 0))
        if fair_line is not None and live is not None
        else None
    )

    if direction == "ALT":
        fair_gap = live - fair_line if live is not None and fair_line is not None else None
        projection_gap = live - projection if live is not None and projection is not None else None
        opening_strength = opening_gap
        base = 50.0
    elif direction == "ÜST":
        fair_gap = fair_line - live if live is not None and fair_line is not None else None
        projection_gap = projection - live if live is not None and projection is not None else None
        opening_strength = -opening_gap if opening_gap is not None else None
        base = 40.0  # ÜST sinyalleri geçmişte %47.7 başarı — düşük başlangıç
    else:
        fair_gap = projection_gap = opening_strength = None
        base = 50.0

    score = base

    if direction == "ALT":
        # fair_edge en önemli gösterge
        if fair_gap is not None:
            if fair_gap >= 10:
                score += 30
            elif fair_gap >= 7:
                score += 20
            elif fair_gap >= 5:
                score += 15
            elif fair_gap >= 3:
                score += 8
            elif fair_gap >= 0:
                score += 3
            elif fair_gap >= -3:
                score -= 15
            else:
                score -= 30  # adil barem ALT ile açıkça çelişiyor

        # Periyot — Q1 güvenilmez, Q3 en iyi
        if period == 1:
            score -= 20
        elif period == 2:
            score += 2
        elif period == 3:
            score += 10
        elif period == 4:
            score += 5

        # Anlık skor farkı
        if gap is not None:
            if gap <= 5:
                score -= 5   # yakın maç = ÜST riski
            elif gap <= 10:
                score += 5
            elif gap > 20:
                score -= 15  # blowout = ALT başarısı %45 → kötü

        # Diff büyüklüğü
        if opening_strength is not None:
            if opening_strength >= 18:
                score += 10
            elif opening_strength >= 12:
                score += 5

    elif direction == "ÜST":
        # fair_edge
        if fair_gap is not None:
            if fair_gap >= 8:
                score += 25
            elif fair_gap >= 5:
                score += 15
            elif fair_gap >= 3:
                score += 8
            elif fair_gap >= 0:
                score += 2
            elif fair_gap >= -5:
                score -= 20
            else:
                score -= 30  # adil barem ÜST ile açıkça çelişiyor

        # Periyot
        if period == 4:
            score += 15
        elif period == 3:
            score += 8
        elif period == 2:
            score += 5
        elif period == 1:
            score += 0  # Q1 ÜST için nötr

        # Anlık skor farkı — yakın maç ÜST için iyi
        if gap is not None:
            if gap <= 5:
                score += 15
            elif gap <= 10:
                score += 5
            elif gap > 15:
                score -= 20
            elif gap > 20:
                score -= 30

        # Diff
        if opening_strength is not None:
            if opening_strength >= 18:
                score += 5
            elif opening_strength >= 12:
                score += 2

    # Sinyal tekrar cezası
    if signal_count >= 3:
        score -= 15
    elif signal_count >= 2:
        score -= 5

    # Adil barem canlıya çok yakınsa → belirsiz
    if fair_abs_gap is not None and fair_abs_gap < 3:
        score -= 10

    ai_score = _clamp_score(score)
    conflict = bool(fair_side and projection_side and fair_side != projection_side)

    if conflict:
        label = "ÇELİŞKİLİ / PAS"
    elif direction == "ALT":
        if ai_score >= 80:
            label = "GÜÇLÜ ALT"
        elif ai_score >= 65:
            label = "ORTA ALT"
        elif ai_score >= 50:
            label = "ZAYIF ALT"
        else:
            label = "ALT RİSKLİ"
    elif direction == "ÜST":
        if ai_score >= 70:
            label = "GÜÇLÜ ÜST"
        elif ai_score >= 55:
            label = "ORTA ÜST"
        elif ai_score >= 40:
            label = "ZAYIF ÜST"
        else:
            label = "ÜST RİSKLİ"
    else:
        label = "ZAYIF"

    # Reason
    pieces = []
    if opening_gap is not None:
        pieces.append(f"canlı-açılış farkı {opening_gap:+.1f}")
    if fair_gap is not None:
        pieces.append(f"adil barem {_format_delta(fair_gap)} puan {'aşağıda' if fair_gap >= 0 and direction == 'ALT' else 'yukarıda' if fair_gap >= 0 and direction == 'ÜST' else 'çelişkili'}")
    if period:
        pieces.append(f"Q{period}")
    if gap is not None:
        pieces.append(f"skor farkı {gap}")
    if signal_count > 1:
        pieces.append(f"#{signal_count}. tekrar")

    if conflict:
        reason = f"Adil barem {fair_side}, projeksiyon {projection_side} gösteriyor — çelişki var."
    elif pieces:
        reason = "; ".join(pieces) + f". {label}."
    else:
        reason = f"{label}."

    if signal_count > 1 and not conflict:
        reason += f" Tekrar sinyal #{signal_count}."

    raw = _safe_float(raw_score) or _safe_float(signal.get("raw_score")) or 50.0

    return {
        "raw_score": _clamp_score(raw),
        "ai_score": ai_score,
        "ai_label": label,
        "ai_reason": reason,
        "final_score": ai_score,
    }


# ---------------------------------------------------------------------------
# Bahis Öneri Motoru — Seçenek B
# ---------------------------------------------------------------------------

def calculate_bet_recommendation(
    signal: dict, analysis: dict | None = None
) -> dict:
    """
    995 silinmiş sinyal üzerinde sistematik grid search ile kalibre edilmiş
    bahis yönü öneri motoru.

    Veri keşfi: hiçbir geniş koşul altında 'ÜST kazanır' için %70+ pattern
    bulunamadı (n>=15). Pazar canlı baremleri ÜST'e doğru sistematik
    sapma gösteriyor → tüm aktif öneriler ALT yönlü.

    Sıralama mantığı: önce en spesifik (yüksek doğrulanmış hit) kurallar,
    sonra geniş kapsama kuralları. İlk eşleşen kural kullanılır.

    Döndürdüğü alanlar:
        bet_dir        : 'ALT' | 'PAS'
        bet_label      : 'GÜÇLÜ ALT' | 'KONTRA ALT' | 'ZAYIF ALT' | 'PAS'
        bet_confidence : doğrulanmış geçmiş başarı oranı (0-100)
        bet_rule       : kural kodu (B1..B12)
        bet_reason     : kullanıcıya gösterilen açıklama
    """
    analysis = analysis or {}
    direction = _direction(signal.get("direction") or analysis.get("direction"))
    period = _period(signal, analysis)
    fair_edge = _safe_float(analysis.get("fair_edge", signal.get("fair_edge")))

    opening = _safe_float(signal.get("opening_total", signal.get("opening")))
    live = _safe_float(signal.get("inplay_total", signal.get("live")))
    diff = abs(live - opening) if live is not None and opening is not None else 0.0

    gap = _score_gap(str(signal.get("score") or ""))

    def _r(bet_dir, label, confidence, rule, reason):
        return {
            "bet_dir": bet_dir,
            "bet_label": label,
            "bet_confidence": confidence,
            "bet_rule": rule,
            "bet_reason": reason,
        }

    # ===================================================================
    # ÜST SİNYALLERİ — hepsi KONTRA ALT (ÜST tarafında %70+ pattern yok)
    # ===================================================================
    if direction == "ÜST":

        # B1 — ÜST + Q2 + 8≤gap≤20 + -5≤fe<0 → ALT %76.7 (n=30)
        if (
            period == 2
            and gap is not None and 8 <= gap <= 20
            and fair_edge is not None and -5 <= fair_edge < 0
        ):
            return _r(
                "ALT", "KONTRA ALT", 77, "B1",
                f"ÜST sinyal ama adil barem ALT'ta ({fair_edge:+.1f}), Q2 + dengeli skor farkı ({gap}). "
                "İki gösterge ALT'ı işaret ediyor. Geçmiş: 30 sinyal, %76.7 başarı.",
            )

        # B2 — ÜST + Q1/Q2 + gap≥10 + -5≤fe<0 → ALT %81.0 (n=42, en güçlü)
        if (
            period in (1, 2)
            and gap is not None and gap >= 10
            and fair_edge is not None and -5 <= fair_edge < 0
        ):
            return _r(
                "ALT", "KONTRA ALT", 81, "B2",
                f"ÜST sinyal + erken periyot (Q{period}) + skor farkı {gap} + adil barem hafif ALT ({fair_edge:+.1f}). "
                "En güvenilir KONTRA ALT paterni. Geçmiş: 42 sinyal, %81 başarı.",
            )

        # B3 — ÜST + Q1 + gap≥10 + diff≥10 → ALT %73.0 (n=37)
        if (
            period == 1
            and gap is not None and gap >= 10
            and diff >= 10
        ):
            return _r(
                "ALT", "KONTRA ALT", 73, "B3",
                f"ÜST sinyal + Q1 + skor farkı {gap} + market hareketi (diff {diff:.0f}). "
                "Erken blowout senaryosu, tempo düşer. Geçmiş: 37 sinyal, %73 başarı.",
            )

        # ÜST sinyaller için diğer tüm durumlar → PAS
        return _r(
            "PAS", "PAS", 0, "U_DEFAULT",
            "ÜST sinyali için doğrulanmış %70+ pattern eşleşmedi. Pas geçmek önerilir.",
        )

    # ===================================================================
    # ALT SİNYALLERİ — hepsi ALT (KONTRA ÜST kuralları %47'de patladı)
    # ===================================================================
    if direction == "ALT":

        # B6 — ALT + Q2/Q3 + 5<gap≤15 + diff≥20 → ALT %80.8 (n=26, en güçlü ALT)
        if (
            period in (2, 3)
            and gap is not None and 5 < gap <= 15
            and diff >= 20
        ):
            return _r(
                "ALT", "GÜÇLÜ ALT", 81, "B6",
                f"ALT sinyal + Q{period} + dengeli üstü fark ({gap}) + büyük market hareketi (diff {diff:.0f}). "
                "En güvenilir ALT paterni. Geçmiş: 26 sinyal, %80.8 başarı.",
            )

        # B7 — ALT + Q3 + diff≥20 → ALT %77.8 (n=36)
        if period == 3 and diff >= 20:
            return _r(
                "ALT", "GÜÇLÜ ALT", 78, "B7",
                f"ALT sinyal + Q3 + büyük market düşüşü (diff {diff:.0f}). "
                "Pazar tempoyu doğru fiyatlamış. Geçmiş: 36 sinyal, %77.8 başarı.",
            )

        # B8 — ALT + gap≤5 + fe≤-5 + diff<15 → ALT %75.8 (n=33)
        if (
            gap is not None and gap <= 5
            and fair_edge is not None and fair_edge <= -5
            and diff < 15
        ):
            return _r(
                "ALT", "GÜÇLÜ ALT", 76, "B8",
                f"ALT sinyal + yakın maç (fark {gap}) + adil barem {fair_edge:.1f} + ölçülü market (diff {diff:.0f}). "
                "Yakın maçta düşük tempo onayı. Geçmiş: 33 sinyal, %75.8 başarı.",
            )

        # B9 — ALT + Q2 + gap≥10 + fe≤-3 → ALT %72.0 (n=50)
        if (
            period == 2
            and gap is not None and gap >= 10
            and fair_edge is not None and fair_edge <= -3
        ):
            return _r(
                "ALT", "ORTA ALT", 72, "B9",
                f"ALT sinyal + Q2 + skor farkı {gap} + adil barem {fair_edge:.1f}. "
                "Geçmiş: 50 sinyal, %72 başarı.",
            )

        # ALT sinyaller için kalan durumlar → PAS
        return _r(
            "PAS", "PAS", 0, "A_DEFAULT",
            "ALT sinyali için doğrulanmış %70+ pattern eşleşmedi. Pas geçmek önerilir.",
        )

    return _r("PAS", "PAS", 0, "UNKNOWN", "Sinyal yönü belirlenemedi.")
