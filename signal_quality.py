from __future__ import annotations

from projection import game_clock, parse_score


def _safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", ".").strip())
    except (TypeError, ValueError):
        return None


def _safe_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_direction(value) -> str:
    text = str(value or "").strip().upper().replace("UST", "ÜST")
    return text if text in {"ALT", "ÜST"} else ""


def _clamp_score(value: float) -> int:
    return int(round(max(0, min(100, value))))


def _quality_label(score: int) -> str:
    if score >= 80:
        return "YÜKSEK KALİTE"
    if score >= 60:
        return "İZLE"
    if score >= 50:
        return "BELİRSİZ"
    return "PAS"


def _fmt(value, digits: int = 1) -> str:
    number = _safe_float(value)
    return "-" if number is None else f"{number:.{digits}f}"


def _played_minutes(match_data: dict, total_score: int | None) -> tuple[float | None, dict]:
    explicit = _safe_float(match_data.get("played_minutes") or match_data.get("elapsed_minutes"))
    clock = game_clock(
        str(match_data.get("status") or ""),
        str(match_data.get("match_name") or ""),
        str(match_data.get("tournament") or ""),
    )
    if explicit is not None and explicit > 0:
        return explicit, clock

    period = clock.get("period")
    remaining_min = clock.get("remaining_min")
    quarter_length = clock.get("quarter_length") or 10
    if period is not None and remaining_min is not None:
        played = (int(period) - 1) * quarter_length + (quarter_length - float(remaining_min))
        return max(0.0, played), clock

    # Live status but no clock: keep projection limited instead of inventing a
    # precise minute from score. The caller gets an explicit missing-data note.
    if total_score is not None and str(match_data.get("status") or "").strip().lower() == "live":
        return None, clock
    return None, clock


def _minute_phase_delta(clock: dict, played: float | None) -> tuple[int, str | None]:
    period = clock.get("period")
    remaining_min = clock.get("remaining_min")
    quarter_length = float(clock.get("quarter_length") or 10)
    if period is None or remaining_min is None or played is None:
        return 0, None

    elapsed_in_period = max(0.0, quarter_length - float(remaining_min))
    if int(period) == 1:
        if elapsed_in_period < quarter_length * 0.4:
            return -10, "1. periyot erken bölüm: veri henüz oturmamış."
        return -5, "1. periyot sonu: sinyal evresi sınırlı güven verdi."
    if int(period) == 2:
        if elapsed_in_period < quarter_length * 0.35:
            return -5, "2. periyot başı: geçiş evresi sınırlı güven verdi."
        if float(remaining_min) <= quarter_length * 0.35:
            return 5, "Devreye yakın bölüm: veri daha oturmuş görünüyor."
        return 0, "2. periyot orta bölüm: maç evresi nötr."
    if int(period) == 3:
        if elapsed_in_period < quarter_length * 0.35:
            return 5, "3. periyot başı: ikinci yarı verisi oluşuyor."
        return 10, "3. periyot orta/son bölüm: tempo verisi daha güvenilir."
    if int(period) == 4:
        if float(remaining_min) <= 5:
            return -10, "4. periyot son 5 dakika: maç sonu dinamikleri riski artırır."
        return 5, "4. periyot başı: veri oturmuş ancak maç sonu etkisi yaklaşabilir."
    return 0, None


def calculate_signal_quality(match_data: dict) -> dict:
    score_value = 50
    reasons: list[str] = []
    risk_notes: list[str] = []

    direction = _normalize_direction(match_data.get("direction") or match_data.get("final_direction"))
    opening = _safe_float(match_data.get("opening") or match_data.get("opening_total"))
    live = _safe_float(match_data.get("live") or match_data.get("inplay_total"))
    home_score, away_score = parse_score(str(match_data.get("score") or ""))
    total_score = home_score + away_score if home_score is not None and away_score is not None else None
    played, clock = _played_minutes(match_data, total_score)
    total_game_min = _safe_float(match_data.get("total_game_min")) or float(clock.get("total_game_min") or 40)

    missing = []
    if not direction:
        missing.append("sinyal yönü")
    if opening is None:
        missing.append("açılış baremi")
    if live is None:
        missing.append("canlı barem")
    if total_score is None:
        missing.append("skor")
    if played is None or played <= 0:
        missing.append("maç dakikası")
    if missing:
        risk_notes.append("eksik veri nedeniyle kalite sınırlı")
        reasons.append("Eksik veri: " + ", ".join(missing) + ".")

    line_diff = live - opening if opening is not None and live is not None else None
    if line_diff is not None and direction:
        abs_diff = abs(line_diff)
        delta = 0
        if direction == "ALT" and line_diff >= 10:
            delta = 5 if line_diff < 15 else 10 if line_diff < 20 else 15
        elif direction == "ÜST" and line_diff <= -10:
            delta = 5 if line_diff > -15 else 10 if line_diff > -20 else 15
        score_value += delta
        reasons.append(f"Canlı barem açılıştan {line_diff:+.1f} farklı ({delta:+d}).")
        if abs_diff > 30:
            score_value += 5
            risk_notes.append("aşırı barem sapması")
            reasons.append("Mutlak barem farkı 30 puanın üzerinde; ek güven sınırlı tutuldu.")

    minute_delta, minute_reason = _minute_phase_delta(clock, played)
    score_value += minute_delta
    if minute_reason:
        reasons.append(f"{minute_reason} ({minute_delta:+d})")

    current_pace = None
    projection = None
    projection_diff = None
    if total_score is not None and played is not None and played > 0 and live is not None:
        current_pace = total_score / played
        projection = current_pace * total_game_min
        projection_diff = projection - live
        if abs(projection_diff) < 5:
            reasons.append(f"Projeksiyon canlı bareme yakın ({projection_diff:+.1f}).")
        elif direction == "ALT":
            delta = 15 if projection_diff <= -5 else -15
            score_value += delta
            reasons.append(f"Projeksiyon canlı baremin {abs(projection_diff):.1f} puan {'altında' if projection_diff < 0 else 'üstünde'} ({delta:+d}).")
        elif direction == "ÜST":
            delta = 15 if projection_diff >= 5 else -15
            score_value += delta
            reasons.append(f"Projeksiyon canlı baremin {abs(projection_diff):.1f} puan {'üstünde' if projection_diff > 0 else 'altında'} ({delta:+d}).")

    if line_diff is not None and projection is not None and live is not None and direction:
        if direction == "ALT" and line_diff > 10:
            if projection < live - 5:
                score_value += 10
                reasons.append("Açılışa göre yükselen canlı barem, projeksiyon tarafından desteklenmiyor (+10).")
            elif projection > live + 5:
                score_value -= 10
                reasons.append("Canlı barem yükselişi projeksiyon tarafından destekleniyor (-10).")
        elif direction == "ÜST" and line_diff < -10:
            if projection > live + 5:
                score_value += 10
                reasons.append("Açılışa göre düşen canlı barem, projeksiyon tarafından desteklenmiyor (+10).")
            elif projection < live - 5:
                score_value -= 10
                reasons.append("Canlı barem düşüşü projeksiyon tarafından destekleniyor (-10).")

    score_gap = abs(home_score - away_score) if home_score is not None and away_score is not None else None
    if score_gap is not None:
        if score_gap <= 7:
            score_value += 5
            reasons.append("Skor farkı düşük (+5).")
        elif score_gap <= 15:
            reasons.append("Skor farkı orta, nötr.")
        elif score_gap <= 24:
            score_value -= 5
            risk_notes.append("skor farkı açılıyor")
            reasons.append("Skor farkı 16-24 aralığında (-5).")
        else:
            score_value -= 15
            risk_notes.append("kopan maç riski")
            reasons.append("Skor farkı 25 ve üzerinde (-15).")

    required_pace = None
    if live is not None and total_score is not None and played is not None:
        remaining = max(0.0, total_game_min - played)
        if remaining > 0:
            required_pace = (live - total_score) / remaining
            pace_gap = required_pace - current_pace if current_pace is not None else None
            if pace_gap is not None:
                if abs(pace_gap) < 0.20:
                    reasons.append("Gereken tempo mevcut tempoya yakın.")
                elif direction == "ALT":
                    delta = 10 if pace_gap >= 0.20 else -10
                    score_value += delta
                    reasons.append(f"Gereken tempo mevcut tempodan {'yüksek' if pace_gap > 0 else 'düşük'} ({delta:+d}).")
                elif direction == "ÜST":
                    delta = -10 if pace_gap >= 0.20 else 10
                    score_value += delta
                    reasons.append(f"Gereken tempo mevcut tempoya göre {'çok yüksek' if pace_gap > 0 else 'makul/düşük'} ({delta:+d}).")

    league_rate = _safe_float(match_data.get("league_success_rate"))
    if league_rate is not None:
        if league_rate > 70:
            score_value += 10
            reasons.append(f"Lig geçmiş başarı oranı %{league_rate:.1f} (+10).")
        elif league_rate >= 60:
            score_value += 5
            reasons.append(f"Lig geçmiş başarı oranı %{league_rate:.1f} (+5).")
        elif league_rate >= 50:
            reasons.append(f"Lig geçmiş başarı oranı %{league_rate:.1f}, nötr.")
        else:
            score_value -= 10
            risk_notes.append("lig geçmişi zayıf")
            reasons.append(f"Lig geçmiş başarı oranı %{league_rate:.1f} (-10).")

    previous_directions = [
        _normalize_direction(item)
        for item in (match_data.get("previous_directions") or [])
        if _normalize_direction(item)
    ]
    if previous_directions and direction:
        if any(prev != direction for prev in previous_directions):
            score_value -= 15
            risk_notes.append("yön değişimi var")
            reasons.append("Aynı maçta önceki sinyal yönüyle çelişki var (-15).")
        elif any(prev == direction for prev in previous_directions):
            score_value += 5
            reasons.append("Aynı yönde tekrar sinyal var (+5).")
    elif _safe_int(match_data.get("signal_count")) and int(match_data.get("signal_count") or 1) >= 2:
        score_value += 5
        reasons.append("Tekrar sinyal kaydı var (+5).")

    final_score = _clamp_score(score_value)
    label = _quality_label(final_score)
    risk_note = "; ".join(dict.fromkeys(risk_notes)) if risk_notes else ""
    if not risk_note:
        risk_note = "Belirgin ek risk notu yok."

    if not reasons:
        reasons.append("Hesaplanabilir veri sınırlı olduğu için başlangıç puanı korundu.")
    summary = (
        f"Canlı barem açılıştan {_fmt(line_diff)} farklı. "
        f"Projeksiyon canlı baremden {_fmt(projection_diff)} farklı. "
        f"Mevcut tempo {_fmt(current_pace, 2)}, gereken tempo {_fmt(required_pace, 2)}. "
        f"Skor farkı {score_gap if score_gap is not None else '-'}. "
        f"Bu nedenle sinyal kalitesi etiketi {label}."
    )

    return {
        "quality_score": final_score,
        "quality_label": label,
        "projection": round(projection, 1) if projection is not None else None,
        "projection_diff": round(projection_diff, 1) if projection_diff is not None else None,
        "current_pace": round(current_pace, 3) if current_pace is not None else None,
        "required_pace": round(required_pace, 3) if required_pace is not None else None,
        "score_gap": int(score_gap) if score_gap is not None else None,
        "risk_note": risk_note,
        "reason": " ".join(reasons[:8]) + " " + summary,
    }
