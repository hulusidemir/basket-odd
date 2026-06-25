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
    """Return basketball expert evidence, not a claimed win probability."""
    score_value = 50.0
    reasons: list[str] = []
    risk_notes: list[str] = []
    caps: list[int] = []
    components = {
        "data": 0,
        "phase": 0,
        "model_edge": 0,
        "pace_stability": 0,
        "game_script": 0,
        "market_context": 0,
        "history_prior": 0,
    }

    direction = _normalize_direction(match_data.get("direction") or match_data.get("final_direction"))
    opening = _safe_float(match_data.get("opening") or match_data.get("opening_total"))
    prematch = _safe_float(match_data.get("prematch") or match_data.get("prematch_total"))
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
        caps.append(49)
    else:
        components["data"] = 10

    status = str(match_data.get("status") or "").strip()
    if status.upper().startswith("OT"):
        caps.append(39)
        risk_notes.append("uzatma periyodu desteklenmiyor")

    period = clock.get("period")
    remaining_min = _safe_float(clock.get("remaining_min"))
    quarter_length = _safe_float(clock.get("quarter_length")) or 10.0
    if period is None or remaining_min is None:
        caps.append(49)
        risk_notes.append("kesin maç saati yok")
    else:
        components["data"] += 5

    line_diff = live - opening if opening is not None and live is not None else None
    market_reference = prematch if prematch is not None else opening
    market_move = live - market_reference if live is not None and market_reference is not None else None
    if market_move is not None:
        abs_move = abs(market_move)
        if 10 <= abs_move <= 16:
            market_delta = 4
        elif abs_move <= 22:
            market_delta = 0
        elif abs_move <= 30:
            market_delta = -4
        else:
            market_delta = -8
            caps.append(69)
            risk_notes.append("aşırı piyasa hareketi")
        components["market_context"] = market_delta
        score_value += market_delta
        reference_name = "prematch" if prematch is not None else "açılış"
        reasons.append(f"Canlı barem {reference_name} bareminden {market_move:+.1f} farklı ({market_delta:+d}).")

    minute_delta, minute_reason = _minute_phase_delta(clock, played)
    if period == 3:
        minute_delta = 8 if remaining_min is not None and remaining_min <= quarter_length * 0.65 else 3
    elif period == 4 and remaining_min is not None and remaining_min > 5:
        minute_delta = 2
    if period == 1 and played is not None and played < 4:
        caps.append(49)
        risk_notes.append("Q1 ilk 4 dakika")
    if period == 4 and remaining_min is not None and remaining_min <= 5:
        caps.append(59)
        risk_notes.append("Q4 son 5 dakika faul/rotasyon riski")
    components["phase"] = minute_delta
    score_value += minute_delta
    if minute_reason:
        reasons.append(f"{minute_reason} ({minute_delta:+d})")

    current_pace = total_score / played if total_score is not None and played is not None and played > 0 else None
    fallback_projection = current_pace * total_game_min if current_pace is not None else None
    projection = _safe_float(
        match_data.get("pure_projected_total")
        or match_data.get("projected_total")
        or match_data.get("projected")
    )
    advanced_projection = projection is not None
    if projection is None:
        projection = fallback_projection
        caps.append(69)
        risk_notes.append("yalnız ham puan/dakika projeksiyonu var")
    projection_diff = None
    signed_model_edge = None
    fair_line = _safe_float(match_data.get("fair_line"))
    model_line = fair_line if fair_line is not None else projection
    if model_line is not None and live is not None and direction:
        projection_diff = projection - live
        signed_model_edge = model_line - live if direction == "ÜST" else live - model_line
        if signed_model_edge < 0:
            model_delta = -20
        elif signed_model_edge < 3:
            model_delta = -8
        elif signed_model_edge < 5:
            model_delta = 4
        elif signed_model_edge < 8:
            model_delta = 12
        else:
            model_delta = 16
        if not advanced_projection:
            model_delta = max(-10, min(8, model_delta))
        components["model_edge"] = model_delta
        score_value += model_delta
        reasons.append(f"Yön işaretli model avantajı {signed_model_edge:+.1f} ({model_delta:+d}).")

    projection_quality = _safe_float(match_data.get("projection_quality"))
    if projection_quality is None:
        caps.append(69)
        risk_notes.append("projeksiyon kalite ölçümü yok")
    else:
        if projection_quality < 60:
            caps.append(49)
            risk_notes.append("projeksiyon kalitesi düşük")
        elif projection_quality < 70:
            caps.append(59)
        elif projection_quality < 85:
            caps.append(79)

    score_gap = abs(home_score - away_score) if home_score is not None and away_score is not None else None
    script_delta = 0
    if score_gap is not None and period is not None and remaining_min is not None:
        remaining_game = max(0.1, total_game_min - float(played or 0))
        if period >= 3 and score_gap / remaining_game >= 2.0:
            script_delta = 6 if direction == "ALT" else -12
            risk_notes.append("garbage-time senaryosu")
        elif period == 4 and 5 < remaining_min <= 8 and score_gap <= 7:
            script_delta = 5 if direction == "ÜST" else -8
            risk_notes.append("yakın maç faul senaryosu")
    components["game_script"] = script_delta
    score_value += script_delta
    if script_delta:
        reasons.append(f"Skor farkı ve kalan süre senaryosu ({script_delta:+d}).")

    required_pace = None
    if live is not None and total_score is not None and played is not None:
        remaining = max(0.0, total_game_min - played)
        if remaining > 0:
            required_pace = (live - total_score) / remaining
    sustainable_pace = _safe_float(match_data.get("sustainable_ppm"))
    if sustainable_pace is None:
        projection_components = match_data.get("projection_components")
        if isinstance(projection_components, dict):
            sustainable_pace = _safe_float(projection_components.get("sustainable_pace_per_min"))
    if current_pace and sustainable_pace and sustainable_pace > 0 and direction:
        deviation = (current_pace - sustainable_pace) / sustainable_pace
        regression_direction = "ALT" if deviation > 0 else "ÜST"
        magnitude = abs(deviation)
        if magnitude >= 0.20:
            pace_delta = 12 if regression_direction == direction else -18
        elif magnitude >= 0.10:
            pace_delta = 8 if regression_direction == direction else -12
        else:
            pace_delta = 0
        # Model edge and pace stability share information; cap their combined upside.
        if components["model_edge"] > 0:
            pace_delta = min(pace_delta, max(0, 20 - components["model_edge"]))
        components["pace_stability"] = pace_delta
        score_value += pace_delta
        reasons.append(f"Sürdürülebilir tempoya dönüş {regression_direction} yönünde ({pace_delta:+d}).")
    else:
        caps.append(69)
        risk_notes.append("tamamlanmış periyot temposu yok")

    league_rate = _safe_float(match_data.get("league_success_rate"))
    league_samples = _safe_int(match_data.get("league_success_samples")) or 0
    if league_rate is not None and league_samples >= 30:
        weight = league_samples / (league_samples + 50.0)
        shrunk_rate = 51.0 + weight * (league_rate - 51.0)
        history_delta = 5 if shrunk_rate >= 60 else -5 if shrunk_rate < 45 else 0
        components["history_prior"] = history_delta
        score_value += history_delta
        reasons.append(f"Geçmişe doğru küçültülmüş lig oranı %{shrunk_rate:.1f} ({history_delta:+d}).")

    previous_directions = [
        _normalize_direction(item)
        for item in (match_data.get("previous_directions") or [])
        if _normalize_direction(item)
    ]
    if previous_directions and direction:
        if any(prev != direction for prev in previous_directions):
            score_value -= 12
            caps.append(59)
            risk_notes.append("yön değişimi var")
            reasons.append("Aynı maçta önceki sinyal yönüyle çelişki var (-12 ve güven tavanı).")

    if signed_model_edge is None or signed_model_edge < 5:
        caps.append(79)
    final_score = _clamp_score(score_value)
    if caps:
        final_score = min(final_score, min(caps))

    label = f"DENEYSEL / {_quality_label(final_score)}"
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
        "score_version": "sk-v2.0-expert",
        "score_type": "expert_evidence",
        "validation_status": "shadow",
        "quality_score": final_score,
        "quality_label": label,
        "components": components,
        "quality_cap": min(caps) if caps else 100,
        "projection": round(projection, 1) if projection is not None else None,
        "projection_diff": round(projection_diff, 1) if projection_diff is not None else None,
        "current_pace": round(current_pace, 3) if current_pace is not None else None,
        "required_pace": round(required_pace, 3) if required_pace is not None else None,
        "score_gap": int(score_gap) if score_gap is not None else None,
        "risk_note": risk_note,
        "reason": " ".join(reasons[:8]) + " " + summary,
    }
