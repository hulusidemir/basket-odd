import re
from statistics import mean

from projection import calculate_projected_total, game_clock, parse_score


def _split_match_name(match_name: str) -> tuple[str, str]:
    parts = [part.strip() for part in (match_name or "").split(" - ", 1)]
    if len(parts) == 2:
        return parts[0], parts[1]
    return match_name or "Home", "Away"


def _safe_float(value) -> float | None:
    if value is None:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", str(value).replace(",", "."))
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _parse_stat_pair(raw_value: str) -> dict:
    text = str(raw_value or "").strip()
    data = {"raw": text, "value": _safe_float(text), "made": None, "attempts": None, "pct": None}

    slash = re.match(r"\s*(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)\s*$", text)
    if slash:
        data["made"] = float(slash.group(1))
        data["attempts"] = float(slash.group(2))
        return data

    pct = re.match(r"\s*(\d+(?:\.\d+)?)\s*%\s*$", text)
    if pct:
        data["pct"] = float(pct.group(1))
        return data

    return data


def _canonical_stat_label(label: str) -> str | None:
    value = (label or "").strip().lower()
    mappings = {
        "fg_pct": ["field goal", "fg%"],
        "fg": ["field goals", "fg made"],
        "three_pct": ["3-point", "three-point", "3 pt", "3pts", "three points %"],
        "three": ["3-point field goals", "three-point field goals", "3-pointers", "3 pointers"],
        "two_pct": ["2-point", "two-point"],
        "two": ["2-point field goals", "two-point field goals"],
        "ft_pct": ["free throw %", "ft%"],
        "ft": ["free throws", "free-throws"],
        "rebounds": ["rebounds", "total rebounds"],
        "off_rebounds": ["offensive rebounds"],
        "def_rebounds": ["defensive rebounds"],
        "assists": ["assists"],
        "turnovers": ["turnovers", "turnover"],
        "steals": ["steals"],
        "blocks": ["blocks"],
        "fouls": ["personal fouls", "fouls", "personal foul"],
        "timeouts": ["timeouts", "time outs"],
    }
    for canonical, keywords in mappings.items():
        if any(keyword in value for keyword in keywords):
            return canonical
    return None


def _build_stat_metrics(rows: list[dict]) -> dict:
    parsed = {}
    for row in rows or []:
        canonical = _canonical_stat_label(row.get("label", ""))
        if not canonical or canonical in parsed:
            continue
        parsed[canonical] = {
            "home": _parse_stat_pair(row.get("home")),
            "away": _parse_stat_pair(row.get("away")),
            "label": row.get("label", canonical),
        }

    def pct_average(key: str) -> float | None:
        item = parsed.get(key)
        if not item:
            return None
        values = [item["home"].get("pct") or item["home"].get("value"), item["away"].get("pct") or item["away"].get("value")]
        values = [v for v in values if v is not None]
        return round(mean(values), 1) if values else None

    def attempts_total(key: str) -> float | None:
        item = parsed.get(key)
        if not item:
            return None
        attempts = [item["home"].get("attempts"), item["away"].get("attempts")]
        attempts = [v for v in attempts if v is not None]
        return round(sum(attempts), 1) if attempts else None

    def values_total(key: str) -> float | None:
        item = parsed.get(key)
        if not item:
            return None
        values = [item["home"].get("value"), item["away"].get("value")]
        values = [v for v in values if v is not None]
        return round(sum(values), 1) if values else None

    return {
        "rows_found": len(parsed),
        "fg_pct_avg": pct_average("fg_pct"),
        "three_pct_avg": pct_average("three_pct"),
        "ft_pct_avg": pct_average("ft_pct"),
        "total_fga": attempts_total("fg"),
        "total_three_pa": attempts_total("three"),
        "total_fta": attempts_total("ft"),
        "total_rebounds": values_total("rebounds"),
        "total_turnovers": values_total("turnovers"),
        "total_fouls": values_total("fouls"),
        "total_assists": values_total("assists"),
        "raw": parsed,
    }


def _extract_h2h_metrics(body_text: str, match_name: str) -> dict:
    text = re.sub(r"\s+", " ", body_text or "").strip()
    home_team, away_team = _split_match_name(match_name)

    def team_metrics(team_name: str) -> dict:
        escaped = re.escape(team_name)
        pattern = (
            rf"Last 5[, ]+{escaped}.*?"
            rf"(\d+(?:\.\d+)?) points per match,\s*"
            rf"(\d+(?:\.\d+)?) opponent points per game,.*?"
            rf"Total points over%:\s*(\d+(?:\.\d+)?)%"
        )
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            return {}
        ppg = float(match.group(1))
        oppg = float(match.group(2))
        return {
            "team": team_name,
            "ppg": ppg,
            "oppg": oppg,
            "avg_total": round(ppg + oppg, 1),
            "over_pct": float(match.group(3)),
        }

    h2h_over = None
    h2h_games = None
    h2h_match = re.search(
        r"Past H2H Results.*?Total Points Over%:\s*(\d+(?:\.\d+)?)%",
        text,
        re.IGNORECASE,
    )
    if h2h_match:
        h2h_over = float(h2h_match.group(1))

    total_games_match = re.search(r"Total Matches\s+(\d+)", text, re.IGNORECASE)
    if total_games_match:
        h2h_games = int(total_games_match.group(1))

    home_metrics = team_metrics(home_team)
    away_metrics = team_metrics(away_team)

    expected_total = None
    if home_metrics and away_metrics:
        expected_total = round(mean([
            home_metrics["avg_total"],
            away_metrics["avg_total"],
        ]), 1)

    return {
        "home_last5": home_metrics,
        "away_last5": away_metrics,
        "h2h_over_pct": h2h_over,
        "h2h_games": h2h_games,
        "expected_total": expected_total,
    }


def _team_profile_label(avg_total: float | None, over_pct: float | None) -> str:
    """Takımın son 5 maç ortalama toplam + over eğilimini tek etikete çevir."""
    if avg_total is None:
        return "veri yok"
    if avg_total >= 180:
        scoring = "Yüksek skorlu"
    elif avg_total >= 160:
        scoring = "Orta skorlu"
    elif avg_total >= 140:
        scoring = "Düşük-orta skorlu"
    else:
        scoring = "Düşük skorlu"
    if over_pct is None:
        return scoring
    if over_pct >= 60:
        return f"{scoring} · over eğilimli"
    if over_pct <= 40:
        return f"{scoring} · under eğilimli"
    return f"{scoring} · dengeli"


def build_team_context(h2h_metrics: dict, live: float, direction: str) -> dict | None:
    """Sinyalin maçtaki takım geçmişiyle uyumunu çıkarır.
    Döndürdüğü sözlük hem notifier hem dashboard tarafından gösterilir."""
    home = h2h_metrics.get("home_last5") or {}
    away = h2h_metrics.get("away_last5") or {}
    if not home and not away:
        return None

    expected = h2h_metrics.get("expected_total")
    regression_note = None
    regression_direction = None
    regression_delta = None
    if expected is not None and live is not None:
        delta = round(live - expected, 1)
        regression_delta = delta
        if delta >= 4:
            regression_direction = "ALT"
            regression_note = (
                f"Son 5 maç ortalaması {expected:.1f}, canlı barem {live:.1f} "
                f"(+{delta:.1f}). Ortalamaya dönüş ALT tarafını destekler."
            )
        elif delta <= -4:
            regression_direction = "ÜST"
            regression_note = (
                f"Son 5 maç ortalaması {expected:.1f}, canlı barem {live:.1f} "
                f"({delta:.1f}). Ortalamaya dönüş ÜST tarafını destekler."
            )
        else:
            regression_note = (
                f"Son 5 maç ortalaması {expected:.1f}, canlı barem {live:.1f} "
                f"({delta:+.1f}). Barem tarihsel profile yakın."
            )

    def profile(raw: dict) -> dict | None:
        if not raw:
            return None
        return {
            "team": raw.get("team", "-"),
            "avg_total": raw.get("avg_total"),
            "ppg": raw.get("ppg"),
            "oppg": raw.get("oppg"),
            "over_pct": raw.get("over_pct"),
            "label": _team_profile_label(raw.get("avg_total"), raw.get("over_pct")),
        }

    home_profile = profile(home)
    away_profile = profile(away)

    h2h_games = h2h_metrics.get("h2h_games")
    h2h_over = h2h_metrics.get("h2h_over_pct")
    h2h_note = None
    if h2h_over is not None:
        game_part = f"{int(h2h_games)} maç" if h2h_games else "geçmiş maçlar"
        if h2h_over >= 60:
            tilt = "ÜST tarafına eğilimli"
        elif h2h_over <= 40:
            tilt = "ALT tarafına eğilimli"
        else:
            tilt = "dengeli"
        h2h_note = f"Karşılaşma geçmişi ({game_part}): %{h2h_over:.0f} over — {tilt}."

    # Sinyale destek mi, karşı mı?
    support_points = 0
    against_points = 0
    avg_over = None
    if home.get("over_pct") is not None and away.get("over_pct") is not None:
        avg_over = (home["over_pct"] + away["over_pct"]) / 2
        if direction == "ÜST":
            if avg_over >= 60:
                support_points += 1
            elif avg_over <= 40:
                against_points += 1
        else:
            if avg_over <= 40:
                support_points += 1
            elif avg_over >= 60:
                against_points += 1
    if regression_direction:
        if regression_direction == direction:
            support_points += 1
        else:
            against_points += 1
    if h2h_over is not None:
        if direction == "ÜST" and h2h_over >= 60:
            support_points += 1
        elif direction == "ALT" and h2h_over <= 40:
            support_points += 1
        elif direction == "ÜST" and h2h_over <= 40:
            against_points += 1
        elif direction == "ALT" and h2h_over >= 60:
            against_points += 1

    if support_points == 0 and against_points == 0:
        alignment = "Nötr"
        alignment_code = "neutral"
    elif support_points > against_points:
        alignment = f"Takım profili {direction} sinyalini destekliyor"
        alignment_code = "support"
    elif against_points > support_points:
        alignment = f"Takım profili {direction} sinyaline karşı"
        alignment_code = "against"
    else:
        alignment = "Takım profili karışık sinyal veriyor"
        alignment_code = "mixed"

    return {
        "expected_total": expected,
        "regression_delta": regression_delta,
        "regression_direction": regression_direction,
        "regression_note": regression_note,
        "home_profile": home_profile,
        "away_profile": away_profile,
        "h2h_games": h2h_games,
        "h2h_over_pct": h2h_over,
        "h2h_note": h2h_note,
        "avg_last5_over_pct": round(avg_over, 1) if avg_over is not None else None,
        "alignment": alignment,
        "alignment_code": alignment_code,
        "signal_direction": direction,
    }


def _derive_setup_name(direction: str, tags: set[str]) -> str:
    if direction == "ALT":
        if "hot_shooting" in tags:
            return "Sicak Sut Regresyonu ALT"
        if "blowout" in tags:
            return "Blowout Game Script ALT"
        if "low_foul" in tags:
            return "Durgun Tempo ALT"
        return "Market Sapmasi ALT"

    if "clutch_over" in tags:
        return "Faul Oyunu UST"
    if "cold_shooting" in tags and "shot_volume" in tags:
        return "Tempo Destekli UST"
    if "history_over" in tags:
        return "Profil Destekli UST"
    return "Market Sapmasi UST"


def _build_counter_signal(
    direction: str,
    live: float,
    projected_total: float | None,
    period: int | None,
    remaining_min: float | None,
    score_gap: int | None,
    avg_over: float | None,
    expected_total: float | None,
    h2h_over_pct: float | None,
) -> dict:
    counter_direction = "ÜST" if direction == "ALT" else "ALT"
    pressure_score = 0.0
    reasons = []

    if projected_total is not None:
        if counter_direction == "ALT":
            projection_edge = live - projected_total
            if projection_edge >= 8:
                pressure_score += 4
                reasons.append(f"Projeksiyon canlı baremin belirgin altında ({projected_total:.1f})")
            elif projection_edge >= 4:
                pressure_score += 3
                reasons.append(f"Projeksiyon ALT tarafına kayıyor ({projected_total:.1f})")
            elif projection_edge >= 1:
                pressure_score += 1.5
        else:
            projection_edge = projected_total - live
            if projection_edge >= 8:
                pressure_score += 4
                reasons.append(f"Projeksiyon canlı baremin belirgin üstünde ({projected_total:.1f})")
            elif projection_edge >= 4:
                pressure_score += 3
                reasons.append(f"Projeksiyon ÜST tarafına kayıyor ({projected_total:.1f})")
            elif projection_edge >= 1:
                pressure_score += 1.5

    if counter_direction == "ALT":
        if score_gap is not None and score_gap >= 15:
            pressure_score += 2
            reasons.append(f"Skor farki {score_gap} puan, oyun kopma riski var")
        if period == 4 and remaining_min is not None and remaining_min <= 6:
            pressure_score += 1.5
            reasons.append("Son bolumde tempo dususu ALT lehine olabilir")
    else:
        if period == 4 and score_gap is not None and score_gap <= 8:
            pressure_score += 2.5
            reasons.append("Mac yakin, son bolum faul oyunu USTe donebilir")
        if period == 4 and remaining_min is not None and remaining_min <= 6:
            pressure_score += 1.5
            reasons.append("Son bolum ekstra pozisyon ve faul riski var")

    if avg_over is not None:
        if counter_direction == "ALT" and avg_over <= 40:
            pressure_score += 1.5
            reasons.append(f"Son 5 total profili daha dusuk skorlu ({avg_over:.0f}% over)")
        elif counter_direction == "ÜST" and avg_over >= 60:
            pressure_score += 1.5
            reasons.append(f"Son 5 total profili daha yuksek skorlu ({avg_over:.0f}% over)")

    if expected_total is not None:
        if counter_direction == "ALT" and expected_total <= live - 4:
            pressure_score += 1.5
            reasons.append(f"Tarihsel total profili daha asagida ({expected_total:.1f})")
        elif counter_direction == "ÜST" and expected_total >= live + 4:
            pressure_score += 1.5
            reasons.append(f"Tarihsel total profili daha yukarida ({expected_total:.1f})")

    if h2h_over_pct is not None:
        if counter_direction == "ALT" and h2h_over_pct <= 40:
            pressure_score += 1
        elif counter_direction == "ÜST" and h2h_over_pct >= 60:
            pressure_score += 1

    if pressure_score >= 6:
        level = "YÜKSEK"
    elif pressure_score >= 3:
        level = "ORTA"
    elif pressure_score >= 1.5:
        level = "DÜŞÜK"
    else:
        level = "YOK"

    note = ""
    if level != "YOK" and reasons:
        note = f"{counter_direction} tarafi daha mantikli olabilir: {reasons[0]}"

    return {
        "direction": counter_direction,
        "level": level,
        "score": round(pressure_score, 1),
        "note": note,
        "reasons": reasons[:3],
    }


def assess_signal_quality(match: dict, context: dict, threshold: float) -> dict:
    direction = match["direction"]
    opening = float(match["opening_total"])
    live = float(match["inplay_total"])
    baseline = float(match.get("baseline") or opening)
    baseline_label = str(match.get("baseline_label") or "Açılış")
    diff = abs(live - baseline)
    match_name = match.get("match_name", "")
    tournament = match.get("tournament", "")
    status = match.get("status", "")
    score = match.get("score", "")
    locked = bool(match.get("market_locked"))

    clock = game_clock(status, match_name, tournament)
    period = clock["period"]
    remaining_min = clock["remaining_min"]
    home_score, away_score = parse_score(score)
    score_gap = abs(home_score - away_score) if home_score is not None and away_score is not None else None

    h2h_metrics = _extract_h2h_metrics((context.get("h2h") or {}).get("body_text", ""), match_name)
    projected_total = calculate_projected_total(score, status, match_name, tournament)

    score_value = 50.0
    reasons = []
    risks = []
    tags = set()
    source_count = 1

    diff_ratio = diff / max(float(threshold or 1), 1.0)
    if diff_ratio >= 1.7:
        score_value += 14
        reasons.append(f"Market sapmasi cok guclu: {baseline_label}-canli farki {diff:.1f}")
    elif diff_ratio >= 1.4:
        score_value += 10
        reasons.append(f"Market sapmasi guclu: {baseline_label}-canli farki {diff:.1f}")
    elif diff_ratio >= 1.15:
        score_value += 6
        reasons.append(f"Market sapmasi esitigin ustunde: {baseline_label}-canli farki {diff:.1f}")

    if period == 2:
        score_value += 7
        reasons.append("Q2 veri penceresi saglikli")
    elif period == 3:
        score_value += 10
        reasons.append("Q3 ana fiyatlama penceresi")
    elif period == 1:
        score_value -= 7
        risks.append("Q1 sinyali daha oynak")
    elif period == 4:
        score_value -= 4
        risks.append("Q4 sonu varyansi yuksek")
        if remaining_min is not None and remaining_min <= 6:
            score_value -= 5
            risks.append("Son bolum faul ve clock management etkisi artiyor")
    else:
        score_value -= 8
        risks.append("Periyot/sure bilgisi net degil")

    if locked:
        score_value -= 4
        risks.append("Market satirlari kilitli/suspended gorundu")

    if projected_total is not None:
        source_count += 1
        if direction == "ALT":
            projected_edge = live - projected_total
            if projected_edge >= 8:
                score_value += 12
                reasons.append(f"Tempo projeksiyonu ALTi guclu destekliyor ({projected_total:.1f} < {live:.1f})")
            elif projected_edge >= 4:
                score_value += 8
                reasons.append(f"Projeksiyon ALT yonunde ({projected_total:.1f})")
            elif projected_edge >= 1:
                score_value += 4
            else:
                penalty = -4 if period == 1 else (-7 if period == 2 else -10)
                score_value += penalty
                risks.append(f"Projeksiyon ALT ile ayni hizada degil ({projected_total:.1f})")
        else:
            projected_edge = projected_total - live
            if projected_edge >= 8:
                score_value += 12
                reasons.append(f"Tempo projeksiyonu USTu guclu destekliyor ({projected_total:.1f} > {live:.1f})")
            elif projected_edge >= 4:
                score_value += 8
                reasons.append(f"Projeksiyon UST yonunde ({projected_total:.1f})")
            elif projected_edge >= 1:
                score_value += 4
            else:
                penalty = -4 if period == 1 else (-7 if period == 2 else -10)
                score_value += penalty
                risks.append(f"Projeksiyon UST ile ayni hizada degil ({projected_total:.1f})")
    else:
        risks.append("Tempo projeksiyonu hesaplanamadi")

    if score_gap is not None:
        if direction == "ALT" and score_gap >= 15:
            score_value += 6
            reasons.append(f"Skor farki {score_gap} puan, tempo dusme riski var")
            tags.add("blowout")
        if direction == "ÜST" and period == 4 and score_gap <= 8:
            score_value += 7
            reasons.append("Mac yakin, son bolum faul oyunu USTu destekleyebilir")
            tags.add("clutch_over")
        if direction == "ÜST" and score_gap >= 18:
            score_value -= 7
            risks.append(f"Skor farki {score_gap} puan, blowout UST icin negatif")
        if direction == "ALT" and period == 4 and score_gap <= 6:
            score_value -= 6
            risks.append("Mac cok yakin, gec oyun ALT icin riskli")

    home_last5 = (h2h_metrics.get("home_last5") or {}).get("over_pct")
    away_last5 = (h2h_metrics.get("away_last5") or {}).get("over_pct")
    expected_total = h2h_metrics.get("expected_total")
    h2h_over_pct = h2h_metrics.get("h2h_over_pct")

    avg_over = None
    if home_last5 is not None and away_last5 is not None:
        source_count += 1
        avg_over = (home_last5 + away_last5) / 2
        if direction == "ÜST":
            if avg_over >= 60:
                score_value += 6
                reasons.append(f"Son 5 total profili UST lehine ({avg_over:.0f}% over)")
                tags.add("history_over")
            elif avg_over <= 40:
                score_value -= 5
                risks.append(f"Son 5 total profili USTu desteklemiyor ({avg_over:.0f}% over)")
        else:
            if avg_over <= 40:
                score_value += 6
                reasons.append(f"Son 5 total profili ALT lehine ({avg_over:.0f}% over)")
            elif avg_over >= 60:
                score_value -= 5
                risks.append(f"Son 5 total profili ALTi desteklemiyor ({avg_over:.0f}% over)")

    if expected_total is not None:
        if direction == "ÜST" and expected_total >= live + 4:
            score_value += 5
            reasons.append(f"Tarihsel total profili yukarida ({expected_total:.1f})")
            tags.add("history_over")
        elif direction == "ALT" and expected_total <= live - 4:
            score_value += 5
            reasons.append(f"Tarihsel total profili asagida ({expected_total:.1f})")
        elif direction == "ÜST" and expected_total < live - 6:
            score_value -= 4
            risks.append(f"Tarihsel total profili UST ile celisiyor ({expected_total:.1f})")
        elif direction == "ALT" and expected_total > live + 6:
            score_value -= 4
            risks.append(f"Tarihsel total profili ALT ile celisiyor ({expected_total:.1f})")

    if h2h_over_pct is not None:
        if direction == "ÜST" and h2h_over_pct >= 60:
            score_value += 2
        elif direction == "ALT" and h2h_over_pct <= 40:
            score_value += 2

    lineup_page = context.get("lineups") or {}
    standings_page = context.get("standings") or {}
    if lineup_page.get("available"):
        source_count += 0.5
    if standings_page.get("available"):
        source_count += 0.5

    if source_count >= 3:
        score_value += 4
        reasons.append("Veri kapsamı guclu, birden fazla kaynak teyidi var")
    elif source_count <= 1.5:
        score_value -= 6
        risks.append("Kalite puani sinirli veriyle hesaplandi")

    score_value = max(30.0, min(99.0, score_value))

    if score_value >= 88:
        grade = "A++"
    elif score_value >= 80:
        grade = "A+"
    elif score_value >= 72:
        grade = "A"
    elif score_value >= 62:
        grade = "B"
    else:
        grade = "C"

    counter_signal = _build_counter_signal(
        direction=direction,
        live=live,
        projected_total=projected_total,
        period=period,
        remaining_min=remaining_min,
        score_gap=score_gap,
        avg_over=avg_over,
        expected_total=expected_total,
        h2h_over_pct=h2h_over_pct,
    )

    setup = _derive_setup_name(direction, tags)

    reasons = reasons[:4]
    risks = risks[:3]

    summary_parts = []
    if reasons:
        summary_parts.append(reasons[0])
    if risks:
        summary_parts.append(f"Risk: {risks[0]}")
    summary = " | ".join(summary_parts) if summary_parts else "Kalite puani temel market verisinden hesaplandi"

    lines = []
    for item in reasons:
        lines.append(f"+ {item}")
    for item in risks:
        lines.append(f"- {item}")

    team_context = build_team_context(h2h_metrics, live, direction)

    return {
        "grade": grade,
        "score": round(score_value, 1),
        "setup": setup,
        "summary": summary,
        "reasons_text": "\n".join(lines),
        "projected_total": projected_total,
        "data_sources": source_count,
        "counter_direction": counter_signal["direction"],
        "counter_level": counter_signal["level"],
        "counter_score": counter_signal["score"],
        "counter_note": counter_signal["note"],
        "counter_reasons_text": "\n".join(counter_signal["reasons"]),
        "team_context": team_context,
    }
