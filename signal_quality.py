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
            return aiscore_team_metrics(team_name)
        ppg = float(match.group(1))
        oppg = float(match.group(2))
        return {
            "team": team_name,
            "ppg": ppg,
            "oppg": oppg,
            "avg_total": round(ppg + oppg, 1),
            "over_pct": float(match.group(3)),
        }

    def aiscore_team_metrics(team_name: str) -> dict:
        if not team_name:
            return {}
        escaped = re.escape(team_name)
        pattern = (
            rf"{escaped}\s+"
            rf"(?P<games>\d+)\s+"
            rf"(?P<scope>Home|Away|All)\s+"
            rf"This league\s+W\s*X(?P<wins>\d+)\s+L\s*X(?P<losses>\d+)\s+"
            rf"pts\s+(?P<ppg>\d+(?:\.\d+)?)\s*[-–]\s*(?P<oppg>\d+(?:\.\d+)?)\s+per game"
        )
        matches = list(re.finditer(pattern, text, re.IGNORECASE))
        if not matches:
            return {}

        preferred_scope = "home" if team_name == home_team else "away"

        def rank(item: re.Match) -> tuple[int, int]:
            scope = (item.group("scope") or "").lower()
            games = int(item.group("games") or 0)
            return (1 if scope == preferred_scope else 0, games)

        match = max(matches, key=rank)
        ppg = float(match.group("ppg"))
        oppg = float(match.group("oppg"))
        return {
            "team": team_name,
            "ppg": ppg,
            "oppg": oppg,
            "avg_total": round(ppg + oppg, 1),
            "over_pct": None,
            "games": int(match.group("games")),
            "scope": match.group("scope"),
            "wins": int(match.group("wins")),
            "losses": int(match.group("losses")),
        }

    h2h_over = None
    h2h_games = None
    h2h_avg_total = None
    h2h_ppg = None
    h2h_oppg = None
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
    else:
        h2h_games_match = re.search(r"\bH2H\s+(\d+)\b", text, re.IGNORECASE)
        if h2h_games_match:
            h2h_games = int(h2h_games_match.group(1))

    if home_team:
        escaped_home = re.escape(home_team)
        h2h_pts_match = re.search(
            rf"\bH2H\b.*?Home\s*-\s*{escaped_home}\s+This league\s+W\s*X\d+\s+L\s*X\d+\s+"
            rf"pts\s+(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)\s+per game",
            text,
            re.IGNORECASE,
        )
        if h2h_pts_match:
            h2h_ppg = float(h2h_pts_match.group(1))
            h2h_oppg = float(h2h_pts_match.group(2))
            h2h_avg_total = round(h2h_ppg + h2h_oppg, 1)

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
        "h2h_avg_total": h2h_avg_total,
        "h2h_ppg": h2h_ppg,
        "h2h_oppg": h2h_oppg,
        "expected_total": expected_total,
    }


def _team_profile_label(avg_total: float | None, over_pct: float | None) -> str:
    """Takımın son 5 maç over eğilimini ligden bağımsız tek etikete çevir."""
    if avg_total is None:
        return "veri yok"
    scoring = f"son 5 toplam ort {avg_total:.1f}"
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
    h2h_avg_total = h2h_metrics.get("h2h_avg_total")
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
    elif h2h_avg_total is not None:
        game_part = f"{int(h2h_games)} maç" if h2h_games else "geçmiş maçlar"
        h2h_note = f"Karşılaşma geçmişi ({game_part}): ortalama toplam {h2h_avg_total:.1f}."

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
        "h2h_avg_total": h2h_avg_total,
        "h2h_note": h2h_note,
        "avg_last5_over_pct": round(avg_over, 1) if avg_over is not None else None,
        "alignment": alignment,
        "alignment_code": alignment_code,
        "signal_direction": direction,
    }


def _safe_round(value, digits: int = 1):
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _history_total_from_metrics(h2h_metrics: dict) -> float | None:
    values = [
        h2h_metrics.get("expected_total"),
        h2h_metrics.get("h2h_avg_total"),
    ]
    values = [float(value) for value in values if value is not None]
    if not values:
        return None
    return round(mean(values), 1)


def _weighted_fair_line(projected_total: float | None, history_total: float | None) -> tuple[float | None, dict]:
    if projected_total is not None and history_total is not None:
        return round((projected_total * 0.60) + (history_total * 0.40), 1), {
            "projection": 60,
            "history": 40,
        }
    if projected_total is not None:
        return round(projected_total, 1), {"projection": 100, "history": 0}
    if history_total is not None:
        return round(history_total, 1), {"projection": 0, "history": 100}
    return None, {"projection": 0, "history": 0}


def _pace_note(projected_total: float | None, live: float, fair_line: float | None) -> str:
    if projected_total is None:
        return "Maç içi projeksiyon hesaplanamadı; pace yorumu sınırlı."
    projection_gap = round(projected_total - live, 1)
    if projection_gap >= 6:
        return f"Maç hızlı gidiyor: mevcut tempo {projected_total:.1f} final gösteriyor, canlı baremin {projection_gap:.1f} üstü."
    if projection_gap <= -6:
        return f"Maç yavaş gidiyor: mevcut tempo {projected_total:.1f} final gösteriyor, canlı baremin {abs(projection_gap):.1f} altı."
    if fair_line is not None and abs(fair_line - live) <= 3:
        return "Pace canlı bareme yakın; belirgin değer alanı yok."
    return f"Pace nötr: mevcut tempo {projected_total:.1f}, canlı bareme yakın."


def _script_warning(score: str, status: str, match_name: str, tournament: str) -> str:
    clock = game_clock(status, match_name, tournament)
    period = clock["period"]
    remaining_min = clock["remaining_min"]
    home_score, away_score = parse_score(score)
    if home_score is None or away_score is None:
        return "Skor okunamadı; maç script uyarısı sınırlı."
    score_gap = abs(home_score - away_score)
    if score_gap >= 18 and period == 4 and remaining_min is not None and remaining_min <= 5:
        return f"Blowout/garbage time riski: fark {score_gap}, son bölümde tempo düşebilir."
    if score_gap >= 15:
        return f"Blowout riski: fark {score_gap}; önde olan takım tempoyu düşürebilir."
    if period == 4 and remaining_min is not None and remaining_min <= 6 and score_gap <= 8:
        return f"Yakın maç riski: fark {score_gap}; faul oyunu ve ekstra pozisyonlar maçı hızlandırabilir."
    return "Maç scriptinde belirgin ekstra risk yok."


def build_signal_analysis(match: dict, context: dict, threshold: float) -> dict:
    opening = float(match["opening_total"])
    live = float(match["inplay_total"])
    baseline = float(match.get("baseline") or opening)
    baseline_label = str(match.get("baseline_label") or "Açılış")
    match_name = match.get("match_name", "")
    tournament = match.get("tournament", "")
    status = match.get("status", "")
    score = match.get("score", "")

    h2h_metrics = _extract_h2h_metrics((context.get("h2h") or {}).get("body_text", ""), match_name)
    projected_total = calculate_projected_total(score, status, match_name, tournament)
    history_total = _history_total_from_metrics(h2h_metrics)
    fair_line, weights = _weighted_fair_line(projected_total, history_total)
    line_delta_open = round(live - opening, 1)
    line_delta_baseline = round(live - baseline, 1)

    direction = "ALT" if line_delta_open > 0 else "ÜST"
    fair_edge = round(fair_line - live, 1) if fair_line is not None else None

    team_context = build_team_context(h2h_metrics, live, direction)
    h2h_note = (team_context or {}).get("h2h_note") or "H2H verisi yok veya okunamadı."
    history_note = (
        f"H2H/son maç adil toplamı {history_total:.1f}."
        if history_total is not None
        else "H2H ve son maçlardan adil toplam çıkarılamadı."
    )
    pace = _pace_note(projected_total, live, fair_line)
    script = _script_warning(score, status, match_name, tournament)

    warning_lines = [
        h2h_note,
        history_note,
        pace,
        script,
    ]
    if abs(line_delta_open) >= 20:
        warning_lines.append(
            f"Açılış-canlı farkı çok yüksek ({line_delta_open:+.1f}); bu bölge ekstra riskli."
        )
    if fair_edge is not None and abs(fair_edge) <= 3:
        warning_lines.append("Adil barem canlıya çok yakın; net değer alanı zayıf.")

    if fair_line is None:
        recommendation = "Adil barem hesaplanamadı; sadece eşik uyarısı olarak izle."
    elif fair_edge >= 6:
        recommendation = "Adil barem canlıdan yüksek: değer ÜST tarafında; maç hızlanabilir."
    elif fair_edge <= -6:
        recommendation = "Adil barem canlıdan düşük: değer ALT tarafında; maç yavaşlayabilir."
    elif fair_edge > 0:
        recommendation = "Adil barem canlıdan az yüksek: ÜST tarafı hafif değerli, temkinli izle."
    elif fair_edge < 0:
        recommendation = "Adil barem canlıdan az düşük: ALT tarafı hafif değerli, temkinli izle."
    else:
        recommendation = "Adil barem canlıyla aynı; bahis için net avantaj yok."

    summary = (
        f"Adil Barem: {fair_line:.1f}" if fair_line is not None else "Adil Barem: hesaplanamadı"
    )
    if fair_edge is not None:
        summary += f" | Canlıya göre {fair_edge:+.1f}"
    summary += f" | Projeksiyon ağırlığı %{weights['projection']}, geçmiş ağırlığı %{weights['history']}"

    return {
        "direction": direction,
        "fair_line": fair_line,
        "fair_edge": fair_edge,
        "projected_total": _safe_round(projected_total),
        "history_total": _safe_round(history_total),
        "weights": weights,
        "opening_delta": line_delta_open,
        "baseline_delta": line_delta_baseline,
        "baseline_label": baseline_label,
        "recommendation": recommendation,
        "pace_note": pace,
        "h2h_note": h2h_note,
        "history_note": history_note,
        "script_note": script,
        "warnings": warning_lines,
        "summary": summary,
        "reasons_text": "\n".join(f"? {line}" for line in warning_lines),
        "team_context": team_context,
    }


def _opposite(direction: str) -> str:
    return "ÜST" if direction == "ALT" else "ALT"


def _source(name: str, vote: str, detail: str, tag: str | None = None) -> dict:
    return {
        "name": name,
        "vote": vote if vote in {"ALT", "ÜST"} else "NÖTR",
        "detail": detail,
        "tag": tag or "",
    }


def _derive_setup_name(direction: str, tags: set[str]) -> str:
    if "blowout" in tags:
        return f"Blowout Senaryosu {direction}"
    if "clutch_game" in tags:
        return f"Clutch Senaryosu {direction}"
    if "garbage_time" in tags:
        return f"Garbage Time {direction}"
    if "hot_pace" in tags:
        return f"Yüksek Tempo {direction}"
    if "cold_pace" in tags:
        return f"Düşük Tempo {direction}"
    return f"Market Sapması {direction}"


def _grade_from_votes(support_count: int, against_count: int) -> str:
    if against_count > support_count:
        return "D"
    if support_count >= 3 and against_count <= 1:
        return "A"
    if support_count == 2 and against_count <= 1:
        return "B"
    return "C"


def _score_from_votes(support_count: int, against_count: int, neutral_count: int) -> float:
    score = 50 + ((support_count - against_count) * 15) + (support_count * 2) - (neutral_count * 2)
    return round(max(5.0, min(95.0, score)), 1)


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
    clock = game_clock(status, match_name, tournament)
    period = clock["period"]
    remaining_min = clock["remaining_min"]
    home_score, away_score = parse_score(score)
    score_gap = abs(home_score - away_score) if home_score is not None and away_score is not None else None

    h2h_metrics = _extract_h2h_metrics((context.get("h2h") or {}).get("body_text", ""), match_name)
    projected_total = calculate_projected_total(score, status, match_name, tournament)

    team_context = build_team_context(h2h_metrics, live, direction)
    tags: set[str] = set()
    sources: list[dict] = []

    diff_ratio = diff / max(float(threshold or 1), 1.0)
    if diff >= float(threshold or 0):
        if diff_ratio >= 1.7:
            strength = "çok güçlü"
        elif diff_ratio >= 1.4:
            strength = "güçlü"
        elif diff_ratio >= 1.15:
            strength = "eşik üstü"
        else:
            strength = "eşikte"
        sources.append(_source(
            "market sapması",
            "NÖTR",
            f"{baseline_label}-canlı farkı {diff:.1f} puan, barem hareketi {strength}; yön için bağımsız kanıt sayılmaz",
        ))
    else:
        sources.append(_source(
            "market sapması",
            "NÖTR",
            f"{baseline_label}-canlı farkı {diff:.1f}, eşik {float(threshold or 0):.1f}",
        ))

    if projected_total is None:
        sources.append(_source("projeksiyon", "NÖTR", "tempo projeksiyonu hesaplanamadı"))
    else:
        projection_edge = round(projected_total - live, 1)
        if projection_edge >= 4:
            tags.add("hot_pace")
            sources.append(_source(
                "projeksiyon",
                "ÜST",
                f"mevcut hız finali {projected_total:.1f} gösteriyor, canlı baremin {projection_edge:.1f} üstü",
                "hot_pace",
            ))
        elif projection_edge <= -4:
            tags.add("cold_pace")
            sources.append(_source(
                "projeksiyon",
                "ALT",
                f"mevcut hız finali {projected_total:.1f} gösteriyor, canlı baremin {abs(projection_edge):.1f} altı",
                "cold_pace",
            ))
        else:
            sources.append(_source(
                "projeksiyon",
                "NÖTR",
                f"mevcut hız {projected_total:.1f}, canlı bareme yakın",
            ))

    home_last5 = (h2h_metrics.get("home_last5") or {}).get("over_pct")
    away_last5 = (h2h_metrics.get("away_last5") or {}).get("over_pct")
    home_avg_total = (h2h_metrics.get("home_last5") or {}).get("avg_total")
    away_avg_total = (h2h_metrics.get("away_last5") or {}).get("avg_total")
    expected_total = h2h_metrics.get("expected_total")
    h2h_over_pct = h2h_metrics.get("h2h_over_pct")
    h2h_avg_total = h2h_metrics.get("h2h_avg_total")
    history_average_note = ""
    if expected_total is not None:
        history_average_note = f"Geçmiş maç ortalaması {expected_total:.1f}"
        side_parts = []
        if home_avg_total is not None:
            side_parts.append(f"ev {home_avg_total:.1f}")
        if away_avg_total is not None:
            side_parts.append(f"dep {away_avg_total:.1f}")
        if side_parts:
            history_average_note += f" ({', '.join(side_parts)})"
    avg_over = None
    if home_last5 is not None and away_last5 is not None:
        avg_over = round((home_last5 + away_last5) / 2, 1)
        expected_part = f", son 5 ortalama toplam {expected_total:.1f}" if expected_total is not None else ""
        if avg_over >= 60:
            sources.append(_source(
                "takım profili",
                "ÜST",
                f"iki takım son 5 over ortalaması %{avg_over:.0f}{expected_part}",
            ))
        elif avg_over <= 40:
            sources.append(_source(
                "takım profili",
                "ALT",
                f"iki takım son 5 over ortalaması %{avg_over:.0f}{expected_part}",
            ))
        else:
            sources.append(_source(
                "takım profili",
                "NÖTR",
                f"iki takım son 5 over ortalaması %{avg_over:.0f}{expected_part}",
            ))
    elif expected_total is not None:
        if expected_total >= live + 4:
            sources.append(_source(
                "takım profili",
                "ÜST",
                f"son maç profili {expected_total:.1f}, canlı baremin {expected_total - live:.1f} üstü",
            ))
        elif expected_total <= live - 4:
            sources.append(_source(
                "takım profili",
                "ALT",
                f"son maç profili {expected_total:.1f}, canlı baremin {live - expected_total:.1f} altı",
            ))
        else:
            sources.append(_source(
                "takım profili",
                "NÖTR",
                f"son maç profili {expected_total:.1f}, canlı bareme yakın",
            ))
    else:
        sources.append(_source("takım profili", "NÖTR", "son 5 takım profili eksik"))

    if h2h_over_pct is not None:
        h2h_games = h2h_metrics.get("h2h_games")
        game_part = f"{int(h2h_games)} maçta " if h2h_games else ""
        if h2h_over_pct >= 60:
            sources.append(_source("H2H geçmiş", "ÜST", f"{game_part}over %{h2h_over_pct:.0f}"))
        elif h2h_over_pct <= 40:
            sources.append(_source("H2H geçmiş", "ALT", f"{game_part}over %{h2h_over_pct:.0f}"))
        else:
            sources.append(_source("H2H geçmiş", "NÖTR", f"{game_part}over %{h2h_over_pct:.0f}, dengeli"))
    elif h2h_avg_total is not None:
        h2h_games = h2h_metrics.get("h2h_games")
        game_part = f"{int(h2h_games)} maçta " if h2h_games else ""
        if h2h_avg_total >= live + 4:
            sources.append(_source(
                "H2H geçmiş",
                "ÜST",
                f"{game_part}ortalama toplam {h2h_avg_total:.1f}, canlı baremin {h2h_avg_total - live:.1f} üstü",
            ))
        elif h2h_avg_total <= live - 4:
            sources.append(_source(
                "H2H geçmiş",
                "ALT",
                f"{game_part}ortalama toplam {h2h_avg_total:.1f}, canlı baremin {live - h2h_avg_total:.1f} altı",
            ))
        else:
            sources.append(_source(
                "H2H geçmiş",
                "NÖTR",
                f"{game_part}ortalama toplam {h2h_avg_total:.1f}, canlı bareme yakın",
            ))
    else:
        sources.append(_source("H2H geçmiş", "NÖTR", "karşılıklı geçmiş verisi yok"))

    script_vote = "NÖTR"
    script_detail = "maç scripti belirgin değil"
    if score_gap is not None and score_gap >= 18 and period == 4 and remaining_min is not None and remaining_min <= 5:
        script_vote = "ALT"
        script_detail = f"garbage time — skor farkı {score_gap}, son bölümde tempo düşebilir"
        tags.add("garbage_time")
    elif score_gap is not None and score_gap >= 15:
        script_vote = "ALT"
        script_detail = f"blowout senaryosu — skor farkı {score_gap}, tempo düşebilir"
        tags.add("blowout")
    elif score_gap is not None and period == 4 and remaining_min is not None and remaining_min <= 6 and score_gap <= 8:
        script_vote = "ÜST"
        script_detail = f"clutch game — skor farkı {score_gap}, faul oyunu ve ekstra pozisyon riski"
        tags.add("clutch_game")
    sources.append(_source("maç script", script_vote, script_detail, next(iter(tags), None) if script_vote != "NÖTR" else None))

    supporting = [item for item in sources if item["vote"] == direction]
    opposing = [item for item in sources if item["vote"] == _opposite(direction)]
    neutral = [item for item in sources if item["vote"] == "NÖTR"]
    support_count = len(supporting)
    against_count = len(opposing)
    neutral_count = len(neutral)

    grade = _grade_from_votes(support_count, against_count)
    score_value = _score_from_votes(support_count, against_count, neutral_count)
    setup = _derive_setup_name(direction, tags)
    reverse_risk = grade == "D"

    summary = (
        f"{support_count} kaynak destekliyor, {against_count} kaynak karşı"
        + (f", {neutral_count} nötr" if neutral_count else "")
    )
    if history_average_note:
        summary += f" | {history_average_note}"
    if reverse_risk:
        summary += " — TERS RİSKİ VAR"

    lines = []
    for item in supporting:
        lines.append(f"+ {item['name']}: {item['detail']}")
    for item in opposing:
        lines.append(f"- {item['name']}: {item['detail']}")
    for item in neutral:
        lines.append(f"= {item['name']}: {item['detail']}")

    return {
        "grade": grade,
        "score": round(score_value, 1),
        "setup": setup,
        "summary": summary,
        "reasons_text": "\n".join(lines),
        "projected_total": projected_total,
        "data_sources": len(sources),
        "sources": sources,
        "supporting_signals": supporting,
        "opposing_signals": opposing,
        "neutral_signals": neutral,
        "support_count": support_count,
        "against_count": against_count,
        "neutral_count": neutral_count,
        "reverse_risk": reverse_risk,
        "script_note": script_detail if script_vote != "NÖTR" else "",
        "history_average_note": history_average_note,
        "team_context": team_context,
    }
