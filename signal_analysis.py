import re
from statistics import mean

from projection import calculate_projected_total, game_clock, parse_score


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


def _safe_round(value, digits: int = 1):
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _split_match_name(match_name: str) -> tuple[str, str]:
    parts = [part.strip() for part in (match_name or "").split(" - ", 1)]
    if len(parts) == 2:
        return parts[0], parts[1]
    return match_name or "Home", "Away"


def _extract_h2h_metrics(body_text: str, match_name: str) -> dict:
    text = re.sub(r"\s+", " ", body_text or "").strip()
    home_team, away_team = _split_match_name(match_name)
    text_lower = text.lower()
    h2h_source = ""
    h2h_quality_notes: list[str] = []

    # Matches any unicode dash/separator between words
    _DASH = r"[-–—\u2012\u2015]"

    if not text:
        h2h_quality_notes.append("H2H sayfa metni boş.")
    if "no data" in text_lower:
        h2h_quality_notes.append("AiScore H2H sayfası No data döndürdü.")
    if "invalid date" in text_lower:
        h2h_quality_notes.append("AiScore H2H sayfası geçersiz/eksik maç sayfası döndürdü.")
    if re.search(r"\bpts\s+0(?:\.0)?\s*[-–—]\s*0(?:\.0)?\s+per\s+game\b", text, re.IGNORECASE):
        h2h_quality_notes.append("H2H istatistik satırı 0-0 döndü; yok sayıldı.")

    def team_metrics(team_name: str) -> dict:
        if not team_name:
            return {}
        escaped = re.escape(team_name)
        # Unicode dash + hyphen variants
        pts_sep = r"[-–—\u2012\u2015]"
        # Suffix tolerates "per game", "/game", "pts/game", "points/game"
        per_game = r"(?:per\s*game|/\s*game|pts?/game|points?/game)"
        patterns = [
            # AiScore: "Last 5, Team 90.5 points per match, 88.2 opponent points per game"
            (
                rf"Last\s*5[,\s]+{escaped}.{{0,200}}"
                rf"(\d+(?:\.\d+)?)\s*points?\s*per\s*match[,\s]*"
                rf"(\d+(?:\.\d+)?)\s*opponent\s*points?\s*per\s*game.{{0,200}}"
                rf"Total\s*points?\s*over%[:\s]*(\d+(?:\.\d+)?)%",
                3,
            ),
            # AiScore format 1: "Team 6 Away This league ... pts 79.4 – 94.4 per game"
            (
                rf"{escaped}\s+\d+\s+(?:Home|Away|All).{{0,150}}"
                rf"pts\s+(\d+(?:\.\d+)?)\s*{pts_sep}\s*(\d+(?:\.\d+)?)\s+{per_game}",
                2,
            ),
            # AiScore format 2: "H2H 6 Home — Team This league ... pts 96.2 – 88.0 per game"
            (
                rf"H2H\s+\d+\s+(?:Home|Away|All).{{0,12}}{escaped}.{{0,200}}"
                rf"pts\s+(\d+(?:\.\d+)?)\s*{pts_sep}\s*(\d+(?:\.\d+)?)\s+{per_game}",
                2,
            ),
            # AiScore format 3: "Team ... pts 96.2 – 88.0 per game" (any proximity)
            (
                rf"{escaped}.{{0,250}}"
                rf"pts\s+(\d+(?:\.\d+)?)\s*{pts_sep}\s*(\d+(?:\.\d+)?)\s+{per_game}",
                2,
            ),
            # Loose fallback: "TeamName ... 90.5 - 88.2 per game" (no 'pts' marker)
            (
                rf"{escaped}.{{0,180}}"
                rf"(\d{{2,3}}(?:\.\d+)?)\s*{pts_sep}\s*(\d{{2,3}}(?:\.\d+)?)\s+{per_game}",
                2,
            ),
        ]
        for pattern, groups in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if not m:
                continue
            if groups == 3:
                ppg, oppg, over_pct = float(m.group(1)), float(m.group(2)), float(m.group(3))
                return {"team": team_name, "ppg": ppg, "oppg": oppg,
                        "avg_total": round(ppg + oppg, 1), "over_pct": over_pct}
            ppg, oppg = float(m.group(1)), float(m.group(2))
            return {"team": team_name, "ppg": ppg, "oppg": oppg,
                    "avg_total": round(ppg + oppg, 1), "over_pct": None}
        return {}

    def recent_team_metrics(team_name: str) -> dict:
        """Strictly parse team recent/last-5 blocks, not H2H aggregate rows."""
        if not team_name:
            return {}
        escaped = re.escape(team_name)
        pts_sep = r"[-–—\u2012\u2015]"
        patterns = [
            rf"(?:Last\s*5|Last\s*Matches|Recent\s*Form).{{0,220}}{escaped}.{{0,220}}"
            rf"(\d{{2,3}}(?:\.\d+)?)\s*points?\s*per\s*match[,\s]*"
            rf"(\d{{2,3}}(?:\.\d+)?)\s*opponent\s*points?\s*per\s*game.{{0,160}}"
            rf"(?:Total\s*points?\s*over%[:\s]*(\d+(?:\.\d+)?)%)?",
            rf"(?:Last\s*5|Last\s*Matches|Recent\s*Form).{{0,260}}{escaped}.{{0,260}}"
            rf"pts\s+(\d{{2,3}}(?:\.\d+)?)\s*{pts_sep}\s*(\d{{2,3}}(?:\.\d+)?)\s+"
            rf"(?:per\s*game|/\s*game|pts?/game|points?/game)",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if not m:
                continue
            ppg, oppg = float(m.group(1)), float(m.group(2))
            if ppg <= 0 or oppg <= 0:
                continue
            over_pct = float(m.group(3)) if m.lastindex and m.lastindex >= 3 and m.group(3) else None
            return {
                "team": team_name,
                "ppg": ppg,
                "oppg": oppg,
                "avg_total": round(ppg + oppg, 1),
                "over_pct": over_pct,
            }
        return {}

    def h2h_totals_from_match_rows() -> list[int]:
        if not text:
            return []
        date_re = (
            r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
            r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+"
            r"\d{1,2},\s+\d{4}"
        )
        totals: list[int] = []
        for match in re.finditer(date_re, text, re.IGNORECASE):
            chunk = text[match.end():match.end() + 220]
            if home_team and away_team and (home_team not in chunk or away_team not in chunk):
                continue
            nums = [int(n) for n in re.findall(r"\b\d{2,3}\b", chunk)]
            score_nums = [n for n in nums if 40 <= n <= 180]
            if len(score_nums) >= 2:
                total = score_nums[0] + score_nums[1]
                if 100 <= total <= 320:
                    totals.append(total)
        return totals[:12]

    h2h_games = None
    h2h_avg_total = None
    h2h_over_pct = None

    # H2H over % — multiple phrasings
    over_match = (
        re.search(r"(?:Past\s+)?H2H\s+Results.{0,300}Total\s+Points?\s+Over%[:\s]*(\d+(?:\.\d+)?)%", text, re.IGNORECASE)
        or re.search(r"Total\s+Points?\s+Over%[:\s]*(\d+(?:\.\d+)?)%", text, re.IGNORECASE)
        or re.search(r"Over\s*(?:Under\s*)?%[:\s]*(\d+(?:\.\d+)?)%", text, re.IGNORECASE)
    )
    if over_match:
        h2h_over_pct = float(over_match.group(1))

    # H2H game count
    games_match = (
        re.search(r"Total\s+Matches[:\s]+(\d+)", text, re.IGNORECASE)
        or re.search(r"\bH2H\b[:\s]+(\d+)\b", text, re.IGNORECASE)
        or re.search(r"(?:head.to.head|h2h)\D{0,20}(\d{1,3})\s*(?:games?|matches?)", text, re.IGNORECASE)
    )
    if games_match:
        h2h_games = int(games_match.group(1))

    # H2H average total from past meetings
    # Handles: "H2H 6 Home — Team This league ... pts 96.2 – 88.0 per game"
    #       or: "H2H ... Home - Team ... pts X – Y per game"
    pts_sep = r"[-–—\u2012\u2015]"
    per_game = r"(?:per\s*game|/\s*game|pts?/game|points?/game)"
    if home_team:
        h2h_home_pattern = (
            rf"\bH2H\b.{{0,250}}Home.{{0,12}}{re.escape(home_team)}.{{0,250}}"
            rf"pts\s+(\d+(?:\.\d+)?)\s*{pts_sep}\s*(\d+(?:\.\d+)?)\s+{per_game}"
        )
        h2h_m = re.search(h2h_home_pattern, text, re.IGNORECASE)
        if h2h_m:
            left, right = float(h2h_m.group(1)), float(h2h_m.group(2))
            if left > 0 and right > 0:
                h2h_avg_total = round(left + right, 1)
                h2h_source = "stats_row"

    # Generic H2H average total fallback
    if h2h_avg_total is None:
        generic = (
            re.search(r"\bH2H\b.{0,60}avg(?:erage)?\D{0,10}(\d{3}(?:\.\d+)?)", text, re.IGNORECASE)
            or re.search(r"average\s+total[:\s]*(\d{3}(?:\.\d+)?)", text, re.IGNORECASE)
        )
        if generic:
            h2h_avg_total = float(generic.group(1))
            h2h_source = "average_text"

    h2h_row_totals = h2h_totals_from_match_rows()
    if h2h_avg_total is None and h2h_row_totals:
        h2h_avg_total = round(mean(h2h_row_totals), 1)
        h2h_source = "score_rows"
        if h2h_games is None:
            h2h_games = len(h2h_row_totals)
    if h2h_games is None and h2h_row_totals:
        h2h_games = len(h2h_row_totals)

    home = recent_team_metrics(home_team)
    away = recent_team_metrics(away_team)
    expected_total = None
    if home and away:
        # Cross-pair: home'un attığı + away'in yediği ≈ home skoru beklentisi (ve tersi).
        # Ham mean([home_avg, away_avg]) yerine bu karşılıklı eşleştirme daha doğrudur.
        home_expected = (home["ppg"] + away["oppg"]) / 2
        away_expected = (away["ppg"] + home["oppg"]) / 2
        expected_total = round(home_expected + away_expected, 1)

    if not home or not away:
        h2h_quality_notes.append("Takım son-form verisi eksik; team_recent bileşeni kullanılmadı.")
    if h2h_avg_total is None:
        h2h_quality_notes.append("H2H ortalama toplam çıkarılamadı.")

    quality_score = 100
    if not text:
        quality_score -= 60
    if h2h_avg_total is None:
        quality_score -= 35
    if expected_total is None:
        quality_score -= 20
    if h2h_games is not None and h2h_games < 3:
        quality_score -= 20
    if "no data" in text_lower or "invalid date" in text_lower:
        quality_score -= 25
    quality_score = max(0, min(100, quality_score))

    return {
        "home_last5": home,
        "away_last5": away,
        "expected_total": expected_total,
        "h2h_avg_total": h2h_avg_total,
        "h2h_over_pct": h2h_over_pct,
        "h2h_games": h2h_games,
        "h2h_source": h2h_source,
        "h2h_row_totals": h2h_row_totals,
        "h2h_body_chars": len(text),
        "h2h_quality_score": quality_score,
        "h2h_quality_notes": h2h_quality_notes,
    }


def _team_profile_label(avg_total: float | None, over_pct: float | None) -> str:
    if avg_total is None:
        return "veri yok"
    label = f"son 5 toplam ort {avg_total:.1f}"
    if over_pct is None:
        return label
    if over_pct >= 60:
        return f"{label} · over eğilimli"
    if over_pct <= 40:
        return f"{label} · under eğilimli"
    return f"{label} · dengeli"


def build_team_context(h2h_metrics: dict, live: float, direction: str) -> dict | None:
    home = h2h_metrics.get("home_last5") or {}
    away = h2h_metrics.get("away_last5") or {}
    if not home and not away and h2h_metrics.get("h2h_avg_total") is None and h2h_metrics.get("h2h_over_pct") is None:
        return None

    expected = h2h_metrics.get("expected_total")
    regression_direction = None
    regression_note = None
    regression_delta = None
    if expected is not None:
        regression_delta = round(live - expected, 1)
        if regression_delta >= 4:
            regression_direction = "ALT"
            regression_note = f"Son 5 maç ortalaması {expected:.1f}, canlı barem {live:.1f} (+{regression_delta:.1f}). Ortalamaya dönüş ALT tarafını destekler."
        elif regression_delta <= -4:
            regression_direction = "ÜST"
            regression_note = f"Son 5 maç ortalaması {expected:.1f}, canlı barem {live:.1f} ({regression_delta:.1f}). Ortalamaya dönüş ÜST tarafını destekler."
        else:
            regression_note = f"Son 5 maç ortalaması {expected:.1f}, canlı barem {live:.1f} ({regression_delta:+.1f}). Barem tarihsel profile yakın."

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

    h2h_note = None
    h2h_games = h2h_metrics.get("h2h_games")
    h2h_over = h2h_metrics.get("h2h_over_pct")
    h2h_avg = h2h_metrics.get("h2h_avg_total")
    if h2h_over is not None:
        game_part = f"{int(h2h_games)} maç" if h2h_games else "geçmiş maçlar"
        if h2h_over >= 60:
            tilt = "ÜST tarafına eğilimli"
        elif h2h_over <= 40:
            tilt = "ALT tarafına eğilimli"
        else:
            tilt = "dengeli"
        h2h_note = f"Karşılaşma geçmişi ({game_part}): %{h2h_over:.0f} over — {tilt}."
    elif h2h_avg is not None:
        game_part = f"{int(h2h_games)} maç" if h2h_games else "geçmiş maçlar"
        h2h_note = f"Karşılaşma geçmişi ({game_part}): ortalama toplam {h2h_avg:.1f}."

    support_points = 0
    against_points = 0
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
        alignment, alignment_code = "Takım profili nötr", "neutral"
    elif support_points > against_points:
        alignment, alignment_code = f"Takım profili {direction} sinyalini destekliyor", "support"
    elif against_points > support_points:
        alignment, alignment_code = f"Takım profili {direction} sinyaline karşı", "against"
    else:
        alignment, alignment_code = "Takım profili karışık sinyal veriyor", "mixed"

    return {
        "expected_total": expected,
        "regression_delta": regression_delta,
        "regression_direction": regression_direction,
        "regression_note": regression_note,
        "home_profile": profile(home),
        "away_profile": profile(away),
        "h2h_games": h2h_games,
        "h2h_over_pct": h2h_over,
        "h2h_avg_total": h2h_avg,
        "h2h_note": h2h_note,
        "alignment": alignment,
        "alignment_code": alignment_code,
        "signal_direction": direction,
    }


def _market_total(match: dict, opening: float) -> float | None:
    for key in ("prematch_total", "prematch", "baseline", "opening_total", "opening"):
        value = match.get(key)
        parsed = _safe_float(value)
        if parsed is not None:
            return round(parsed, 1)
    return round(opening, 1)


def _fair_weight_profile(status: str, match_name: str, tournament: str) -> dict:
    # Erken periyotta pace numunesi küçük, projeksiyon varyansı çok yüksek.
    # Geçmiş maç profili erken bölümde daha etkilidir; maç ilerledikçe
    # canlı tempo daha fazla ağırlık alır.
    period = game_clock(status, match_name, tournament)["period"]
    is_live_unknown = (period is None) and re.match(
        r"^live\b", (status or "").strip(), re.IGNORECASE
    )
    if is_live_unknown:
        return {"projection": 0, "market": 55, "team_recent": 30, "h2h": 15}
    if period == 1:
        return {"projection": 10, "market": 55, "team_recent": 20, "h2h": 15}
    if period == 2:
        return {"projection": 35, "market": 35, "team_recent": 20, "h2h": 10}
    if period == 3:
        return {"projection": 50, "market": 25, "team_recent": 17, "h2h": 8}
    if period and period >= 4:
        return {"projection": 60, "market": 20, "team_recent": 15, "h2h": 5}
    return {"projection": 0, "market": 55, "team_recent": 30, "h2h": 15}


def _normalize_weights(weights: dict) -> dict:
    total = sum(weights.values())
    if total <= 0:
        return {key: 0 for key in weights}
    exact = {key: (value / total) * 100 for key, value in weights.items()}
    rounded = {key: int(value) for key, value in exact.items()}
    remainder = 100 - sum(rounded.values())
    ranked = sorted(exact, key=lambda key: exact[key] - rounded[key], reverse=True)
    for key in ranked[:remainder]:
        rounded[key] += 1
    return rounded


def _weighted_fair_line(components: dict, base_weights: dict) -> tuple[float | None, dict]:
    if components.get("projection") is None:
        return None, {key: 0 for key in base_weights}

    usable = {
        key: float(value)
        for key, value in components.items()
        if value is not None and key in base_weights and base_weights.get(key, 0) > 0
    }
    if not usable:
        return None, {key: 0 for key in base_weights}
    active_weights = {key: base_weights[key] for key in usable}
    weight_sum = sum(active_weights.values())
    fair_line = sum(usable[key] * active_weights[key] for key in usable) / weight_sum
    return round(fair_line, 1), _normalize_weights(active_weights)


def _pace_note(projected_total: float | None, live: float, pace_data: dict | None = None) -> str:
    """
    Maç hızını yorumlar. Regresyon uygulanmış projeksiyon kullanır:
    Q1/Q2'deki yüksek tempo doğrusal değil, ortalamaya dönüş varsayımıyla hesaplanır.
    pace_data sağlanmışsa çeyrek bazlı anomali notu da eklenir.
    """
    anomaly_note = (pace_data or {}).get("pace_note", "")

    if projected_total is None:
        base = "Maç içi projeksiyon hesaplanamadı (çeyrek bilgisi yok); pace yorumu sınırlı."
        return f"{base} {anomaly_note}".strip() if anomaly_note else base

    gap = round(projected_total - live, 1)
    if gap >= 6:
        base = (
            f"Maç hızlı gidiyor: regresyonlu tempo {projected_total:.1f} final gösteriyor, "
            f"canlı baremin {gap:.1f} üstü → ALT baskısı."
        )
    elif gap <= -6:
        base = (
            f"Maç yavaş gidiyor: regresyonlu tempo {projected_total:.1f} final gösteriyor, "
            f"canlı baremin {abs(gap):.1f} altı → ÜST baskısı."
        )
    else:
        base = f"Pace nötr: regresyonlu tempo {projected_total:.1f}, canlı bareme yakın."

    return f"{base} {anomaly_note}".strip() if anomaly_note else base


def _script_pace_adjustment(score: str, status: str, match_name: str, tournament: str) -> float:
    """Skor farkı ve periyot durumuna göre projeksiyona çarpan döndürür.
    1.0 = değişiklik yok. 0.85 = pace %15 düşer (blowout). 1.10 = pace %10 artar (faul oyunu)."""
    clock = game_clock(status, match_name, tournament)
    period = clock["period"]
    remaining_min = clock["remaining_min"]
    home_score, away_score = parse_score(score)
    if home_score is None or away_score is None or period is None:
        return 1.0
    gap = abs(home_score - away_score)
    if period >= 4 and remaining_min is not None and remaining_min <= 6:
        if gap >= 18:
            return 0.85
        if gap >= 12:
            return 0.92
        if gap <= 6:
            return 1.08
    elif period >= 3 and gap >= 20:
        return 0.93
    return 1.0


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


def build_signal_analysis(
    match: dict,
    context: dict | None = None,
    threshold: float = 0,
    pace_data: dict | None = None,
) -> dict:
    context = context or {}
    opening = float(match.get("opening_total", match.get("opening")))
    live = float(match.get("inplay_total", match.get("live")))
    match_name = match.get("match_name", "")
    tournament = match.get("tournament", "")
    status = match.get("status", "")
    score = match.get("score", "")

    direction = str(match.get("direction") or ("ALT" if live - opening > 0 else "ÜST")).replace("UST", "ÜST")
    line_delta_open = round(live - opening, 1)
    h2h_body = (context.get("h2h") or {}).get("body_text", "") if isinstance(context, dict) else ""
    h2h_metrics = _extract_h2h_metrics(h2h_body, match_name)
    clock = game_clock(status, match_name, tournament)
    has_reliable_clock = clock.get("period") is not None and clock.get("remaining_min") is not None
    raw_projected = (
        calculate_projected_total(score, status, match_name, tournament)
        if has_reliable_clock
        else None
    )
    pace_adj = _script_pace_adjustment(score, status, match_name, tournament)
    projected_total = round(raw_projected * pace_adj, 1) if raw_projected is not None else None
    market_total = _market_total(match, opening)
    team_recent_total = h2h_metrics.get("expected_total")
    h2h_total = h2h_metrics.get("h2h_avg_total")
    h2h_games = h2h_metrics.get("h2h_games")
    h2h_quality_score = h2h_metrics.get("h2h_quality_score")
    h2h_quality_notes = h2h_metrics.get("h2h_quality_notes") or []
    history_values = [value for value in (team_recent_total, h2h_total) if value is not None]
    history_total = round(mean(history_values), 1) if history_values else None

    components = {
        "projection": projected_total,
        "market": market_total,
        "team_recent": team_recent_total,
        "h2h": h2h_total,
    }
    base_weights = _fair_weight_profile(status, match_name, tournament)
    # H2H numunesi 3'ten az ise güvenilmez — ağırlığını sıfırla.
    if h2h_games is not None and h2h_games < 3:
        base_weights = {**base_weights, "h2h": 0}
        h2h_total = None
        components["h2h"] = None
    fair_line, weights = _weighted_fair_line(components, base_weights)
    fair_edge = round(fair_line - live, 1) if fair_line is not None else None

    team_context = build_team_context(h2h_metrics, live, direction)
    h2h_note = (team_context or {}).get("h2h_note") or "H2H verisi yok veya okunamadı."
    if h2h_quality_score is not None and h2h_quality_score < 70:
        h2h_note = f"{h2h_note} Veri kalitesi düşük ({h2h_quality_score}/100)."
    history_note = (
        f"H2H/son maç adil toplamı {history_total:.1f}."
        if history_total is not None
        else "H2H ve son maçlardan adil toplam çıkarılamadı."
    )
    pace = _pace_note(projected_total, live, pace_data)
    script = _script_warning(score, status, match_name, tournament)

    # Çeyrek hız anomalisi uyarısı
    pace_anomaly_direction = (pace_data or {}).get("anomaly_direction")
    pace_anomaly_pct = (pace_data or {}).get("anomaly_pct")
    quarter_paces = (pace_data or {}).get("quarter_paces", {})
    pace_anomaly_note = ""
    if pace_anomaly_direction and pace_anomaly_pct is not None:
        pace_anomaly_note = (
            f"Çeyrek hız anomalisi (%{abs(int(pace_anomaly_pct))} sapma): "
            f"ortalamaya dönüş {pace_anomaly_direction} tarafını destekler."
        )

    warnings = [h2h_note, history_note, pace, script]
    warnings.extend(h2h_quality_notes[:3])
    if pace_anomaly_note:
        warnings.insert(0, pace_anomaly_note)
    if abs(line_delta_open) >= 20:
        warnings.append(f"Açılış-canlı farkı çok yüksek ({line_delta_open:+.1f}); bu bölge ekstra riskli.")
    if projected_total is None:
        warnings.append("Adil barem hesaplanamadı: maç süresi/projeksiyon güvenilir okunamadı.")
    if fair_edge is not None and abs(fair_edge) <= 3:
        warnings.append("Adil barem canlıya çok yakın; net değer alanı zayıf.")

    if fair_line is None:
        if projected_total is None:
            recommendation = "Adil barem hesaplanamadı: maç süresi/projeksiyon güvenilir okunamadı."
        else:
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

    summary = f"Adil Barem: {fair_line:.1f}" if fair_line is not None else "Adil Barem: hesaplanamadı"
    if fair_edge is not None:
        summary += f" | Canlıya göre {fair_edge:+.1f}"

    return {
        "direction": direction,
        "fair_line": fair_line,
        "fair_edge": fair_edge,
        "projected_total": _safe_round(projected_total),
        "market_total": _safe_round(market_total),
        "team_recent_total": _safe_round(team_recent_total),
        "h2h_total": _safe_round(h2h_total),
        "history_total": _safe_round(history_total),
        "h2h_games": h2h_games,
        "h2h_source": h2h_metrics.get("h2h_source") or "",
        "h2h_body_chars": h2h_metrics.get("h2h_body_chars"),
        "h2h_quality_score": h2h_quality_score,
        "h2h_quality_notes": h2h_quality_notes,
        "weights": weights,
        "base_weights": base_weights,
        "fair_components": {key: _safe_round(value) for key, value in components.items()},
        "opening_delta": line_delta_open,
        "recommendation": recommendation,
        "pace_note": pace,
        "h2h_note": h2h_note,
        "history_note": history_note,
        "script_note": script,
        "warnings": warnings,
        "summary": summary,
        "team_context": team_context,
        "threshold": threshold,
        "pace_anomaly_direction": pace_anomaly_direction,
        "pace_anomaly_pct": pace_anomaly_pct,
        "quarter_paces": quarter_paces,
        "pace_anomaly_note": pace_anomaly_note,
    }
