import json
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


def _fold(value) -> str:
    return (
        str(value or "").strip().lower()
        .replace("ı", "i").replace("ş", "s")
        .replace("ğ", "g").replace("ü", "u")
        .replace("ö", "o").replace("ç", "c")
    )


def _normalize_direction(value) -> str:
    text = str(value or "").strip().upper().replace("UST", "ÜST")
    return "ÜST" if text == "ÜST" else "ALT"


def _opposite_direction(direction: str) -> str:
    return "ALT" if _normalize_direction(direction) == "ÜST" else "ÜST"


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


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
        per_game = r"(?:per\s*game|/\s*game|pts?/game|points?/game)"
        patterns = [
            rf"(?:Last\s*5|Last\s*Matches|Recent\s*Form).{{0,220}}{escaped}.{{0,220}}"
            rf"(\d{{2,3}}(?:\.\d+)?)\s*points?\s*per\s*match[,\s]*"
            rf"(\d{{2,3}}(?:\.\d+)?)\s*opponent\s*points?\s*per\s*game.{{0,160}}"
            rf"(?:Total\s*points?\s*over%[:\s]*(\d+(?:\.\d+)?)%)?",
            rf"(?:Last\s*5|Last\s*Matches|Recent\s*Form).{{0,260}}{escaped}.{{0,260}}"
            rf"pts\s+(\d{{2,3}}(?:\.\d+)?)\s*{pts_sep}\s*(\d{{2,3}}(?:\.\d+)?)\s+"
            rf"{per_game}",
            # AiScore H2H page format: "TeamName N [chars] Home/Away [chars] pts X – Y per game"
            # \b(?:[1-9]|[12]\d)\b matches game counts (1-29) but NOT basketball scores (50+)
            # This prevents false matches where a match-row score is followed by the next
            # section's Home/Away heading within 80 chars.
            rf"{escaped}.{{0,10}}\b(?:[1-9]|[12]\d)\b.{{0,80}}?(?:Home|Away|All).{{0,250}}?"
            rf"pts\s+(\d{{2,3}}(?:\.\d+)?)\s*{pts_sep}\s*(\d{{2,3}}(?:\.\d+)?)\s+"
            rf"{per_game}",
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


def _script_signal(
    score: str,
    status: str,
    match_name: str,
    tournament: str,
    projected_gap: float | None = None,
) -> dict | None:
    clock = game_clock(status, match_name, tournament)
    period = clock["period"]
    remaining_min = clock["remaining_min"]
    home_score, away_score = parse_score(score)
    if home_score is None or away_score is None or period is None:
        return None

    gap = abs(home_score - away_score)
    if period >= 4 and remaining_min is not None and remaining_min <= 5 and gap >= 16:
        return {
            "direction": "ALT",
            "strength": 16,
            "confidence": 82,
            "reason": f"Q4 son bölüm blowout: fark {gap}; rotasyon ve hücum kalitesi düşebilir.",
            "pace_state": "tempo_drop",
        }
    if period >= 4 and remaining_min is not None and remaining_min <= 6 and gap <= 7:
        return {
            "direction": "ÜST",
            "strength": 14,
            "confidence": 76,
            "reason": f"Q4 yakın maç: fark {gap}; faul oyunu/erken hücumlar toplamı yukarı iter.",
            "pace_state": "tempo_rise",
        }
    if period >= 3 and gap >= 16:
        if projected_gap is not None and projected_gap >= 6 and not (period >= 4 and remaining_min is not None and remaining_min <= 6):
            return {
                "direction": "ÜST",
                "strength": 15,
                "confidence": 74,
                "reason": f"Maç kopuyor ama tempo yukarıda: fark {gap}, projeksiyon canlıdan {projected_gap:.1f} yüksek.",
                "pace_state": "runaway_rise",
            }
        return {
            "direction": "ALT",
            "strength": 11,
            "confidence": 68,
            "reason": f"Kopma riski: fark {gap}; öndeki takım set hücumuna ve süre eritmeye dönebilir.",
            "pace_state": "blowout_risk",
        }
    return None


def _period_key(status: str, alert_moment: str = "") -> str:
    source = alert_moment or status or ""
    s = _fold(source)
    if not s:
        return "period:unknown"
    if "devre arasi" in s or re.search(r"\bht\b", s):
        return "period:q2_ht"
    if "q1" in s or re.search(r"(^|\D)1(?:q|\.|c|st|\s*-)", s):
        return "period:q1"
    if "q2" in s or re.search(r"(^|\D)2(?:q|\.|c|nd|\s*-)", s):
        return "period:q2_ht"
    if "q3" in s or re.search(r"(^|\D)3(?:q|\.|c|rd|\s*-)", s):
        return "period:q3"
    if "q4" in s or re.search(r"(^|\D)4(?:q|\.|c|th|\s*-)", s):
        return "period:q4"
    return "period:unknown"


def _diff_key(diff) -> str:
    value = abs(_safe_float(diff) or 0)
    if value < 12:
        return "diff:10_12"
    if value < 16:
        return "diff:12_16"
    if value < 20:
        return "diff:16_20"
    return "diff:20_plus"


def _edge_key(fair_edge) -> str:
    edge = _safe_float(fair_edge)
    if edge is None:
        return "fair:none"
    if edge <= -6:
        return "fair:alt_strong"
    if edge < -2:
        return "fair:alt_mild"
    if edge < 2:
        return "fair:neutral"
    if edge < 6:
        return "fair:ust_mild"
    return "fair:ust_strong"


def _projected_gap_key(projected_total, live) -> str:
    projected = _safe_float(projected_total)
    live_total = _safe_float(live)
    if projected is None or live_total is None:
        return "projection:none"
    gap = projected - live_total
    if gap <= -8:
        return "projection:alt_strong"
    if gap < -3:
        return "projection:alt_mild"
    if gap < 3:
        return "projection:neutral"
    if gap < 8:
        return "projection:ust_mild"
    return "projection:ust_strong"


def _signal_count_key(value) -> str:
    try:
        count = int(value or 1)
    except (TypeError, ValueError):
        count = 1
    return "signal:first" if count <= 1 else "signal:repeat"


def _tournament_key(value) -> str | None:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip().lower())
    if not cleaned:
        return None
    return f"tournament:{cleaned[:80]}"


def _backtest_keys_from_values(
    *,
    legacy_direction: str,
    diff,
    status: str = "",
    alert_moment: str = "",
    fair_edge=None,
    projected_total=None,
    live=None,
    signal_count=1,
    tournament: str = "",
) -> list[str]:
    keys = [
        "all",
        f"legacy:{_normalize_direction(legacy_direction)}",
        _period_key(status, alert_moment),
        _diff_key(diff),
        _edge_key(fair_edge),
        _projected_gap_key(projected_total, live),
        _signal_count_key(signal_count),
    ]
    t_key = _tournament_key(tournament)
    if t_key:
        keys.append(t_key)
    return keys


def build_backtest_profile(rows: list[dict] | None) -> dict:
    """
    Silinen ve sonucu belli sinyallerden fade edilebilir bir profil çıkarır.
    Her bucket için iki aday yön de skorlanır: aynı yöndeki başarılar + ters yöndeki
    başarısızlıklar, aday yönün kazanımı sayılır.
    """
    buckets: dict[str, dict[str, dict[str, int]]] = {}
    resolved = 0

    def add_bucket(key: str, row_direction: str, success: bool) -> None:
        bucket = buckets.setdefault(
            key,
            {
                "ALT": {"wins": 0, "total": 0},
                "ÜST": {"wins": 0, "total": 0},
            },
        )
        for candidate in ("ALT", "ÜST"):
            bucket[candidate]["total"] += 1
            candidate_won = (
                (row_direction == candidate and success)
                or (row_direction != candidate and not success)
            )
            if candidate_won:
                bucket[candidate]["wins"] += 1

    for row in rows or []:
        result = _fold(row.get("result"))
        if result not in {"basarili", "basarisiz"}:
            continue
        resolved += 1
        direction = _normalize_direction(row.get("direction"))
        success = result == "basarili"
        analysis = row.get("analysis")
        if not isinstance(analysis, dict):
            try:
                analysis = json.loads(row.get("ai_analysis") or "{}")
            except Exception:
                analysis = {}
        fair_edge = analysis.get("fair_edge", row.get("fair_edge"))
        projected = analysis.get("projected_total", row.get("projected"))
        keys = _backtest_keys_from_values(
            legacy_direction=analysis.get("legacy_direction") or direction,
            diff=row.get("diff"),
            status=row.get("status") or "",
            alert_moment=row.get("alert_moment") or "",
            fair_edge=fair_edge,
            projected_total=projected,
            live=row.get("live"),
            signal_count=row.get("signal_count") or 1,
            tournament=row.get("tournament") or "",
        )
        for key in keys:
            add_bucket(key, direction, success)

    return {"sample_size": resolved, "buckets": buckets}


def _bucket_rate(profile: dict | None, key: str, direction: str) -> dict | None:
    if not profile:
        return None
    bucket = (profile.get("buckets") or {}).get(key) or {}
    stats = bucket.get(_normalize_direction(direction))
    if not stats or int(stats.get("total") or 0) <= 0:
        return None
    total = int(stats.get("total") or 0)
    wins = int(stats.get("wins") or 0)
    return {"key": key, "wins": wins, "total": total, "rate": round((wins / total) * 100, 1)}


def _backtest_direction_scores(profile: dict | None, keys: list[str]) -> dict:
    weights = {
        "all": 0.8,
        "legacy": 1.1,
        "period": 0.9,
        "diff": 1.1,
        "fair": 1.35,
        "projection": 1.35,
        "signal": 0.75,
        "tournament": 0.85,
    }
    result = {}
    for direction in ("ALT", "ÜST"):
        weighted_sum = 0.0
        weight_sum = 0.0
        samples = 0
        used = []
        for key in keys:
            stats = _bucket_rate(profile, key, direction)
            if not stats:
                continue
            total = stats["total"]
            if total < 8 and not key.startswith("all"):
                continue
            prefix = key.split(":", 1)[0]
            weight = weights.get(prefix, 0.7) * _clamp(total / 60, 0.35, 1.4)
            weighted_sum += stats["rate"] * weight
            weight_sum += weight
            samples += total
            used.append(stats)
        rate = round(weighted_sum / weight_sum, 1) if weight_sum > 0 else None
        result[direction] = {"rate": rate, "samples": samples, "buckets": used[:7]}
    return result


def _vote(name: str, direction: str, strength: float, confidence: float, reason: str) -> dict:
    direction = _normalize_direction(direction)
    strength = round(_clamp(float(strength), 0, 30), 1)
    confidence = round(_clamp(float(confidence), 0, 100), 1)
    return {
        "name": name,
        "direction": direction,
        "strength": strength,
        "confidence": confidence,
        "score": round(strength * (confidence / 100), 1),
        "reason": reason,
    }


def _decision_from_components(
    *,
    legacy_direction: str,
    live: float,
    opening: float,
    diff: float,
    threshold: float,
    status: str,
    alert_moment: str = "",
    score: str = "",
    match_name: str = "",
    tournament: str = "",
    projected_total: float | None = None,
    fair_edge: float | None = None,
    history_total: float | None = None,
    h2h_over_pct: float | None = None,
    h2h_quality_score: float | None = None,
    pace_anomaly_direction: str | None = None,
    pace_anomaly_pct: float | None = None,
    signal_count: int = 1,
    backtest_profile: dict | None = None,
) -> dict:
    legacy_direction = _normalize_direction(legacy_direction)
    votes: list[dict] = []

    abs_diff = abs(float(diff or 0))
    legacy_strength = _clamp(8 + (abs_diff - float(threshold or 0)) * 0.7, 8, 18)
    votes.append(_vote(
        "Eski barem sinyali",
        legacy_direction,
        legacy_strength,
        66,
        f"Eski mantık korundu: canlı-açılış farkı {diff:+.1f}, eşik {float(threshold or 0):.1f}.",
    ))

    if fair_edge is not None:
        edge = float(fair_edge)
        if edge >= 3:
            votes.append(_vote(
                "Adil barem değeri",
                "ÜST",
                _clamp(8 + abs(edge) * 0.9, 8, 24),
                78 if abs(edge) >= 6 else 64,
                f"Adil barem canlıdan {edge:.1f} yüksek; piyasa aşağıda kalmış.",
            ))
        elif edge <= -3:
            votes.append(_vote(
                "Adil barem değeri",
                "ALT",
                _clamp(8 + abs(edge) * 0.9, 8, 24),
                78 if abs(edge) >= 6 else 64,
                f"Adil barem canlıdan {abs(edge):.1f} düşük; piyasa fazla şişmiş.",
            ))

    projected_gap = None
    if projected_total is not None:
        projected_gap = round(float(projected_total) - float(live), 1)
        if projected_gap >= 4:
            votes.append(_vote(
                "Canlı tempo/projeksiyon",
                "ÜST",
                _clamp(7 + projected_gap * 0.75, 7, 22),
                74 if projected_gap >= 8 else 62,
                f"Regresyonlu projeksiyon canlı baremin {projected_gap:.1f} üstünde.",
            ))
        elif projected_gap <= -4:
            votes.append(_vote(
                "Canlı tempo/projeksiyon",
                "ALT",
                _clamp(7 + abs(projected_gap) * 0.75, 7, 22),
                74 if projected_gap <= -8 else 62,
                f"Regresyonlu projeksiyon canlı baremin {abs(projected_gap):.1f} altında.",
            ))

    if history_total is not None:
        history_gap = round(float(history_total) - float(live), 1)
        quality = _clamp((float(h2h_quality_score or 65) + 35) / 100, 0.45, 1.0)
        if history_gap >= 4:
            votes.append(_vote(
                "Takım/H2H regresyonu",
                "ÜST",
                _clamp((6 + history_gap * 0.55) * quality, 4, 16),
                58 + quality * 20,
                f"Tarihsel/son form toplamı canlıdan {history_gap:.1f} yüksek.",
            ))
        elif history_gap <= -4:
            votes.append(_vote(
                "Takım/H2H regresyonu",
                "ALT",
                _clamp((6 + abs(history_gap) * 0.55) * quality, 4, 16),
                58 + quality * 20,
                f"Tarihsel/son form toplamı canlıdan {abs(history_gap):.1f} düşük.",
            ))

    if h2h_over_pct is not None:
        over = float(h2h_over_pct)
        if over >= 62:
            votes.append(_vote("H2H over eğilimi", "ÜST", _clamp((over - 50) * 0.55, 5, 12), 58, f"H2H over oranı %{over:.0f}."))
        elif over <= 38:
            votes.append(_vote("H2H under eğilimi", "ALT", _clamp((50 - over) * 0.55, 5, 12), 58, f"H2H under tarafı güçlü; over oranı %{over:.0f}."))

    if pace_anomaly_direction:
        pct = abs(float(pace_anomaly_pct or 0))
        votes.append(_vote(
            "Çeyrek hız anomalisi",
            pace_anomaly_direction,
            _clamp(7 + pct * 0.15, 7, 15),
            64,
            f"Çeyrek hızı ortalamadan %{pct:.0f} saptı; regresyon {_normalize_direction(pace_anomaly_direction)} tarafında.",
        ))

    script_vote = _script_signal(score, status, match_name, tournament, projected_gap)
    if script_vote:
        votes.append(_vote(
            "Maç scripti",
            script_vote["direction"],
            script_vote["strength"],
            script_vote["confidence"],
            script_vote["reason"],
        ))

    backtest_keys = _backtest_keys_from_values(
        legacy_direction=legacy_direction,
        diff=diff,
        status=status,
        alert_moment=alert_moment,
        fair_edge=fair_edge,
        projected_total=projected_total,
        live=live,
        signal_count=signal_count,
        tournament=tournament,
    )
    backtest_scores = _backtest_direction_scores(backtest_profile, backtest_keys)
    alt_rate = backtest_scores.get("ALT", {}).get("rate")
    ust_rate = backtest_scores.get("ÜST", {}).get("rate")
    if alt_rate is not None and ust_rate is not None:
        delta = round(abs(alt_rate - ust_rate), 1)
        better = "ALT" if alt_rate > ust_rate else "ÜST"
        better_samples = backtest_scores[better]["samples"]
        if delta >= 3 and better_samples >= 25:
            votes.append(_vote(
                "Backtest uyumu",
                better,
                _clamp(5 + delta * 0.55, 5, 15),
                _clamp(56 + delta + min(better_samples, 250) / 12, 56, 82),
                f"Benzer geçmiş bucket'larda {better} %{max(alt_rate, ust_rate):.1f}, diğer taraf %{min(alt_rate, ust_rate):.1f}.",
            ))

    totals = {"ALT": 0.0, "ÜST": 0.0}
    for item in votes:
        totals[item["direction"]] += float(item["score"])

    final_direction = "ALT" if totals["ALT"] > totals["ÜST"] else "ÜST"
    if abs(totals["ALT"] - totals["ÜST"]) < 2.5:
        final_direction = legacy_direction

    final_score_raw = totals[final_direction]
    opposite_score = totals[_opposite_direction(final_direction)]
    consensus = final_score_raw / max(final_score_raw + opposite_score, 1)
    best_rate = backtest_scores.get(final_direction, {}).get("rate")
    backtest_component = best_rate if best_rate is not None else 50
    ai_score = round(_clamp((consensus * 62) + ((backtest_component - 35) * 0.75), 0, 100), 1)

    if ai_score >= 72:
        tier = "A"
    elif ai_score >= 62:
        tier = "B"
    elif ai_score >= 52:
        tier = "C"
    else:
        tier = "D"

    flip_reason = ""
    if final_direction != legacy_direction:
        flip_reason = (
            f"Eski sinyal {legacy_direction}, AI karar {final_direction}: "
            f"tempo/adil barem/script/backtest toplamı yönü çevirdi."
        )

    return {
        "direction": final_direction,
        "legacy_direction": legacy_direction,
        "signal_scores": {key: round(value, 1) for key, value in totals.items()},
        "signal_votes": sorted(votes, key=lambda item: item["score"], reverse=True),
        "ai_score": ai_score,
        "ai_tier": tier,
        "ai_confidence": round(consensus * 100, 1),
        "backtest": {
            "sample_size": (backtest_profile or {}).get("sample_size", 0),
            "keys": backtest_keys,
            "scores": backtest_scores,
            "chosen_rate": best_rate,
            "chosen_samples": backtest_scores.get(final_direction, {}).get("samples", 0),
        },
        "flip_reason": flip_reason,
    }


def enrich_analysis_with_backtest(
    alert: dict,
    analysis: dict | None,
    backtest_profile: dict | None = None,
    threshold: float = 0,
) -> dict:
    analysis = dict(analysis or {})
    opening = _safe_float(alert.get("opening_total", alert.get("opening")))
    live = _safe_float(alert.get("inplay_total", alert.get("live")))
    if opening is None or live is None:
        return analysis
    legacy_direction = analysis.get("legacy_direction") or alert.get("direction") or ("ALT" if live - opening > 0 else "ÜST")
    diff = _safe_float(alert.get("diff"))
    if diff is None:
        diff = abs(live - opening)
    decision = _decision_from_components(
        legacy_direction=legacy_direction,
        live=live,
        opening=opening,
        diff=diff,
        threshold=threshold,
        status=alert.get("status") or "",
        alert_moment=alert.get("alert_moment") or "",
        score=alert.get("score") or "",
        match_name=alert.get("match_name") or "",
        tournament=alert.get("tournament") or "",
        projected_total=analysis.get("projected_total"),
        fair_edge=analysis.get("fair_edge"),
        history_total=analysis.get("history_total"),
        h2h_over_pct=(analysis.get("team_context") or {}).get("h2h_over_pct") if isinstance(analysis.get("team_context"), dict) else None,
        h2h_quality_score=analysis.get("h2h_quality_score"),
        pace_anomaly_direction=analysis.get("pace_anomaly_direction"),
        pace_anomaly_pct=analysis.get("pace_anomaly_pct"),
        signal_count=alert.get("signal_count") or 1,
        backtest_profile=backtest_profile,
    )
    return {**analysis, **decision}


def build_signal_analysis(
    match: dict,
    context: dict | None = None,
    threshold: float = 0,
    pace_data: dict | None = None,
    backtest_profile: dict | None = None,
) -> dict:
    context = context or {}
    opening = float(match.get("opening_total", match.get("opening")))
    live = float(match.get("inplay_total", match.get("live")))
    match_name = match.get("match_name", "")
    tournament = match.get("tournament", "")
    status = match.get("status", "")
    score = match.get("score", "")

    legacy_direction = _normalize_direction(match.get("direction") or ("ALT" if live - opening > 0 else "ÜST"))
    direction = legacy_direction
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

    decision = _decision_from_components(
        legacy_direction=legacy_direction,
        live=live,
        opening=opening,
        diff=line_delta_open,
        threshold=threshold,
        status=status,
        alert_moment=match.get("alert_moment") or "",
        score=score,
        match_name=match_name,
        tournament=tournament,
        projected_total=projected_total,
        fair_edge=fair_edge,
        history_total=history_total,
        h2h_over_pct=h2h_metrics.get("h2h_over_pct"),
        h2h_quality_score=h2h_quality_score,
        pace_anomaly_direction=pace_anomaly_direction,
        pace_anomaly_pct=pace_anomaly_pct,
        signal_count=match.get("signal_count") or 1,
        backtest_profile=backtest_profile,
    )
    direction = decision["direction"]
    if decision.get("flip_reason"):
        warnings.insert(0, decision["flip_reason"])

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

    result = {
        "direction": direction,
        "legacy_direction": legacy_direction,
        "final_direction": direction,
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
        "signal_scores": decision.get("signal_scores"),
        "signal_votes": decision.get("signal_votes"),
        "ai_score": decision.get("ai_score"),
        "ai_tier": decision.get("ai_tier"),
        "ai_confidence": decision.get("ai_confidence"),
        "backtest": decision.get("backtest"),
        "flip_reason": decision.get("flip_reason"),
    }
    return result
