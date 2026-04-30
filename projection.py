import re

# Çeyreğe göre pace regresyon katsayısı.
# Q1 hızı genellikle abartılıdır (takımlar taze, erken yüksek tempo).
# Kalan süreye bu katsayı uygulanır; ilerleyen çeyreklerde giderek gerçeğe yaklaşır.
# Q4'te 1.0 → mevcut hız en güvenilir tahmin kaynağıdır.
_PACE_REGRESSION = {1: 0.88, 2: 0.93, 3: 0.97, 4: 1.00}

# "Live" statüsünde çeyrek tahmini için kaba ortalama puan/çeyrek
# (iki takım toplam, 10 dakikalık birim üzerinden)
_AVG_PTS_PER_QUARTER = {40: 25, 48: 28}


def parse_score(score: str) -> tuple[int | None, int | None]:
    match = re.search(r"(\d+)\s*[-–]\s*(\d+)", score or "")
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def estimate_period_from_score(
    total_pts: int, total_game_min: int, quarter_length: float
) -> tuple[int, float]:
    """
    Çeyrek bilgisi yokken toplam skora göre olası periyot ve kalan süre tahmini.
    Sadece "Live" statüsünde fallback olarak kullanılır; tahmin kaba.
    """
    avg_per_q = _AVG_PTS_PER_QUARTER.get(total_game_min, 25)
    quarters_played = total_pts / avg_per_q
    period = min(4, max(1, int(quarters_played) + 1))
    fractional = max(0.0, min(1.0, quarters_played - (period - 1)))
    remaining = max(1.0, quarter_length * (1.0 - fractional))
    return period, remaining


def game_clock(status: str, match_name: str = "", tournament: str = "") -> dict:
    status_clean = (status or "").strip()
    total_game_min = game_minutes(match_name, tournament)

    quarter_length = (
        20 if total_game_min == 40 and _is_ncaa(match_name, tournament)
        else (12 if total_game_min == 48 else 10)
    )

    if not status_clean or re.match(r"^OT", status_clean, re.IGNORECASE):
        return {
            "period": None,
            "remaining_min": None,
            "quarter_length": quarter_length,
            "total_game_min": total_game_min,
        }

    period = None
    remaining_min = None

    if re.match(r"^HT$", status_clean, re.IGNORECASE):
        period = 1 if quarter_length == 20 else 2
        remaining_min = 0.0

    if period is None:
        ended_q_match = re.match(
            r"^(?:Q(\d)|(\d)Q)\s*[-\s]?\s*Ended$",
            status_clean,
            re.IGNORECASE,
        )
        if ended_q_match:
            period = int(ended_q_match.group(1) or ended_q_match.group(2))
            remaining_min = 0.0

    if period is None:
        q_match = re.match(
            r"(?:Q(\d)|(\d)Q)\s*[-:\s]?\s*(\d{1,2}):(\d{2})",
            status_clean,
            re.IGNORECASE,
        )
        if q_match:
            period = int(q_match.group(1) or q_match.group(2))
            remaining_min = int(q_match.group(3)) + int(q_match.group(4)) / 60.0

    if period is None:
        ord_match = re.match(
            r"(\d)(?:st|nd|rd|th)\s*[-:\s]?\s*(?:(\d{1,2}):(\d{2}))?",
            status_clean,
            re.IGNORECASE,
        )
        if ord_match:
            period = int(ord_match.group(1))
            if ord_match.group(2) and ord_match.group(3):
                remaining_min = int(ord_match.group(2)) + int(ord_match.group(3)) / 60.0
            else:
                remaining_min = quarter_length / 2

    if period is None:
        q_only = re.match(r"^(?:Q(\d)|(\d)Q)$", status_clean, re.IGNORECASE)
        if q_only:
            period = int(q_only.group(1) or q_only.group(2))
            remaining_min = quarter_length / 2

    if period is None:
        h_match = re.match(r"^(\d)H(?:[-:\s]+(\d{1,2}):(\d{2}))?$", status_clean, re.IGNORECASE)
        if h_match:
            half = int(h_match.group(1))
            period = half if quarter_length == 20 else (2 if half == 1 else 4)
            if h_match.group(2) and h_match.group(3):
                remaining_min = int(h_match.group(2)) + int(h_match.group(3)) / 60.0
            else:
                remaining_min = quarter_length / 2

    if period is None:
        d_match = re.match(r"^([1-4])$", status_clean)
        if d_match:
            period = int(d_match.group(1))
            remaining_min = quarter_length / 2

    if period is None:
        nt_match = re.match(r"([1-4])[-:\s]+(\d{1,2}):(\d{2})", status_clean)
        if nt_match:
            period = int(nt_match.group(1))
            remaining_min = int(nt_match.group(2)) + int(nt_match.group(3)) / 60.0

    return {
        "period": period,
        "remaining_min": remaining_min,
        "quarter_length": quarter_length,
        "total_game_min": total_game_min,
    }


def game_minutes(match_name: str = "", tournament: str = "") -> int:
    text_to_check = f"{match_name} {tournament}".upper()
    if "NBA" in text_to_check:
        return 48
    return 40


def _is_ncaa(match_name: str = "", tournament: str = "") -> bool:
    return "NCAA" in f"{match_name} {tournament}".upper()


def calculate_projected_total(
    score: str, status: str, match_name: str = "", tournament: str = ""
) -> float | None:
    """
    Mevcut skor ve oyun saatine göre tahmini final toplam skorunu hesaplar.

    Formül: toplam_atılan + (mevcut_hız × regresyon × kalan_süre)

    Regresyon: Q1 hızı overestimate eder (takımlar taze), bu nedenle
    erken çeyreklerde kalan süre hesabına katsayı uygulanır.
    Q4'te regresyon = 1.0 (mevcut hız en güvenilir bölge).
    """
    home_score, away_score = parse_score(score)
    if home_score is None or away_score is None:
        return None

    total_pts = home_score + away_score
    clock = game_clock(status, match_name, tournament)
    period = clock["period"]
    remaining_min = clock["remaining_min"]
    quarter_length = clock["quarter_length"]
    total_game_min = clock["total_game_min"]

    # "Live" statüsü: çeyrek bilgisi yoksa skora bakarak tahmin et
    estimated = False
    if period is None or remaining_min is None:
        period, remaining_min = estimate_period_from_score(
            total_pts, total_game_min, quarter_length
        )
        estimated = True

    elapsed_min = (period - 1) * quarter_length + (quarter_length - remaining_min)
    if elapsed_min <= 1:
        return None

    # Kalan toplam süre (mevcut çeyreğin kalanı + ilerleyen çeyrekler)
    remaining_total_min = (4 - period) * quarter_length + remaining_min
    if remaining_total_min <= 0:
        return float(total_pts)

    raw_pace = total_pts / elapsed_min  # puan/dakika

    # Çeyreğe özgü regresyon; tahmin edildiyse ek -5% belirsizlik marjı
    regression = _PACE_REGRESSION.get(period, 0.95)
    if estimated:
        regression = max(0.82, regression - 0.05)

    expected_remaining = raw_pace * regression * remaining_total_min
    return round(total_pts + expected_remaining, 1)


def _safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace("%", "").replace(",", ".").strip())
    except (TypeError, ValueError):
        return None


def _stat_value(stats: dict | None, *names: str) -> float | None:
    if not isinstance(stats, dict):
        return None
    normalized = {
        re.sub(r"[^a-z0-9]", "", str(k).lower()): v
        for k, v in stats.items()
    }
    for name in names:
        key = re.sub(r"[^a-z0-9]", "", name.lower())
        for candidate, value in normalized.items():
            if key in candidate or candidate in key:
                return _safe_float(value)
    return None


def _stat_percent_value(stats: dict | None, *names: str) -> float | None:
    if not isinstance(stats, dict):
        return None
    normalized = {
        re.sub(r"[^a-z0-9%]", "", str(k).lower()): v
        for k, v in stats.items()
    }
    percent_markers = ("pct", "percent", "percentage", "%")
    for name in names:
        key = re.sub(r"[^a-z0-9%]", "", name.lower())
        for candidate, value in normalized.items():
            if not any(marker in candidate for marker in percent_markers):
                continue
            if key in candidate or candidate in key:
                return _safe_float(value)
    return None


def _quarter_totals(quarter_scores: dict | None) -> list[int]:
    """
    Çeyrek toplamlarını döndürür. AiScore canlı yayında devam eden veya
    henüz başlamamış çeyrekleri 0 olarak ya da kısmi skorla listelediği
    için, listenin sonundan başlayarak şüpheli (0 veya 12 puanın altı)
    girdileri atar — bunlar tamamlanmış çeyrek olamaz.
    """
    if not isinstance(quarter_scores, dict):
        return []
    home = quarter_scores.get("home") or []
    away = quarter_scores.get("away") or []
    totals = []
    for h, a in zip(home, away):
        try:
            total = int(h) + int(a)
        except (TypeError, ValueError):
            continue
        if 0 <= total <= 100:
            totals.append(total)
    # Trailing zero veya gerçekçi olmayan düşük çeyrek toplamlarını ayıkla.
    # Tamamlanmış bir basket çeyreğinde iki takım toplamı çok nadiren 8'in
    # altında olur (savunmacı kadın basketbolu / amatör ligler dahil).
    # 8 altı genelde devam eden ya da boş kayıttır.
    while totals and totals[-1] < 8:
        totals.pop()
    return totals[:4]


def calculate_live_projection(
    score: str,
    status: str,
    match_name: str = "",
    tournament: str = "",
    *,
    quarter_scores: dict | None = None,
    live_stats: dict | None = None,
    market_total: float | None = None,
    opening_total: float | None = None,
) -> dict:
    """
    Canlı final projeksiyonu.

    Basit skor/süre projeksiyonunu temel alır, sonra AiScore'dan çekilen
    çeyrek dağılımı ve canlı istatistiklerle düzeltir. Amaç tek bir lineer pace
    hesabı yapmak değil; maçın sürdürülebilir temposunu tahmin etmektir.
    """
    base = calculate_projected_total(score, status, match_name, tournament)
    home_score, away_score = parse_score(score)
    clock = game_clock(status, match_name, tournament)
    period = clock["period"]
    remaining_min = clock["remaining_min"]
    quarter_length = clock["quarter_length"]
    total_game_min = clock["total_game_min"]

    if base is None or home_score is None or away_score is None:
        return {
            "projected_total": base,
            "raw_projected_total": base,
            "data_quality": 20,
            "components": {},
            "notes": ["Skor/süre güvenilir okunamadı."],
        }

    total_pts = home_score + away_score
    elapsed = None
    remaining_total = None
    if period is not None and remaining_min is not None:
        elapsed = (period - 1) * quarter_length + (quarter_length - remaining_min)
        remaining_total = max(0.0, total_game_min - elapsed)

    q_totals = _quarter_totals(quarter_scores)
    completed_q = []
    if period is not None:
        completed_q = q_totals[: max(0, period - 1)]
    if not completed_q and len(q_totals) >= 2:
        completed_q = q_totals[:-1]

    sustainable_pace = None
    if completed_q:
        completed_paces = [q / quarter_length for q in completed_q if q > 0]
        if completed_paces:
            sustainable_pace = sum(completed_paces) / len(completed_paces)

    current_pace = None
    if elapsed and elapsed > 1:
        current_pace = total_pts / elapsed

    pace_blend = current_pace
    if sustainable_pace and current_pace:
        # Maç ilerledikçe canlı pace daha güvenilir, ama çeyrek dağılımı
        # anormal spike/dip'leri yumuşatır.
        live_weight = 0.55 if period and period <= 2 else 0.68
        pace_blend = current_pace * live_weight + sustainable_pace * (1 - live_weight)

    projected = base
    notes: list[str] = []
    components = {
        "base_projection": round(base, 1),
        "current_pace_per_min": round(current_pace, 3) if current_pace else None,
        "sustainable_pace_per_min": round(sustainable_pace, 3) if sustainable_pace else None,
    }
    if pace_blend and remaining_total is not None:
        projected = total_pts + pace_blend * remaining_total
        components["pace_blend_projection"] = round(projected, 1)

    # "Saf" projeksiyon: market_anchor uygulanmadan önceki değer.
    # Adil barem hesabı bunu kullanır (gizli double-counting'i önler).
    pure_pre_anchor = projected

    # Market anchor: canlı line piyasanın injury/rotation haberlerini de içerir.
    # Erken bölümde daha çok, son bölümde daha az ağırlık verilir.
    market_anchor = _safe_float(market_total) or _safe_float(opening_total)
    if market_anchor is not None:
        if period is None or period <= 1:
            mw = 0.35
        elif period == 2:
            mw = 0.25
        elif period == 3:
            mw = 0.16
        else:
            mw = 0.08
        projected = projected * (1 - mw) + market_anchor * mw
        components["market_anchor"] = round(market_anchor, 1)
        components["market_weight"] = round(mw, 2)

    # Live stats modifiers. Bunlar final toplamını değil kalan tempo beklentisini
    # düzeltir; şut yüzdesi aşırılıklarında regresyon, faul/FT'de tempo artışı.
    stat_adj = 0.0
    fg_pct = _stat_percent_value(live_stats, "field goal", "fg")
    three_pct = _stat_percent_value(live_stats, "3 point", "three point", "3pt")
    ft_attempts = _stat_value(live_stats, "free throw attempts", "fta")
    ft_makes = _stat_value(live_stats, "ft_makes", "free throws", "free throw", "ft")
    fouls = _stat_value(live_stats, "fouls", "personal fouls")
    turnovers = _stat_value(live_stats, "turnovers")

    if three_pct is not None:
        if three_pct >= 43:
            stat_adj -= 2.5
            notes.append("3 sayı yüzdesi yüksek; şut regresyonu projeksiyonu aşağı çeker.")
        elif three_pct <= 25:
            stat_adj += 2.0
            notes.append("3 sayı yüzdesi düşük; normalleşme projeksiyonu yukarı çekebilir.")
    if fg_pct is not None:
        if fg_pct >= 53:
            stat_adj -= 1.8
        elif fg_pct <= 39:
            stat_adj += 1.5
    if elapsed and (ft_attempts is not None or ft_makes is not None):
        ft_value = ft_attempts if ft_attempts is not None else ft_makes
        ft_per_40 = ft_value / elapsed * 40
        components["ft_per_40"] = round(ft_per_40, 1)
        components["ft_source"] = "attempts" if ft_attempts is not None else "made"
        high_ft = 42 if ft_attempts is not None else 26
        low_ft = 18 if ft_attempts is not None else 10
        if ft_per_40 >= high_ft:
            stat_adj += 3.0
            notes.append("FT trafiği yüksek; oyun durarak skor üretimi destekleniyor.")
        elif ft_value > 0 and ft_per_40 <= low_ft:
            stat_adj -= 1.5
    if fouls is not None and elapsed:
        fouls_per_40 = fouls / elapsed * 40
        components["fouls_per_40"] = round(fouls_per_40, 1)
        if fouls_per_40 >= 44:
            stat_adj += 2.0
    if turnovers is not None and elapsed:
        tov_per_40 = turnovers / elapsed * 40
        components["turnovers_per_40"] = round(tov_per_40, 1)
        if tov_per_40 >= 32:
            stat_adj -= 2.0
            notes.append("Top kaybı oranı yüksek; hücum verimi aşağı baskılanıyor.")

    projected += stat_adj
    pure_pre_anchor += stat_adj  # saf projeksiyon da stat düzeltmesini alır
    components["stats_adjustment"] = round(stat_adj, 1)

    # Maç scripti: blowout/close-game etkisi.
    gap = abs(home_score - away_score)
    script_adj = 0.0
    if period and period >= 4 and remaining_min is not None:
        if gap <= 7 and remaining_min <= 6:
            script_adj += 3.5
            notes.append("Yakın Q4: faul oyunu ve hızlı hücum riski projeksiyonu yükseltir.")
        elif gap >= 16 and remaining_min <= 6:
            script_adj -= 5.0
            notes.append("Q4 blowout: rotasyon/tempo düşüşü projeksiyonu aşağı çeker.")
    elif period and period >= 3 and gap >= 18:
        script_adj -= 2.5
        notes.append("Maç kopma eğiliminde; tempo sürdürülebilirliği düşebilir.")
    projected += script_adj
    pure_pre_anchor += script_adj  # saf projeksiyon script düzeltmesini de alır
    components["script_adjustment"] = round(script_adj, 1)

    data_quality = 45
    if elapsed is not None:
        data_quality += 20
    if q_totals:
        data_quality += 15
    if isinstance(live_stats, dict) and live_stats:
        data_quality += 15
    if market_anchor is not None:
        data_quality += 5

    return {
        "projected_total": round(projected, 1),
        "raw_projected_total": round(base, 1),
        # pure_projected_total = pace_blend + stat_adj + script_adj (market_anchor YOK)
        # Adil barem bu alanı kullanır, prematch ile "temiz" şekilde harmanlamak için.
        "pure_projected_total": round(pure_pre_anchor, 1),
        "data_quality": max(0, min(100, data_quality)),
        "components": components,
        "notes": notes,
        "quarter_totals": q_totals,
    }
