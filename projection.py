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
