import math
import re


PROJECTION_MODEL_VERSION = "shadow_projection_v1"

# Fixed v1 research coefficients. They deliberately keep the observed-pace
# contribution small because raw early-game pace is unstable. These values are
# not win probabilities and remain subject to prospective validation.
_OBSERVED_PACE_WEIGHT = {1: 0.06, 2: 0.14, 3: 0.24, 4: 0.02}

# Yalnız maç önü baremi yoksa kullanılan nötr görünürlük fallback'i. Bu
# fallback ile üretilen kayıt güven kapısından geçmez.
_DEFAULT_PRIOR_TOTAL = {40: 160.0, 48: 220.0}

_PRIOR_TOTAL_MIN = 80.0
_PRIOR_TOTAL_MAX = 350.0


def parse_score(score: str) -> tuple[int | None, int | None]:
    match = re.search(r"(\d+)\s*[-–]\s*(\d+)", score or "")
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def game_clock(status: str, match_name: str = "", tournament: str = "") -> dict:
    status_clean = (status or "").strip()
    total_game_min = game_minutes(match_name, tournament)

    uses_halves = _uses_halves(match_name, tournament)
    quarter_length = 20 if uses_halves else (12 if total_game_min == 48 else 10)
    period_count = 2 if uses_halves else 4
    competition = str(tournament or "").strip().lower()
    competition_known = bool(competition and competition not in {"unknown", "bilinmiyor", "-"})
    model_validated = (
        total_game_min == 40
        and period_count == 4
        and not _is_ncaa(match_name, tournament)
        and competition_known
    )

    if not status_clean or re.match(r"^OT", status_clean, re.IGNORECASE):
        return {
            "period": None,
            "remaining_min": None,
            "quarter_length": quarter_length,
            "total_game_min": total_game_min,
            "period_count": period_count,
            "format": _format_name(total_game_min, period_count),
            "model_validated": model_validated,
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

    if period is None:
        q_only = re.match(r"^(?:Q(\d)|(\d)Q)$", status_clean, re.IGNORECASE)
        if q_only:
            period = int(q_only.group(1) or q_only.group(2))

    if period is None:
        h_match = re.match(r"^(\d)H(?:[-:\s]+(\d{1,2}):(\d{2}))?$", status_clean, re.IGNORECASE)
        if h_match:
            half = int(h_match.group(1))
            period = half if quarter_length == 20 else (2 if half == 1 else 4)
            if h_match.group(2) and h_match.group(3):
                remaining_min = int(h_match.group(2)) + int(h_match.group(3)) / 60.0

    if period is None:
        d_match = re.match(r"^([1-4])$", status_clean)
        if d_match:
            period = int(d_match.group(1))

    if period is None:
        nt_match = re.match(r"([1-4])[-:\s]+(\d{1,2}):(\d{2})", status_clean)
        if nt_match:
            period = int(nt_match.group(1))
            remaining_min = int(nt_match.group(2)) + int(nt_match.group(3)) / 60.0

    if period is not None and (period < 1 or period > period_count):
        period = None
        remaining_min = None
    elif remaining_min is not None and (
        remaining_min < 0 or remaining_min > quarter_length
    ):
        period = None
        remaining_min = None

    return {
        "period": period,
        "remaining_min": remaining_min,
        "quarter_length": quarter_length,
        "total_game_min": total_game_min,
        "period_count": period_count,
        "format": _format_name(total_game_min, period_count),
        "model_validated": model_validated,
    }


def game_minutes(match_name: str = "", tournament: str = "") -> int:
    text_to_check = f"{match_name} {tournament}".upper()
    is_women = any(token in text_to_check for token in ("WNBA", "WOMEN", "WOMEN'S", "WOMAN"))
    is_nba = (
        re.search(r"\bNBA\b", text_to_check)
        or "NATIONAL BASKETBALL ASSOCIATION" in text_to_check
    )
    if is_nba and not is_women:
        return 48
    return 40


def _is_ncaa(match_name: str = "", tournament: str = "") -> bool:
    return "NCAA" in f"{match_name} {tournament}".upper()


def _is_womens(match_name: str = "", tournament: str = "") -> bool:
    text = f"{match_name} {tournament}".upper()
    return any(
        token in text
        for token in ("WNBA", "WOMEN", "WOMEN'S", "WOMAN", "KADIN")
    )


def _uses_halves(match_name: str = "", tournament: str = "") -> bool:
    return _is_ncaa(match_name, tournament) and not _is_womens(match_name, tournament)


def _format_name(total_game_min: int, period_count: int) -> str:
    if total_game_min == 48 and period_count == 4:
        return "4x12"
    if total_game_min == 40 and period_count == 2:
        return "2x20"
    if total_game_min == 40 and period_count == 4:
        return "4x10"
    return "unsupported"


def calculate_projected_total(
    score: str,
    status: str,
    match_name: str = "",
    tournament: str = "",
    *,
    prior_total: float | None = None,
) -> float | None:
    """
    Maç önü sayı/dakika önselini gözlenen canlı tempoya doğru kontrollü
    günceller. Saat bilinmiyorsa kesinlik icat etmek yerine None döndürür.
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
    period_count = clock["period_count"]

    if period is None or remaining_min is None:
        return None

    elapsed_min = (period - 1) * quarter_length + (quarter_length - remaining_min)
    if elapsed_min <= 1:
        return None

    remaining_total_min = (period_count - period) * quarter_length + remaining_min
    if remaining_total_min <= 0:
        return float(total_pts)

    parsed_prior, _ = _canonical_prior_total(prior_total, total_game_min)
    prior_ppm = parsed_prior / total_game_min
    observed_ppm = total_pts / elapsed_min
    observed_weight = _OBSERVED_PACE_WEIGHT.get(period)
    if observed_weight is None:
        return None
    remaining_ppm = prior_ppm + observed_weight * (observed_ppm - prior_ppm)
    projected = total_pts + remaining_total_min * max(0.0, remaining_ppm)
    return round(max(float(total_pts), projected), 1)


def _safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(str(value).replace("%", "").replace(",", ".").strip())
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _canonical_prior_total(value, total_game_min: int) -> tuple[float, bool]:
    parsed = _safe_float(value)
    if (
        parsed is not None
        and _PRIOR_TOTAL_MIN <= parsed <= _PRIOR_TOTAL_MAX
    ):
        return parsed, True
    return _DEFAULT_PRIOR_TOTAL.get(total_game_min, 160.0), False


def _select_prior_total(
    market_total,
    opening_total,
    total_game_min: int,
) -> tuple[float, str, bool, bool]:
    """Return one canonical prior and whether supplied values were invalid."""
    invalid_supplied = False
    for source, raw in (
        ("market_total", market_total),
        ("opening_total", opening_total),
    ):
        if raw is None or str(raw).strip() == "":
            continue
        parsed = _safe_float(raw)
        if (
            parsed is not None
            and _PRIOR_TOTAL_MIN <= parsed <= _PRIOR_TOTAL_MAX
        ):
            return parsed, source, True, invalid_supplied
        invalid_supplied = True

    fallback, _ = _canonical_prior_total(None, total_game_min)
    return fallback, "neutral_fallback", False, invalid_supplied


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


def calculate_quarter_ppm(
    quarter_totals: list[int],
    *,
    period: int | None,
    remaining_min: float | None,
    quarter_length: float,
) -> list[float]:
    """Calculate live-period pace with elapsed time, not full period time."""
    values: list[float] = []
    for index, total in enumerate(quarter_totals[:4], start=1):
        played_minutes = float(quarter_length)
        if index == period and remaining_min is not None and remaining_min > 0:
            played_minutes = max(
                0.0,
                min(float(quarter_length), float(quarter_length) - float(remaining_min)),
            )
        if played_minutes <= 0:
            continue
        values.append(round(float(total) / played_minutes, 2))
    return values


def calculate_live_projection(
    score: str,
    status: str,
    match_name: str = "",
    tournament: str = "",
    *,
    quarter_scores: dict | None = None,
    market_total: float | None = None,
    opening_total: float | None = None,
) -> dict:
    """Return the versioned, leakage-safe shadow projection and diagnostics."""
    clock = game_clock(status, match_name, tournament)
    total_game_min = clock["total_game_min"]
    prior_total, prior_source, prior_valid, invalid_prior = _select_prior_total(
        market_total,
        opening_total,
        total_game_min,
    )
    base = calculate_projected_total(
        score,
        status,
        match_name,
        tournament,
        prior_total=prior_total,
    )
    home_score, away_score = parse_score(score)
    period = clock["period"]
    remaining_min = clock["remaining_min"]
    quarter_length = clock["quarter_length"]

    if base is None or home_score is None or away_score is None:
        notes = ["Skor/süre güvenilir okunamadı."]
        if not prior_valid:
            notes.append(
                "Maç önü baremi geçersiz."
                if invalid_prior
                else "Maç önü baremi yok."
            )
        return {
            "projected_total": base,
            "raw_projected_total": base,
            "pure_projected_total": base,
            "model_version": PROJECTION_MODEL_VERSION,
            "data_quality": 20,
            "components": {
                "model_version": PROJECTION_MODEL_VERSION,
                "game_format": clock.get("format"),
                "model_validated": bool(clock.get("model_validated")),
                "prior_total": round(prior_total, 1),
                "prior_source": prior_source,
                "prior_valid": prior_valid,
            },
            "notes": notes,
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

    completed_period_pace = None
    if completed_q:
        completed_paces = [q / quarter_length for q in completed_q if q > 0]
        if completed_paces:
            completed_period_pace = sum(completed_paces) / len(completed_paces)

    current_pace = None
    if elapsed and elapsed > 1:
        current_pace = total_pts / elapsed

    projected = float(base)
    notes: list[str] = []
    observed_weight = _OBSERVED_PACE_WEIGHT.get(period) if period is not None else None
    raw_pace_projection = (
        total_pts / elapsed * total_game_min
        if elapsed is not None and elapsed > 1
        else None
    )
    components = {
        "model_version": PROJECTION_MODEL_VERSION,
        "game_format": clock.get("format"),
        "model_validated": bool(clock.get("model_validated")),
        "prior_total": round(prior_total, 1),
        "prior_source": prior_source,
        "prior_valid": prior_valid,
        "prior_pace_per_min": round(prior_total / total_game_min, 3),
        "observed_pace_weight": observed_weight,
        "base_projection": round(projected, 1),
        "current_pace_per_min": round(current_pace, 3) if current_pace else None,
        "sustainable_pace_per_min": round(
            (projected - total_pts) / remaining_total, 3
        ) if remaining_total and remaining_total > 0 else None,
        "completed_period_pace_per_min": round(completed_period_pace, 3)
        if completed_period_pace
        else None,
        "raw_pace_projection": round(raw_pace_projection, 1)
        if raw_pace_projection is not None
        else None,
    }

    # Script etkisi doğrulanmış bir katsayı olmadığı için sayıya eklenmez;
    # yalnız risk notu üretir. Böylece kalan süre sıfırda projeksiyon skora yaklaşır.
    gap = abs(home_score - away_score)
    script_adj = 0.0
    if period and period >= 4 and remaining_min is not None:
        if gap <= 7 and remaining_min <= 6:
            notes.append("Yakın Q4: faul oyunu ve hızlı hücum belirsizliği yüksek.")
        elif gap >= 16 and remaining_min <= 6:
            notes.append("Q4 blowout: rotasyon ve tempo düşüşü riski yüksek.")
    elif period and period >= 3 and gap >= 18:
        notes.append("Maç kopma eğiliminde; tempo sürdürülebilirliği belirsiz.")
    components["script_adjustment"] = round(script_adj, 1)

    if not prior_valid:
        if invalid_prior:
            notes.append(
                "Maç önü baremi geçersiz; nötr fallback yalnız görünürlük için kullanıldı."
            )
        else:
            notes.append(
                "Maç önü baremi yok; nötr fallback yalnız görünürlük için kullanıldı."
            )
    if not clock.get("model_validated"):
        notes.append(
            f"{clock.get('format') or 'Bilinmeyen'} formatı bu model sürümünde ileri tarihli doğrulanmadı."
        )

    data_quality = 30
    if elapsed is not None:
        data_quality += 25
    if prior_valid:
        data_quality += 20
    if elapsed is not None and elapsed >= 4:
        data_quality += 15
    if q_totals:
        data_quality += 10
    if period and period >= 2 and not completed_q:
        data_quality -= 10
    if not clock.get("model_validated"):
        data_quality = min(data_quality, 69)

    return {
        "projected_total": round(projected, 1),
        "raw_projected_total": round(raw_pace_projection, 1)
        if raw_pace_projection is not None
        else None,
        "pure_projected_total": round(projected, 1),
        "model_version": PROJECTION_MODEL_VERSION,
        "data_quality": max(0, min(100, data_quality)),
        "components": components,
        "notes": notes,
        "quarter_totals": q_totals,
    }
