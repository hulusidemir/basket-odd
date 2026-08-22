"""Basketball-stat evidence for explaining an opening-to-live total move.

This module never blocks an alert and never treats its label as a win
probability.  It decomposes scoring into possession pace and efficiency, then
compares an independent stats-based total with the live market line.
"""

from __future__ import annotations

import math
from projection import game_clock, parse_score


MARKET_EVIDENCE_VERSION = "market_evidence_4x10_v2"
STATS_SNAPSHOT_VERSION = "aiscore_team_stats_v1"

_PRIOR_COMBINED_POINTS_PER_POSSESSION = 2.10
_MIN_DECISIVE_EDGE = 3.0
_REQUIRED_TEAM_FIELDS = (
    "points", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
    "oreb", "dreb", "tov",
)


def _safe_float(value) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _safe_int(value) -> int | None:
    parsed = _safe_float(value)
    if parsed is None or parsed < 0:
        return None
    return int(round(parsed))


def _direction(value) -> str:
    text = str(value or "").strip().upper().replace("UST", "ÜST")
    return text if text in {"ALT", "ÜST"} else ""


def _round(value, digits: int = 1):
    parsed = _safe_float(value)
    return round(parsed, digits) if parsed is not None else None


def _rate(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def _insufficient(reason: str, *, data_quality: int = 0, metrics: dict | None = None) -> dict:
    return {
        "version": MARKET_EVIDENCE_VERSION,
        "code": "insufficient",
        "label": "İSTATİSTİK YETERSİZ",
        "symbol": "?",
        "tone": "insufficient",
        "data_quality": max(0, min(100, int(data_quality))),
        "primary_reason": reason,
        "reasons": [reason],
        "metrics": metrics or {},
    }


def _possessions(team: dict) -> float | None:
    required = ("fga", "oreb", "tov", "fta")
    values = {key: _safe_float(team.get(key)) for key in required}
    if any(values[key] is None for key in required):
        return None
    value = values["fga"] - values["oreb"] + values["tov"] + 0.44 * values["fta"]
    return value if value > 0 else None


def _valid_team(team: dict) -> bool:
    values = {key: _safe_int(team.get(key)) for key in _REQUIRED_TEAM_FIELDS}
    if any(value is None for value in values.values()):
        return False
    return (
        values["fgm"] <= values["fga"]
        and values["fg3m"] <= values["fg3a"]
        and values["fg3m"] <= values["fgm"]
        and values["fg3a"] <= values["fga"]
        and values["fgm"] - values["fg3m"] <= values["fga"] - values["fg3a"]
        and values["ftm"] <= values["fta"]
    )


def _missing_team_fields(team: dict) -> list[str]:
    return [key for key in _REQUIRED_TEAM_FIELDS if _safe_int(team.get(key)) is None]


def _has_any_team_stat(home: dict, away: dict) -> bool:
    fields = (*_REQUIRED_TEAM_FIELDS, "pf")
    return any(
        _safe_float(team.get(key)) is not None
        for team in (home, away)
        for key in fields
    )


def _shooting_points(team: dict) -> int | None:
    fgm = _safe_int(team.get("fgm"))
    fg3m = _safe_int(team.get("fg3m"))
    ftm = _safe_int(team.get("ftm"))
    if None in (fgm, fg3m, ftm):
        return None
    return 2 * (fgm - fg3m) + 3 * fg3m + ftm


def _recent_flow_note(stats: dict) -> tuple[str | None, float]:
    flow = stats.get("current_period_flow") if isinstance(stats.get("current_period_flow"), dict) else {}
    fouls = _safe_int(flow.get("foul_events")) or 0
    free_throws = _safe_int(flow.get("free_throw_events")) or 0
    turnovers = _safe_int(flow.get("turnover_events")) or 0
    if fouls >= 6 or free_throws >= 8:
        return (
            f"Mevcut çeyrekte faul/serbest atış trafiği yüksek ({fouls} faul, {free_throws} atış).",
            0.8,
        )
    if fouls <= 2 and free_throws <= 2 and turnovers <= 3:
        return "Mevcut çeyrekte belirgin faul kaosu görünmüyor.", 0.0
    return None, 0.0


def _opposite_direction(direction: str) -> str:
    return "ÜST" if direction == "ALT" else "ALT"


def _classify_edge(signal_edge: float, signal_direction: str) -> dict:
    """Classify only the independent fair-vs-live edge.

    The opening-to-live move includes points that are already on the board.
    Those realised points explain *why* a line moved, but they are not evidence
    that either side of the current live line has value.  A symmetric deadband
    keeps a near-market estimate neutral instead of treating it as a market win.
    """
    if signal_edge >= _MIN_DECISIVE_EDGE:
        return {
            "code": "supports_signal",
            "label": "SİNYAL DESTEKLENİYOR",
            "symbol": "✓",
            "tone": "supports",
            "supported_direction": signal_direction,
        }
    if signal_edge <= -_MIN_DECISIVE_EDGE:
        return {
            "code": "opposes_signal",
            "label": "SİNYALİN TERSİ DESTEKLENİYOR",
            "symbol": "⇄",
            "tone": "opposes",
            "supported_direction": _opposite_direction(signal_direction),
        }
    return {
        "code": "mixed",
        "label": "NET AYRIŞMA YOK",
        "symbol": "≈",
        "tone": "mixed",
        "supported_direction": "",
    }


def build_market_evidence(match: dict, direction: str) -> dict:
    """Return a frozen, explanatory label for one 4x10 alert snapshot."""
    clock = game_clock(
        str(match.get("status") or ""),
        str(match.get("match_name") or ""),
        str(match.get("tournament") or ""),
    )
    if (
        clock.get("period_count") != 4
        or clock.get("quarter_length") != 10
        or clock.get("total_game_min") != 40
        or not clock.get("model_validated")
    ):
        return _insufficient("Bu kanıt modeli yalnız doğrulanmış 4x10 maçlarda çalışır.")

    stats = match.get("team_stats") if isinstance(match.get("team_stats"), dict) else {}
    home = stats.get("home") if isinstance(stats.get("home"), dict) else {}
    away = stats.get("away") if isinstance(stats.get("away"), dict) else {}
    if not _valid_team(home) or not _valid_team(away):
        has_any_team_stat = _has_any_team_stat(home, away)
        if stats.get("has_stats") is False and not has_any_team_stat:
            reason = "AIScore bu maç için takım box score istatistiği yayınlamıyor."
        elif not has_any_team_stat:
            reason = "AIScore takım box score istatistiği bu snapshot'ta bulunamadı."
        else:
            reason = "AIScore takım şut/ribaund/top kaybı verisi eksik veya tutarsız."
        return _insufficient(
            reason,
            metrics={
                "has_stats": stats.get("has_stats"),
                "home_missing_fields": _missing_team_fields(home),
                "away_missing_fields": _missing_team_fields(away),
            },
        )

    opening = _safe_float(match.get("opening_total", match.get("opening")))
    live = _safe_float(match.get("inplay_total", match.get("live")))
    signal_direction = _direction(direction)
    period = _safe_int(clock.get("period"))
    remaining_in_period = _safe_float(clock.get("remaining_min"))
    if None in (opening, live, period, remaining_in_period) or not signal_direction:
        return _insufficient("Barem, yön veya kesin maç saati eksik.")

    elapsed = (period - 1) * 10 + (10 - remaining_in_period)
    if elapsed < 4:
        return _insufficient("İlk dört dakikadaki istatistik örneklemi çok küçük.", data_quality=35)

    home_possessions = _possessions(home)
    away_possessions = _possessions(away)
    if home_possessions is None or away_possessions is None:
        return _insufficient("Pozisyon sayısı güvenilir hesaplanamadı.")
    possession_gap = abs(home_possessions - away_possessions)
    if possession_gap > 5.0:
        return _insufficient(
            "İki takımın pozisyon tahminleri birbiriyle uyuşmuyor; AIScore verisi gecikmiş olabilir.",
            data_quality=40,
        )

    score_home, score_away = parse_score(str(match.get("score") or ""))
    box_points = int(home["points"]) + int(away["points"])
    score_points = score_home + score_away if score_home is not None and score_away is not None else None
    shooting_points = (_shooting_points(home) or 0) + (_shooting_points(away) or 0)
    if score_points is None or abs(box_points - score_points) > 2 or abs(shooting_points - box_points) > 2:
        return _insufficient(
            "AIScore box score ile canlı skor henüz eşleşmiyor; eski istatistikle hüküm verilmedi.",
            data_quality=45,
            metrics={"box_points": box_points, "score_points": score_points},
        )
    if live + 0.5 < box_points:
        return _insufficient(
            "Canlı barem mevcut toplam skordan düşük; piyasa snapshot'ı güvenilir değil.",
            data_quality=45,
            metrics={"box_points": box_points, "live_total": round(live, 1)},
        )

    shared_possessions = (home_possessions + away_possessions) / 2
    observed_pace_40 = shared_possessions / elapsed * 40
    expected_pace_40 = max(52.0, min(88.0, opening / _PRIOR_COMBINED_POINTS_PER_POSSESSION))
    progress = max(0.0, min(1.0, elapsed / 40))
    observed_pace_weight = 0.18 + 0.62 * progress
    future_pace_40 = (
        expected_pace_40 * (1 - observed_pace_weight)
        + observed_pace_40 * observed_pace_weight
    )

    observed_ppp = box_points / shared_possessions
    efficiency_prior_possessions = 45.0
    future_ppp = (
        _PRIOR_COMBINED_POINTS_PER_POSSESSION * efficiency_prior_possessions
        + box_points
    ) / (efficiency_prior_possessions + shared_possessions)
    remaining_minutes = max(0.0, 40.0 - elapsed)
    remaining_possessions = future_pace_40 / 40 * remaining_minutes

    flow_note, flow_adjustment = _recent_flow_note(stats)
    score_gap = abs((score_home or 0) - (score_away or 0))
    script_adjustment = 0.0
    script_note = None
    if period == 4 and score_gap <= 8:
        script_adjustment = flow_adjustment
        script_note = "Yakın Q4 skoru maç sonu faul ihtimalini canlı tutuyor."
    elif period >= 3 and score_gap >= 18:
        script_adjustment = -0.8
        script_note = "Yüksek skor farkı kalan bölümde tempoyu aşağı çekebilir."

    stats_fair = box_points + remaining_possessions * future_ppp + script_adjustment
    market_move = live - opening
    model_move = stats_fair - opening
    same_move_direction = market_move != 0 and model_move * market_move > 0
    explained_ratio = (
        abs(model_move) / abs(market_move)
        if same_move_direction and abs(market_move) >= 0.5
        else 0.0
    )
    residual = stats_fair - live
    signal_edge = residual if signal_direction == "ÜST" else -residual

    opening_elapsed_baseline = opening * progress
    opening_remaining_baseline = opening - opening_elapsed_baseline
    realised_scoring_surprise = box_points - opening_elapsed_baseline
    market_remaining_points = live - box_points
    model_remaining_points = stats_fair - box_points
    market_future_revision = market_remaining_points - opening_remaining_baseline
    model_future_revision = model_remaining_points - opening_remaining_baseline

    observed_remaining_possessions = observed_pace_40 / 40 * remaining_minutes
    naive_remaining_points = observed_remaining_possessions * observed_ppp
    pace_regression_points = (
        remaining_possessions - observed_remaining_possessions
    ) * observed_ppp
    efficiency_regression_points = remaining_possessions * (future_ppp - observed_ppp)

    total_fgm = int(home["fgm"]) + int(away["fgm"])
    total_fga = int(home["fga"]) + int(away["fga"])
    total_3pm = int(home["fg3m"]) + int(away["fg3m"])
    total_3pa = int(home["fg3a"]) + int(away["fg3a"])
    total_fta = int(home["fta"]) + int(away["fta"])
    total_oreb = int(home["oreb"]) + int(away["oreb"])
    total_dreb = int(home["dreb"]) + int(away["dreb"])
    total_tov = int(home["tov"]) + int(away["tov"])
    three_pct = _rate(total_3pm, total_3pa)
    efg = _rate(total_fgm + 0.5 * total_3pm, total_fga)
    free_throw_rate = _rate(total_fta, total_fga)
    offensive_rebound_rate = _rate(total_oreb, total_oreb + total_dreb)
    turnover_rate = _rate(total_tov, home_possessions + away_possessions)
    pace_delta_pct = (observed_pace_40 / expected_pace_40 - 1) * 100

    reasons: list[str] = []
    reasons.append(
        f"Açılış-canlı {market_move:+.1f} hareketi; gerçekleşmiş skor sapması "
        f"{realised_scoring_surprise:+.1f}, piyasanın kalan bölüm revizyonu "
        f"{market_future_revision:+.1f}."
    )
    reasons.append(
        f"Kalan bölüm beklentisi: istatistik modeli {model_remaining_points:.1f}, "
        f"canlı piyasa {market_remaining_points:.1f} sayı."
    )
    if pace_delta_pct >= 8:
        reasons.append(f"Gerçek pozisyon temposu açılış önselinin %{pace_delta_pct:.0f} üzerinde.")
    elif pace_delta_pct <= -8:
        reasons.append(f"Gerçek pozisyon temposu açılış önselinin %{abs(pace_delta_pct):.0f} altında.")
    else:
        reasons.append("Gerçek pozisyon temposu açılış önseline yakın.")

    if three_pct is not None and total_3pa >= 8:
        if three_pct >= 0.45:
            reasons.append(f"Üçlük isabeti olağan dışı yüksek: {total_3pm}/{total_3pa} (%{three_pct * 100:.1f}); regresyon ihtimali var.")
        elif three_pct <= 0.25:
            reasons.append(f"Üçlük isabeti olağan dışı düşük: {total_3pm}/{total_3pa} (%{three_pct * 100:.1f}); normalleşme ihtimali var.")
        else:
            reasons.append(f"Üçlük üretimi aşırı bölgede değil: {total_3pm}/{total_3pa} (%{three_pct * 100:.1f}).")
    if free_throw_rate is not None and free_throw_rate >= 0.35:
        reasons.append(f"Serbest atış trafiği yüksek (FTA/FGA %{free_throw_rate * 100:.1f}).")
    if offensive_rebound_rate is not None and offensive_rebound_rate >= 0.32:
        reasons.append(f"Hücum ribaundu ikinci şans üretimini artırıyor (%{offensive_rebound_rate * 100:.1f}).")
    if turnover_rate is not None and turnover_rate >= 0.20:
        reasons.append(f"Top kaybı oranı yüksek (%{turnover_rate * 100:.1f}); oyun akışı oynak.")
    if efficiency_regression_points <= -2:
        reasons.append(
            f"Verimlilik regresyonu kalan tahminden {abs(efficiency_regression_points):.1f} sayı geri alıyor."
        )
    elif efficiency_regression_points >= 2:
        reasons.append(
            f"Düşük verimliliğin normalleşmesi kalan tahmine {efficiency_regression_points:.1f} sayı ekliyor."
        )
    if pace_regression_points <= -2:
        reasons.append(
            f"Tempo regresyonu kalan tahminden {abs(pace_regression_points):.1f} sayı geri alıyor."
        )
    elif pace_regression_points >= 2:
        reasons.append(
            f"Tempo normalleşmesi kalan tahmine {pace_regression_points:.1f} sayı ekliyor."
        )
    if flow_note:
        reasons.append(flow_note)
    if script_note:
        reasons.append(script_note)

    data_quality = 85
    if possession_gap <= 2:
        data_quality += 8
    if abs(box_points - score_points) == 0 and abs(shooting_points - box_points) == 0:
        data_quality += 7
    data_quality = min(100, data_quality)

    verdict = _classify_edge(signal_edge, signal_direction)
    code = verdict["code"]
    label = verdict["label"]
    symbol = verdict["symbol"]
    tone = verdict["tone"]
    supported_direction = verdict["supported_direction"]
    if code == "supports_signal":
        primary = (
            f"İstatistiksel adil barem {stats_fair:.1f}; canlı {live:.1f}. "
            f"{signal_direction} yönünde {signal_edge:.1f} sayı bağımsız fark var."
        )
    elif code == "opposes_signal":
        primary = (
            f"İstatistiksel adil barem {stats_fair:.1f}; canlı {live:.1f}. "
            f"{supported_direction} yönünde {abs(signal_edge):.1f} sayı bağımsız fark var; "
            f"bu {signal_direction} sinyaline karşı."
        )
    else:
        primary = (
            f"İstatistiksel adil barem {stats_fair:.1f}; canlı {live:.1f}. "
            f"{signal_direction} yönündeki {signal_edge:+.1f} fark ±{_MIN_DECISIVE_EDGE:.1f} "
            "karar bandında; iki taraf için de net kanıt yok."
        )

    metrics = {
        "stats_fair_total": round(stats_fair, 1),
        "opening_total": round(opening, 1),
        "live_total": round(live, 1),
        "signal_edge": round(signal_edge, 1),
        "decision_edge": _MIN_DECISIVE_EDGE,
        "supported_direction": supported_direction,
        "opening_move_alignment_pct": round(explained_ratio * 100, 1),
        "realised_scoring_surprise": round(realised_scoring_surprise, 1),
        "opening_elapsed_baseline": round(opening_elapsed_baseline, 1),
        "opening_remaining_baseline": round(opening_remaining_baseline, 1),
        "market_remaining_points": round(market_remaining_points, 1),
        "model_remaining_points": round(model_remaining_points, 1),
        "market_future_revision": round(market_future_revision, 1),
        "model_future_revision": round(model_future_revision, 1),
        "naive_remaining_points": round(naive_remaining_points, 1),
        "pace_regression_points": round(pace_regression_points, 1),
        "efficiency_regression_points": round(efficiency_regression_points, 1),
        "script_adjustment": round(script_adjustment, 1),
        "elapsed_minutes": round(elapsed, 2),
        "shared_possessions": round(shared_possessions, 2),
        "observed_pace_40": round(observed_pace_40, 1),
        "expected_pace_40": round(expected_pace_40, 1),
        "future_pace_40": round(future_pace_40, 1),
        "observed_combined_ppp": round(observed_ppp, 3),
        "future_combined_ppp": round(future_ppp, 3),
        "efg_pct": _round((efg or 0) * 100) if efg is not None else None,
        "three_points": f"{total_3pm}/{total_3pa}",
        "three_pct": _round((three_pct or 0) * 100) if three_pct is not None else None,
        "free_throw_rate_pct": _round((free_throw_rate or 0) * 100) if free_throw_rate is not None else None,
        "offensive_rebound_rate_pct": _round((offensive_rebound_rate or 0) * 100) if offensive_rebound_rate is not None else None,
        "turnover_rate_pct": _round((turnover_rate or 0) * 100) if turnover_rate is not None else None,
        "score_gap": score_gap,
        "possession_estimate_gap": round(possession_gap, 2),
        "stats_source": stats.get("source") or "aiscore",
        "stats_snapshot_version": stats.get("version") or STATS_SNAPSHOT_VERSION,
        "home": {key: home.get(key) for key in ("points", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "oreb", "dreb", "tov", "pf")},
        "away": {key: away.get(key) for key in ("points", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "oreb", "dreb", "tov", "pf")},
        "current_period_flow": stats.get("current_period_flow") or {},
    }
    return {
        "version": MARKET_EVIDENCE_VERSION,
        "code": code,
        "label": label,
        "symbol": symbol,
        "tone": tone,
        "data_quality": data_quality,
        "primary_reason": primary,
        "reasons": [primary, *reasons],
        "metrics": metrics,
    }
