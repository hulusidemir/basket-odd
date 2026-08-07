from __future__ import annotations

import math
import re

from projection import calculate_live_projection, game_clock, parse_score


SIGNAL_SCORE_VERSION = "market_edge_league_v1"
SIGNAL_SCORE_EPOCH = "2026-08-04T00:00:00+03:00"
LEAGUE_EDGE_MINIMUM = 4.0


def _safe_float(value) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _normalize_direction(value) -> str:
    text = str(value or "").strip().upper().replace("UST", "ÜST")
    return text if text in {"ALT", "ÜST"} else ""


def _final_total(value) -> float | None:
    match = re.fullmatch(r"\s*(\d{1,3})\s*[-–]\s*(\d{1,3})\s*", str(value or ""))
    if not match:
        return None
    total = float(int(match.group(1)) + int(match.group(2)))
    return total if 60 <= total <= 400 else None


def _quality_label(score: int) -> str:
    if score >= 85:
        return "ÇOK GÜÇLÜ"
    if score >= 75:
        return "GÜÇLÜ"
    if score >= 65:
        return "ORTA-GÜÇLÜ"
    if score >= 50:
        return "RİSKLİ"
    return "ÇOK RİSKLİ"


def _stars(score: int) -> int:
    if score >= 85:
        return 5
    if score >= 75:
        return 4
    if score >= 65:
        return 3
    if score >= 50:
        return 2
    return 1


def _bucket_stats(wins: int, resolved: int) -> dict:
    if resolved <= 0:
        return {"wins": 0, "resolved": 0, "rate": None, "adjusted_rate": None}
    rate = wins / resolved * 100.0
    # Ten neutral pseudo-games stop a tiny 3/3 or 4/4 sample from presenting
    # itself as trustworthy league evidence.
    adjusted = (wins + 5.0) / (resolved + 10.0) * 100.0
    return {
        "wins": wins,
        "resolved": resolved,
        "rate": round(rate, 1),
        "adjusted_rate": round(adjusted, 1),
    }


def _historical_strategy_observation(row: dict) -> dict | None:
    """Re-evaluate one archived alert with the current fair-line strategy."""
    final_total = _final_total(row.get("final_score"))
    live = _safe_float(row.get("live"))
    opening = _safe_float(row.get("opening"))
    if final_total is None or live is None or opening is None:
        return None

    prematch = _safe_float(row.get("prematch"))
    projection = calculate_live_projection(
        str(row.get("score") or ""),
        str(row.get("status") or ""),
        str(row.get("match_name") or ""),
        str(row.get("tournament") or ""),
        market_total=prematch if prematch is not None else opening,
        opening_total=opening,
    )
    projected = _safe_float(projection.get("projected_total"))
    clock = game_clock(
        str(row.get("status") or ""),
        str(row.get("match_name") or ""),
        str(row.get("tournament") or ""),
    )
    home, away = parse_score(str(row.get("score") or ""))
    if (
        projected is None
        or home is None
        or away is None
        or clock.get("period") is None
        or clock.get("remaining_min") is None
    ):
        return None

    elapsed = (
        (int(clock["period"]) - 1) * float(clock.get("quarter_length") or 10)
        + (float(clock.get("quarter_length") or 10) - float(clock["remaining_min"]))
    )
    from signal_analysis import calculate_fair_line

    fair, _meta = calculate_fair_line(
        prematch=prematch if prematch is not None else opening,
        pure_pace_projection=projected,
        elapsed_minutes=elapsed,
        total_game_minutes=int(clock.get("total_game_min") or 40),
        live_line=live,
        period=int(clock["period"]),
        current_total=float(home + away),
    )
    fair = _safe_float(fair)
    if fair is None:
        return None
    fair_edge = fair - live
    if abs(fair_edge) < LEAGUE_EDGE_MINIMUM:
        return None
    direction = "ÜST" if fair_edge > 0 else "ALT"
    if abs(final_total - live) < 0.0001:
        return None
    won = final_total > live if direction == "ÜST" else final_total < live
    return {
        "direction": direction,
        "won": bool(won),
        "fair_edge": round(fair_edge, 1),
    }


def build_league_signal_profile(rows: list[dict] | None) -> dict:
    """Build match-unique, versioned league evidence for the new strategy."""
    first_by_match: dict[str, tuple[tuple, dict]] = {}
    for row in rows or []:
        match_id = str(row.get("match_id") or "").strip()
        if not match_id:
            continue
        rank = (
            int(row.get("signal_count") or 999),
            str(row.get("alerted_at") or ""),
            int(row.get("id") or 0),
        )
        current = first_by_match.get(match_id)
        if current is None or rank < current[0]:
            first_by_match[match_id] = (rank, row)

    counters: dict[tuple[str, str], dict[str, int]] = {}

    def add(scope: str, direction: str, won: bool) -> None:
        bucket = counters.setdefault((scope, direction), {"wins": 0, "resolved": 0})
        bucket["resolved"] += 1
        if won:
            bucket["wins"] += 1

    for _rank, row in first_by_match.values():
        tournament = str(row.get("tournament") or "").strip()
        if not tournament:
            continue
        observation = _historical_strategy_observation(row)
        if not observation:
            continue
        direction = observation["direction"]
        won = observation["won"]
        for scope in ("__global__", tournament):
            add(scope, "overall", won)
            add(scope, direction, won)

    profile = {
        "version": SIGNAL_SCORE_VERSION,
        "epoch": SIGNAL_SCORE_EPOCH,
        "global": {},
        "leagues": {},
    }
    for (scope, direction), bucket in counters.items():
        stats = _bucket_stats(bucket["wins"], bucket["resolved"])
        if scope == "__global__":
            profile["global"][direction] = stats
        else:
            profile["leagues"].setdefault(scope, {})[direction] = stats
    return profile


def league_stats_for_signal(profile: dict | None, tournament: str, direction: str) -> dict:
    profile = profile if isinstance(profile, dict) else {}
    league = (profile.get("leagues") or {}).get(str(tournament or "").strip()) or {}
    direction = _normalize_direction(direction)
    directional = league.get(direction) if direction else None
    overall = league.get("overall") or {}
    if isinstance(directional, dict) and int(directional.get("resolved") or 0) >= 8:
        return {**directional, "scope": "league_direction"}
    if isinstance(overall, dict) and int(overall.get("resolved") or 0) > 0:
        return {**overall, "scope": "league"}
    global_stats = (profile.get("global") or {}).get(direction) or (profile.get("global") or {}).get("overall") or {}
    return {**global_stats, "scope": "global_fallback"} if global_stats else {}


def _edge_points(edge: float | None, thresholds: tuple[tuple[float, int], ...]) -> int:
    if edge is None:
        return 0
    points = 0
    for threshold, value in thresholds:
        if edge >= threshold:
            points = value
    return points


def calculate_signal_quality(match_data: dict) -> dict:
    """Return a conservative market-edge score, never a win probability."""
    direction = _normalize_direction(match_data.get("direction") or match_data.get("final_direction"))
    live = _safe_float(match_data.get("live") or match_data.get("inplay_total"))
    projection = _safe_float(
        match_data.get("projected_total")
        or match_data.get("pure_projected_total")
        or match_data.get("projected")
    )
    fair = _safe_float(match_data.get("fair_line"))
    fair_edge = None
    projection_edge = None
    if direction and live is not None and fair is not None:
        fair_edge = fair - live if direction == "ÜST" else live - fair
    if direction and live is not None and projection is not None:
        projection_edge = projection - live if direction == "ÜST" else live - projection

    fair_points = _edge_points(
        fair_edge,
        ((0, 5), (2, 15), (4, 28), (5, 36), (6, 45)),
    )
    projection_points = _edge_points(
        projection_edge,
        ((0, 4), (3, 8), (5, 13), (8, 19), (12, 25)),
    )

    league_stats = match_data.get("league_signal_stats")
    if not isinstance(league_stats, dict):
        league_stats = {
            "rate": match_data.get("league_success_rate"),
            "adjusted_rate": match_data.get("league_adjusted_rate"),
            "resolved": match_data.get("league_success_samples"),
            "scope": "legacy_input",
        }
    league_samples = int(league_stats.get("resolved") or 0)
    league_scope = str(league_stats.get("scope") or "")
    league_rate = _safe_float(league_stats.get("rate"))
    adjusted_rate = _safe_float(league_stats.get("adjusted_rate"))
    if adjusted_rate is None and league_rate is not None:
        adjusted_rate = (league_rate * league_samples / 100.0 + 5.0) / (league_samples + 10.0) * 100.0
    league_raw_points = _edge_points(
        adjusted_rate,
        ((0, 0), (45, 3), (50, 7), (55, 12), (60, 16), (65, 20)),
    )
    league_points = (
        0
        if league_scope == "global_fallback"
        else int(round(league_raw_points * min(1.0, league_samples / 30.0)))
    )
    agreement_points = 10 if (fair_edge is not None and fair_edge > 0 and projection_edge is not None and projection_edge > 0) else 0

    score = fair_points + projection_points + league_points + agreement_points
    caps: list[int] = []
    risks: list[str] = []
    clock = game_clock(
        str(match_data.get("status") or ""),
        str(match_data.get("match_name") or ""),
        str(match_data.get("tournament") or ""),
    )
    if clock.get("period") is None or clock.get("remaining_min") is None:
        caps.append(39)
        risks.append("kesin maç saati yok")
    elif int(clock.get("period") or 0) == 1:
        elapsed_in_period = float(clock.get("quarter_length") or 10) - float(clock.get("remaining_min") or 0)
        if elapsed_in_period < 4:
            caps.append(49)
            risks.append("Q1 ilk 4 dakika")
    if not direction or live is None or fair is None or projection is None:
        caps.append(39)
        risks.append("sinyal hesabı için gerekli barem/projeksiyon eksik")
    if fair_edge is not None and fair_edge < 0:
        caps.append(39)
        risks.append("adil barem sinyal yönünü desteklemiyor")
    elif fair_edge is not None and fair_edge < 4:
        caps.append(59)
        risks.append("adil barem avantajı 4 sayının altında")
    if projection_edge is not None and projection_edge < 0:
        caps.append(49)
        risks.append("ham tempo projeksiyonu sinyal yönüne ters")
    if league_samples < 10:
        caps.append(74)
        risks.append("lig örneklemi henüz küçük")
    if league_scope == "global_fallback":
        caps.append(64)
        risks.append("bu lig için karşılaştırılabilir sonuç yok")
    if adjusted_rate is not None and adjusted_rate < 50:
        caps.append(64)
        risks.append("lig geçmişi sinyali desteklemiyor")
    if not clock.get("model_validated"):
        caps.append(59)
        risks.append("maç formatı bu strateji için doğrulanmadı")
    if clock.get("period") == 4:
        caps.append(69)
        risks.append("Q4 maç sonu faul ve rotasyon riski")
    previous_directions = [
        _normalize_direction(item)
        for item in (match_data.get("previous_directions") or [])
        if _normalize_direction(item)
    ]
    if direction and any(previous != direction for previous in previous_directions):
        caps.append(59)
        risks.append("aynı maçta yön değişimi var")
    # The 65-74 band separated prospectively in the current archive, while the
    # few 75+ rows did not. Reserve four/five stars until this exact version has
    # accumulated a dedicated prospective evidence ledger.
    caps.append(74)
    risks.append("4-5 yıldız için yeni stratejide ileri tarihli kanıt henüz yok")
    if caps:
        score = min(score, min(caps))
    score = int(round(max(0, min(100, score))))
    star_count = _stars(score)
    label = _quality_label(score)

    rate_text = (
        f"%{league_rate:.1f} ({league_samples} maç)"
        if league_rate is not None and league_samples
        else "yeterli lig sonucu yok"
    )
    reason = (
        f"Adil barem avantajı {fair_edge:+.1f}, tempo projeksiyonu avantajı {projection_edge:+.1f}. "
        if fair_edge is not None and projection_edge is not None
        else "Adil barem/projeksiyon avantajı tam hesaplanamadı. "
    )
    reason += f"Karşılaştırılabilir lig geçmişi {rate_text}. Sonuç: {star_count} yıldız, {label}."
    risk_note = "; ".join(dict.fromkeys(risks)) if risks else "Belirgin ek risk yok."

    return {
        "quality_score": score,
        "quality_label": label,
        "stars": star_count,
        "score_kind": "evidence_ranking_not_probability",
        "signal_score_version": SIGNAL_SCORE_VERSION,
        "signal_score_epoch": SIGNAL_SCORE_EPOCH,
        "score_components": {
            "fair_edge": {"score": fair_points, "max": 45},
            "pace_projection": {"score": projection_points, "max": 25},
            "league_evidence": {"score": league_points, "max": 20},
            "direction_agreement": {"score": agreement_points, "max": 10},
        },
        "fair_line": round(fair, 1) if fair is not None else None,
        "fair_edge": round(fair_edge, 1) if fair_edge is not None else None,
        "projection": round(projection, 1) if projection is not None else None,
        "projection_diff": round(projection_edge, 1) if projection_edge is not None else None,
        "league_rate": round(league_rate, 1) if league_rate is not None else None,
        "league_adjusted_rate": round(adjusted_rate, 1) if adjusted_rate is not None else None,
        "league_samples": league_samples,
        "league_scope": league_scope,
        "quality_cap": min(caps) if caps else 100,
        "risk_note": risk_note,
        "reason": reason,
    }
