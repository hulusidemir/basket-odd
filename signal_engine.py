from dataclasses import asdict, dataclass

from signal_features import SignalFeatures


@dataclass(frozen=True)
class SignalDecision:
    action: str
    direction: str | None
    grade: str
    confidence: float
    scenario: str
    reason: str
    risk_note: str
    should_alert: bool

    def to_dict(self) -> dict:
        return asdict(self)


def _opposite(direction: str) -> str:
    return "ÜST" if direction == "ALT" else "ALT"


def _action_for(direction: str, scenario: str) -> str:
    if scenario == "contrarian":
        return "CONTRARIAN_OVER" if direction == "ÜST" else "CONTRARIAN_UNDER"
    return "OVER" if direction == "ÜST" else "UNDER"


def _grade(confidence: float, support_count: int, against_count: int) -> str:
    if confidence >= 78 and support_count >= 3 and against_count == 0:
        return "A"
    if confidence >= 68 and support_count >= 2 and against_count <= 1:
        return "B"
    if confidence >= 58:
        return "C"
    return "D"


def _projection_supports(direction: str, features: SignalFeatures, min_edge: float) -> bool:
    edge = features.projection_gap_vs_live_line
    if edge is None:
        return False
    if direction == "ÜST":
        return edge >= min_edge
    return edge <= -min_edge


def _projection_against(direction: str, features: SignalFeatures, min_edge: float) -> bool:
    edge = features.projection_gap_vs_live_line
    if edge is None:
        return False
    if direction == "ÜST":
        return edge <= -min_edge
    return edge >= min_edge


def _recent_pace_supports(direction: str, features: SignalFeatures, min_sample_minutes: float) -> bool:
    if features.recent_sample_minutes is None or features.recent_sample_minutes < min_sample_minutes:
        return False
    if direction == "ÜST":
        return features.recent_pace_trend == "heating"
    return features.recent_pace_trend == "cooling"


def _recent_pace_against(direction: str, features: SignalFeatures, min_sample_minutes: float) -> bool:
    if features.recent_sample_minutes is None or features.recent_sample_minutes < min_sample_minutes:
        return False
    if direction == "ÜST":
        return features.recent_pace_trend == "cooling"
    return features.recent_pace_trend == "heating"


def _line_momentum_supports(direction: str, features: SignalFeatures, min_sample_minutes: float) -> bool:
    velocity = features.live_line_velocity_per_minute
    if (
        velocity is None
        or features.recent_sample_minutes is None
        or features.recent_sample_minutes < min_sample_minutes
        or abs(velocity) < 0.8
    ):
        return False
    if direction == "ÜST":
        return velocity > 0
    return velocity < 0


def _line_momentum_against(direction: str, features: SignalFeatures, min_sample_minutes: float) -> bool:
    velocity = features.live_line_velocity_per_minute
    if (
        velocity is None
        or features.recent_sample_minutes is None
        or features.recent_sample_minutes < min_sample_minutes
        or abs(velocity) < 0.8
    ):
        return False
    if direction == "ÜST":
        return velocity < 0
    return velocity > 0


def _source_names(items: list[dict]) -> set[str]:
    return {str(item.get("name") or "").strip().lower() for item in items}


def _evaluate_direction(
    direction: str,
    features: SignalFeatures,
    quality: dict,
    *,
    min_projection_edge: float,
    min_recent_sample_minutes: float,
) -> dict:
    support_count = int(quality.get("support_count") or 0)
    against_count = int(quality.get("against_count") or 0)
    neutral_count = int(quality.get("neutral_count") or 0)
    supporting_names = _source_names(quality.get("supporting_signals") or [])
    opposing_names = _source_names(quality.get("opposing_signals") or [])

    projection_support = _projection_supports(direction, features, min_projection_edge)
    projection_against = _projection_against(direction, features, min_projection_edge)
    recent_pace_support = _recent_pace_supports(direction, features, min_recent_sample_minutes)
    recent_pace_against = _recent_pace_against(direction, features, min_recent_sample_minutes)
    line_momentum_support = _line_momentum_supports(direction, features, min_recent_sample_minutes)
    line_momentum_against = _line_momentum_against(direction, features, min_recent_sample_minutes)
    has_history_support = bool({"takım profili", "h2h geçmiş"} & supporting_names)
    has_history_against = bool({"takım profili", "h2h geçmiş"} & opposing_names)
    script_support = "maç script" in supporting_names
    script_against = "maç script" in opposing_names

    confidence = 38.0
    confidence += support_count * 13.0
    confidence -= against_count * 17.0
    confidence -= neutral_count * 2.0
    if projection_support:
        confidence += 12.0
    if projection_against:
        confidence -= 18.0
    if recent_pace_support:
        confidence += 10.0
    if recent_pace_against:
        confidence -= 12.0
    if line_momentum_support:
        confidence += 5.0
    if line_momentum_against:
        confidence -= 7.0
    if has_history_support:
        confidence += 8.0
    if has_history_against:
        confidence -= 10.0
    if script_support:
        confidence += 6.0
    if script_against:
        confidence -= 8.0

    if direction == "ÜST":
        confidence -= features.blowout_risk * 8.0
        confidence += features.foul_game_risk * 6.0
    else:
        confidence += features.blowout_risk * 5.0
        confidence -= features.foul_game_risk * 8.0

    if features.elapsed_minutes is not None and features.elapsed_minutes < 8:
        confidence -= 8.0
    confidence -= features.volatility_score * 6.0
    confidence = round(max(0.0, min(95.0, confidence)), 1)

    return {
        "direction": direction,
        "confidence": confidence,
        "support_count": support_count,
        "against_count": against_count,
        "projection_support": projection_support,
        "projection_against": projection_against,
        "recent_pace_support": recent_pace_support,
        "recent_pace_against": recent_pace_against,
        "line_momentum_support": line_momentum_support,
        "line_momentum_against": line_momentum_against,
        "effective_support_count": support_count + (1 if recent_pace_support else 0),
        "has_history_support": has_history_support,
        "has_history_against": has_history_against,
        "script_support": script_support,
        "script_against": script_against,
    }


def _risk_note(direction: str, features: SignalFeatures, evaluated: dict) -> str:
    risks: list[str] = []
    if evaluated.get("projection_against"):
        risks.append("projeksiyon ters yönde")
    if evaluated.get("recent_pace_against"):
        risks.append("son pencere temposu ters yönde")
    if evaluated.get("line_momentum_against"):
        risks.append("canlı barem momentumu ters yönde")
    if evaluated.get("has_history_against"):
        risks.append("takım/H2H geçmişi karşı")
    if features.blowout_risk >= 0.9 and direction == "ÜST":
        risks.append("blowout ÜST temposunu bozabilir")
    if features.foul_game_risk >= 0.55 and direction == "ALT":
        risks.append("yakın maçta faul oyunu ALT riskini artırır")
    if features.volatility_score >= 0.55:
        risks.append("tempo oynaklığı yüksek")
    return "; ".join(risks) if risks else "belirgin ana risk yok"


def _reason(direction: str, scenario: str, features: SignalFeatures, evaluated: dict) -> str:
    side = "ÜST" if direction == "ÜST" else "ALT"
    movement = "yukarı" if features.market_move_direction == "UP" else "aşağı" if features.market_move_direction == "DOWN" else "yatay"
    projection = features.projected_final_total_naive
    projection_part = (
        f"projeksiyon {projection:.1f}, canlı bareme göre {features.projection_gap_vs_live_line:+.1f}"
        if projection is not None and features.projection_gap_vs_live_line is not None
        else "projeksiyon hesaplanamadı"
    )
    if features.recent_points_per_minute is not None:
        recent_part = (
            f"son pencere {features.recent_points_delta} sayı/{features.recent_sample_minutes:.1f} dk "
            f"({features.recent_points_per_minute:.2f}/dk, {features.recent_pace_trend})"
        )
    else:
        recent_part = "son pencere temposu yok"
    source_part = (
        f"{evaluated['support_count']} kaynak destekliyor, {evaluated['against_count']} kaynak karşı; "
        f"{recent_part}"
    )
    scenario_part = "market hareketiyle aynı yön" if scenario == "continuation" else "market hareketine karşı ama kaynak destekli"
    return f"{side}: barem {movement} hareket etti; {projection_part}; {source_part}; {scenario_part}."


def decide_signal(
    features: SignalFeatures,
    quality_by_direction: dict[str, dict],
    config,
) -> SignalDecision:
    min_edge = float(getattr(config, "MINIMUM_PROJECTION_EDGE", 4.0))
    min_recent_sample = float(getattr(config, "MIN_RECENT_PACE_SAMPLE_MINUTES", 1.0))
    min_conf = float(getattr(config, "MINIMUM_CONFIDENCE_FOR_SIGNAL", 62.0))
    min_support = int(getattr(config, "MINIMUM_SUPPORTING_SOURCES", 2))
    continuation_threshold = float(getattr(config, "CONTINUATION_CONFIDENCE_THRESHOLD", 58.0))
    contrarian_threshold = float(getattr(config, "CONTRARIAN_CONFIDENCE_THRESHOLD", 68.0))

    if not features.continuation_direction or features.abs_line_delta_vs_reference <= 0:
        return SignalDecision(
            action="PASS",
            direction=None,
            grade="D",
            confidence=0.0,
            scenario="pass",
            reason="Barem hareketi net değil.",
            risk_note="referans-canlı farkı sinyal için yeterli değil",
            should_alert=False,
        )

    evaluated = {
        direction: _evaluate_direction(
            direction,
            features,
            quality_by_direction.get(direction) or {},
            min_projection_edge=min_edge,
            min_recent_sample_minutes=min_recent_sample,
        )
        for direction in ("ALT", "ÜST")
    }

    continuation_direction = features.continuation_direction
    contrarian_direction = features.contrarian_direction or _opposite(continuation_direction)
    continuation = evaluated[continuation_direction]
    contrarian = evaluated[contrarian_direction]

    candidates: list[tuple[str, dict, float]] = []
    if (
        continuation["confidence"] >= max(min_conf, continuation_threshold)
        and continuation["effective_support_count"] >= min_support
        and continuation["against_count"] <= 1
        and not continuation["projection_against"]
        and not continuation["recent_pace_against"]
    ):
        candidates.append(("continuation", continuation, continuation["confidence"] + 3.0))

    # Ters işlem daha seçici: mutlaka güçlü projeksiyon veya geçmiş desteği ister.
    contrarian_has_real_edge = contrarian["projection_support"] or (
        contrarian["has_history_support"] and contrarian["effective_support_count"] >= min_support + 1
    )
    if (
        contrarian["confidence"] >= max(min_conf, contrarian_threshold)
        and contrarian["effective_support_count"] >= min_support
        and contrarian["against_count"] <= 1
        and contrarian_has_real_edge
    ):
        candidates.append(("contrarian", contrarian, contrarian["confidence"]))

    if not candidates:
        best = max(evaluated.values(), key=lambda item: item["confidence"])
        action = "LOW_CONFIDENCE_PASS" if best["confidence"] < min_conf else "PASS"
        return SignalDecision(
            action=action,
            direction=None,
            grade="D",
            confidence=round(best["confidence"], 1),
            scenario="pass",
            reason=(
                "Bağımsız kaynaklar bahis için yeterli değil: "
                f"en iyi yön {best['direction']} {best['confidence']:.1f} güven, "
                f"{best['effective_support_count']} destek, {best['against_count']} karşı."
            ),
            risk_note="PASS: çizgi hareketine kör şekilde ters işlem açılmadı",
            should_alert=False,
        )

    scenario, chosen, _ = max(candidates, key=lambda item: item[2])
    direction = chosen["direction"]
    confidence = chosen["confidence"]
    grade = _grade(confidence, chosen["effective_support_count"], chosen["against_count"])
    return SignalDecision(
        action=_action_for(direction, scenario),
        direction=direction,
        grade=grade,
        confidence=confidence,
        scenario=scenario,
        reason=_reason(direction, scenario, features, chosen),
        risk_note=_risk_note(direction, features, chosen),
        should_alert=True,
    )
