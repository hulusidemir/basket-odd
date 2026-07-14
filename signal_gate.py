from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class GatePolicy:
    """Prospective evidence policy for a genuinely playable signal."""

    policy_id: str = "trusted_70_v2"
    strategy_id: str = "calibrated_projection_q2q3"
    strategy_version: int = 2
    strategy_fingerprint: str = (
        "projection=shadow_projection_v1|fair=calibrated_fair_v1|"
        "candidate=projection_edge_6_q2q3_v2|market=paired_consensus_v1|format=4x10_v1"
    )
    evidence_epoch: str = "2026-07-13T00:00:00+00:00"
    min_data_reliability: int = 85
    min_model_support: int = 80
    min_unique_resolved: int = 100
    evidence_window: int = 200
    block_size: int = 50
    min_resolution_coverage: float = 90.0
    min_overall_wilson_95: float = 70.0
    min_block_rate: float = 70.0
    min_block_wilson_95: float = 60.0
    max_block_rate_gap: float = 10.0


DEFAULT_GATE_POLICY = GatePolicy()


def _fold(value) -> str:
    return (
        str(value or "")
        .strip()
        .lower()
        .replace("ı", "i")
        .replace("ş", "s")
        .replace("ğ", "g")
        .replace("ü", "u")
        .replace("ö", "o")
        .replace("ç", "c")
    )


def _parse_dict(value) -> dict:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_datetime(value) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def wilson_lower_95(wins: int, resolved: int) -> float | None:
    if resolved <= 0:
        return None
    wins = max(0, min(int(wins), int(resolved)))
    resolved = int(resolved)
    z = 1.96
    rate = wins / resolved
    denominator = 1 + (z * z / resolved)
    center = rate + (z * z / (2 * resolved))
    adjustment = z * math.sqrt(
        (rate * (1 - rate) + z * z / (4 * resolved)) / resolved
    )
    return round(((center - adjustment) / denominator) * 100, 2)


def _block_stats(rows: list[dict]) -> dict:
    resolved = len(rows)
    wins = sum(1 for row in rows if row.get("success"))
    return {
        "resolved": resolved,
        "wins": wins,
        "losses": resolved - wins,
        "rate": round((wins / resolved) * 100, 2) if resolved else None,
        "wilson_low_95": wilson_lower_95(wins, resolved),
    }


def empty_gate_evidence(policy: GatePolicy = DEFAULT_GATE_POLICY) -> dict:
    return {
        "policy_id": policy.policy_id,
        "strategy_id": policy.strategy_id,
        "strategy_version": policy.strategy_version,
        "strategy_fingerprint": policy.strategy_fingerprint,
        "evidence_epoch": policy.evidence_epoch,
        "eligible_unique": 0,
        "resolved_unique": 0,
        "wins": 0,
        "losses": 0,
        "rate": None,
        "wilson_low_95": None,
        "resolution_coverage": None,
        "previous_block": _block_stats([]),
        "recent_block": _block_stats([]),
    }


def build_gate_evidence(
    rows: list[dict],
    policy: GatePolicy = DEFAULT_GATE_POLICY,
    *,
    as_of: datetime | None = None,
) -> dict:
    """Build time-safe evidence from the first eligible alert of each match."""
    cutoff = as_of or datetime.now(timezone.utc)
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=timezone.utc)
    cutoff = cutoff.astimezone(timezone.utc)
    epoch = _parse_datetime(policy.evidence_epoch)

    candidates: list[dict] = []
    for row in rows or []:
        analysis = _parse_dict(row.get("ai_analysis"))
        gate = analysis.get("signal_gate")
        if not isinstance(gate, dict):
            continue
        if (
            gate.get("policy_id") != policy.policy_id
            or gate.get("strategy_id") != policy.strategy_id
            or int(gate.get("strategy_version") or 0) != policy.strategy_version
            or gate.get("strategy_fingerprint") != policy.strategy_fingerprint
            or gate.get("evidence_epoch") != policy.evidence_epoch
            or not gate.get("trial_eligible")
        ):
            continue
        evaluated_at = _parse_datetime(gate.get("evaluated_at"))
        if evaluated_at is None or evaluated_at > cutoff:
            continue
        if epoch is not None and evaluated_at < epoch:
            continue
        match_id = str(row.get("match_id") or gate.get("match_id") or "").strip()
        if not match_id:
            continue
        candidates.append(
            {
                "match_id": match_id,
                "evaluated_at": evaluated_at,
                "id": int(row.get("id") or 0),
                "result": _fold(row.get("result")),
                "result_source": str(row.get("result_source") or ""),
                "settled_at": _parse_datetime(row.get("settled_at")),
                "legacy_result_fixture": "result_source" not in row,
            }
        )

    first_by_match: dict[str, dict] = {}
    for item in sorted(candidates, key=lambda row: (row["evaluated_at"], row["id"])):
        first_by_match.setdefault(item["match_id"], item)
    unique_trials = list(first_by_match.values())[-max(1, policy.evidence_window):]

    resolved_rows = []
    for item in unique_trials:
        if item["result"] not in {"basarili", "basarisiz"}:
            continue
        automatic = (
            item["legacy_result_fixture"]
            or item["result_source"] == "automatic_final_score"
        )
        if not automatic:
            continue
        if item["settled_at"] is not None and item["settled_at"] > cutoff:
            continue
        resolved_rows.append({**item, "success": item["result"] == "basarili"})

    overall = _block_stats(resolved_rows)
    block_span = max(1, policy.block_size * 2)
    block_rows = resolved_rows[-block_span:]
    previous_rows = block_rows[: policy.block_size] if len(block_rows) >= block_span else []
    recent_rows = block_rows[policy.block_size:] if len(block_rows) >= block_span else []
    coverage = (
        round((len(resolved_rows) / len(unique_trials)) * 100, 2)
        if unique_trials
        else None
    )
    return {
        "policy_id": policy.policy_id,
        "strategy_id": policy.strategy_id,
        "strategy_version": policy.strategy_version,
        "strategy_fingerprint": policy.strategy_fingerprint,
        "evidence_epoch": policy.evidence_epoch,
        "eligible_unique": len(unique_trials),
        "resolved_unique": len(resolved_rows),
        "wins": overall["wins"],
        "losses": overall["losses"],
        "rate": overall["rate"],
        "wilson_low_95": overall["wilson_low_95"],
        "resolution_coverage": coverage,
        "previous_block": _block_stats(previous_rows),
        "recent_block": _block_stats(recent_rows),
    }


def _evidence_reason_codes(evidence: dict, policy: GatePolicy) -> list[str]:
    reasons = []
    resolved = int(evidence.get("resolved_unique") or 0)
    coverage = evidence.get("resolution_coverage")
    overall_low = evidence.get("wilson_low_95")
    previous = evidence.get("previous_block") or {}
    recent = evidence.get("recent_block") or {}

    if resolved < policy.min_unique_resolved:
        reasons.append("INSUFFICIENT_PROSPECTIVE_SAMPLE")
    if coverage is None or float(coverage) < policy.min_resolution_coverage:
        reasons.append("LOW_RESOLUTION_COVERAGE")
    if overall_low is None or float(overall_low) < policy.min_overall_wilson_95:
        reasons.append("OVERALL_WILSON_BELOW_70")
    if (
        int(previous.get("resolved") or 0) < policy.block_size
        or int(recent.get("resolved") or 0) < policy.block_size
    ):
        reasons.append("TIME_BLOCKS_INCOMPLETE")
    else:
        previous_rate = float(previous.get("rate") or 0)
        recent_rate = float(recent.get("rate") or 0)
        previous_low = float(previous.get("wilson_low_95") or 0)
        recent_low = float(recent.get("wilson_low_95") or 0)
        if min(previous_rate, recent_rate) < policy.min_block_rate:
            reasons.append("TIME_BLOCK_RATE_BELOW_70")
        if min(previous_low, recent_low) < policy.min_block_wilson_95:
            reasons.append("TIME_BLOCK_WILSON_TOO_LOW")
        if abs(previous_rate - recent_rate) > policy.max_block_rate_gap:
            reasons.append("TIME_BLOCKS_UNSTABLE")
    return reasons


def evaluate_signal_gate(
    match: dict,
    analysis: dict,
    quality: dict,
    evidence: dict | None = None,
    policy: GatePolicy = DEFAULT_GATE_POLICY,
    *,
    evaluated_at: datetime | None = None,
) -> dict:
    """Freeze a BLOCKED, SHADOW or TRUSTED decision at alert creation time."""
    now = evaluated_at or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    evidence = dict(evidence or empty_gate_evidence(policy))
    reason_codes: list[str] = []

    candidate = bool(analysis.get("candidate_eligible"))
    signal_count = int(match.get("signal_count") or 1)
    data_score = int(quality.get("data_reliability_score") or 0)
    model_score = int(quality.get("model_support_score") or quality.get("quality_score") or 0)
    if not candidate:
        reason_codes.append("CANDIDATE_RULE_NOT_MET")
    if signal_count > 1:
        reason_codes.append("BLOCKED_DUPLICATE")
    if quality.get("data_hard_fail"):
        reason_codes.append("CRITICAL_DATA_FAILURE")
    if data_score < policy.min_data_reliability:
        reason_codes.append("DATA_RELIABILITY_BELOW_THRESHOLD")
    if model_score < policy.min_model_support:
        reason_codes.append("MODEL_SUPPORT_BELOW_THRESHOLD")
    components = quality.get("components") if isinstance(quality.get("components"), dict) else {}
    if float(components.get("game_script") or 0) < 0:
        reason_codes.append("ADVERSE_GAME_SCRIPT")

    trial_eligible = not reason_codes
    if not trial_eligible:
        state = "BLOCKED"
    else:
        reason_codes.extend(_evidence_reason_codes(evidence, policy))
        state = "TRUSTED" if not reason_codes else "SHADOW"

    match_id = str(match.get("match_id") or "").strip()
    return {
        "schema_version": 1,
        "policy_id": policy.policy_id,
        "strategy_id": policy.strategy_id,
        "strategy_version": policy.strategy_version,
        "strategy_fingerprint": policy.strategy_fingerprint,
        "evidence_epoch": policy.evidence_epoch,
        "evaluated_at": now.astimezone(timezone.utc).isoformat(),
        "trial_key": f"{policy.strategy_id}:{policy.strategy_version}:{match_id}",
        "match_id": match_id,
        "trial_eligible": trial_eligible,
        "state": state,
        "telegram_allowed": state == "TRUSTED",
        "evidence": evidence,
        "reason_codes": reason_codes,
        "policy": asdict(policy),
    }
