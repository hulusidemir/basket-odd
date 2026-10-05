"""Verify bet365's full-game total against its source history, never row order."""

import base64
import hashlib
import math
import re
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit


BOOKMAKER_ID = 2
MARKET = "bs"

LIVE_SOURCE_JS = r"""() => {
    const state = window.$nuxt?.$store?.state;
    const source = state?.basketball?.basketballDetailMatchData?.match;
    const component = [...document.querySelectorAll('.oddsBox, .oddsContent')]
        .map(node => node.__vue__)
        .find(value => typeof value?.historyOdd === 'function');
    const data = component?.copyOddsListData;
    const companyNames = new Map((data?.companies || []).map(c => [Number(c.id), c.name]));
    const rows = (data?.bs || []).map(row => ({
        bookmaker_id: Number(row.company?.id),
        bookmaker: companyNames.get(Number(row.company?.id)) || '',
        opening: [...(row.f?.odd || [])],
        prematch: [...(row.l?.odd || [])],
        live: [...(row.s?.odd || [])],
    }));
    const rendered = component?.oddListDataArray?.filter(row => Number(row.id) === 2) || [];
    // Read the actual rendered row by its component's company order, not the first row.
    const companies = component?.oddListDataArray || [];
    const boxes = component?.$el?.querySelectorAll('.oddsBoxContent') || [];
    const index = companies.findIndex(row => Number(row.id) === 2);
    const cell = index >= 0 ? boxes[index]?.querySelector('.border3') : null;
    const locked = !!cell && /(?:^|[^a-z])(?:lock(?:ed|icon)?|islocked|suspend(?:ed)?|unavailable|closed)/i
        .test(`${cell.className || ''} ${cell.innerHTML || ''}`);
    const values = cell ? [cell, ...cell.querySelectorAll('*')]
        .filter(node => !node.children.length)
        .map(node => (node.innerText || '').trim())
        .filter(value => /^(?:[ou]\s*)?\d+(?:\.\d+)?$/i.test(value))
        .map(value => Number(value.replace(/^[ou]\s*/i, '')))
        .filter(value => value >= 100 && value <= 400) : [];
    const unique = [...new Set(values)];
    const score = values => Array.isArray(values) && values.length === 5
        && values.every(v => Number.isInteger(v) && v >= 0 && v <= 200)
        ? values.reduce((a, b) => a + b, 0) : null;
    const top = (document.querySelector('.topBox')?.innerText || '').replace(/\s+/g, ' ').trim();
    const scores = [...top.matchAll(/\b(\d{1,3})\s*[-–]\s*(\d{1,3})\b/g)];
    const domScore = scores.length ? scores[scores.length - 1] : null;
    return {
        match_id: source?.id || '',
        component_match_id: component?.match?.match?.id || '',
        active_market: component?.activeTab || '',
        market: 'bs', bookmaker_id: 2,
        source_status_id: source?.statusId,
        source_score: [score(source?.homeScores), score(source?.awayScores)],
        top_text: top,
        dom_score: domScore ? `${domScore[1]} - ${domScore[2]}` : '',
        rows,
        rendered_row_count: rendered.length,
        rendered_cell_present: !!cell && boxes.length === companies.length,
        rendered_live_locked: locked,
        rendered_live_values: unique,
        rendered_live: rendered.length === 1 && unique.length === 1 ? unique[0] : null,
    };
}"""


FETCH_HISTORY_JS = r"""expectedId => {
    const component = [...document.querySelectorAll('.oddsBox, .oddsContent')]
        .map(node => node.__vue__)
        .find(value => typeof value?.historyOdd === 'function');
    if (!component || component.match?.match?.id !== expectedId || component.activeTab !== 'bs')
        throw new Error('Unverified total market identity');
    component.allData = [];
    // The raw response is consumed by Python. The UI's rendering promise must
    // not hold it up after a response has already arrived.
    Promise.resolve(component.historyOdd(2, 'bet365'))
        .catch(() => {})
        .finally(() => { component.isShowModal = false; });
    return true;
}"""


def history_response_matches(url: str, match_id: str) -> bool:
    parsed = urlsplit(url)
    query = parse_qs(parsed.query)
    return (
        parsed.scheme == "https" and parsed.hostname == "api.aiscore.com"
        and parsed.path == "/v1/m/api/match/odds/detail"
        and query.get("match_id") == [match_id]
        and query.get("odds_type") == [MARKET]
        and query.get("cid") == [str(BOOKMAKER_ID)]
    )


def _varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    for shift in range(0, 70, 7):
        if offset >= len(data):
            raise ValueError("truncated_provider_response")
        byte = data[offset]
        offset += 1
        value |= (byte & 127) << shift
        if byte < 128:
            return value, offset
    raise ValueError("invalid_provider_response")


def _fields(data: bytes) -> dict[int, list]:
    if not isinstance(data, bytes):
        raise ValueError("invalid_provider_response")
    result = {}
    offset = 0
    while offset < len(data):
        tag, offset = _varint(data, offset)
        number, wire = tag >> 3, tag & 7
        if not number:
            raise ValueError("invalid_provider_response")
        if wire == 0:
            value, offset = _varint(data, offset)
        else:
            if wire == 2:
                size, offset = _varint(data, offset)
            elif wire in (1, 5):
                size = 8 if wire == 1 else 4
            else:
                raise ValueError("invalid_provider_response")
            end = offset + size
            if end > len(data):
                raise ValueError("truncated_provider_response")
            value, offset = data[offset:end], end
        result.setdefault(number, []).append(value)
    return result


def _single_field(fields: dict, number: int, default):
    values = fields.get(number, [default])
    if len(values) != 1:
        raise ValueError("ambiguous_history_field")
    return values[0]


def decode_history(data: bytes) -> list[dict]:
    """Decode AiScore Response -> MatchOddsDetail using its published client schema."""
    if not data or len(data) > 512_000:
        raise ValueError("invalid_provider_response_size")
    envelope = _fields(data)
    if envelope.get(1, [0]) != [0] or len(envelope.get(15, [])) != 1:
        raise ValueError("provider_response_error")
    payload = _fields(envelope[15][0])
    companies = payload.get(1, [])
    if len(companies) != 1:
        raise ValueError("ambiguous_history_bookmaker")
    company = _fields(companies[0])
    metadata = _fields(_single_field(company, 2, b""))
    if metadata.get(1) != [BOOKMAKER_ID]:
        raise ValueError("history_bookmaker_mismatch")
    rows = []
    for raw in company.get(1, []):
        fields = _fields(raw)
        row = {}
        for number, name in ((1, "clock"), (2, "score"), (3, "over"),
                             (4, "total"), (5, "under"), (6, "closed")):
            value = _single_field(fields, number, b"")
            if not isinstance(value, bytes):
                raise ValueError("invalid_history_field")
            row[name] = value.decode("utf-8")
        row["status_id"] = _single_field(fields, 7, 0)
        row["updated_at"] = _single_field(fields, 8, 0)
        rows.append(row)
    return rows


def _total(value) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and 100 <= number <= 400 else None


def _slot(values) -> float | None:
    if not isinstance(values, list) or len(values) != 4 or str(values[3]) != "0":
        return None
    return _total(values[1])


def source_error(source: dict, match_id: str, status: str, score: str) -> str:
    """Reject unavailable/invalid source rows before spending time on history."""
    if source.get("match_id") != match_id or source.get("component_match_id") != match_id:
        return "market_match_identity_mismatch"
    if source.get("market") != MARKET or source.get("active_market") != MARKET:
        return "total_market_unverified"
    if source.get("bookmaker_id") != BOOKMAKER_ID:
        return "bookmaker_unverified"
    rows = [row for row in source.get("rows", []) if row.get("bookmaker_id") == BOOKMAKER_ID]
    if len(rows) != 1:
        return "bet365_missing_or_ambiguous"
    selected = rows[0]
    opening, source_live = _slot(selected.get("opening")), _slot(selected.get("live"))
    if opening is None or (selected.get("live") and source_live is None):
        return "bet365_odds_unavailable"
    if source.get("rendered_row_count") != 1 or source.get("rendered_cell_present") is not True:
        return "rendered_total_unavailable"
    if source.get("rendered_live_locked") is not False:
        return "rendered_total_locked"
    rendered_values = source.get("rendered_live_values")
    if not isinstance(rendered_values, list) or len(rendered_values) > 1:
        return "rendered_total_ambiguous"
    if rendered_values != ([] if source_live is None else [source_live]):
        return "rendered_total_mismatch"
    if source.get("rendered_live") != source_live:
        return "rendered_total_mismatch"
    match = re.fullmatch(r"Q([1-4])\s+(\d{1,2}):([0-5]\d)", status)
    scores = re.fullmatch(r"\s*(\d{1,3})\s*-\s*(\d{1,3})\s*", score)
    if not match or int(match[2]) * 60 + int(match[3]) > 20 * 60 or not scores:
        return "live_clock_or_score_unverified"
    expected_score = [int(value) for value in scores.groups()]
    if source.get("source_score") != expected_score:
        return "source_score_mismatch"
    if source.get("source_status_id") != int(match[1]) * 2:
        return "source_period_mismatch"
    return ""


def verify_market(source: dict, history: list[dict], match_id: str, status: str,
                  score: str, *, now: float, max_age: float = 30.0) -> tuple[dict | None, str]:
    reason = source_error(source, match_id, status, score)
    if reason:
        return None, reason
    selected = next(row for row in source["rows"] if row.get("bookmaker_id") == BOOKMAKER_ID)
    opening, source_live = _slot(selected.get("opening")), _slot(selected.get("live"))
    match = re.fullmatch(r"Q([1-4])\s+(\d{1,2}):([0-5]\d)", status)
    expected_score = list(map(int, re.fullmatch(r"\s*(\d{1,3})\s*-\s*(\d{1,3})\s*", score).groups()))
    # Pick by update timestamp, never by line magnitude or an older unlocked record.
    if not history or any(type(row.get("updated_at")) is not int or row["updated_at"] <= 0 for row in history):
        return None, "history_timestamp_missing"
    newest = max(row["updated_at"] for row in history)
    latest = [row for row in history if row["updated_at"] == newest]
    if any(row != latest[0] for row in latest):
        return None, "ambiguous_latest_history"
    record = latest[0]
    age = now - newest
    if age < -5 or age > max_age:
        return None, "provider_history_stale"
    if record["closed"] not in ("", "0"):
        return None, "provider_history_locked"
    live = _total(record["total"])
    if live is None:
        return None, "provider_history_total_invalid"
    if source_live is not None and live != source_live:
        return None, "provider_history_total_mismatch"
    period, minutes, seconds = map(int, match.groups())
    if record["status_id"] != {1: 2, 2: 4, 3: 6, 4: 8}[period]:
        return None, "history_period_mismatch"
    history_score = re.fullmatch(r"\s*(\d+)\s*-\s*(\d+)\s*", record["score"])
    history_clock = re.fullmatch(r"(?:Q([1-4])\s+)?(\d{1,2}):([0-5]\d)", record["clock"])
    if not history_score or list(map(int, history_score.groups())) != expected_score:
        return None, "history_score_mismatch"
    if (not history_clock
            or (history_clock[1] is not None and int(history_clock[1]) != period)
            or int(history_clock[2]) * 60 + int(history_clock[3]) > 20 * 60
            or abs(int(history_clock[2]) * 60 + int(history_clock[3]) - minutes * 60 - seconds) > 30):
        return None, "history_clock_mismatch"
    proof = {
        "version": "bet365_history_v1", "match_id": match_id, "market": MARKET,
        "bookmaker_id": BOOKMAKER_ID, "bookmaker": "bet365", "verified": True,
        "opening": opening, "prematch": _slot(selected.get("prematch")), "live": live,
        "live_source": "bet365_history", "source_live": source_live,
        "status": status, "score": score, "provider_updated_at": newest,
        "provider_age_seconds": max(0.0, age),
        "captured_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
        "source": source, "history_latest": record,
    }
    return proof, ""


def attach_raw_history(proof: dict, raw: bytes) -> dict:
    return {**proof, "raw_history_sha256": hashlib.sha256(raw).hexdigest(),
            "raw_history_base64": base64.b64encode(raw).decode("ascii")}


def provenance_error(match: dict, max_age: float = 30.0) -> str:
    """Recheck frozen source identity and age at the final signal boundary."""
    proof = match.get("market_provenance")
    if not isinstance(proof, dict) or proof.get("version") != "bet365_history_v1" or proof.get("verified") is not True:
        return "market_provenance_missing"
    for key, value in (("match_id", match.get("match_id")), ("market", MARKET),
                       ("bookmaker_id", BOOKMAKER_ID), ("live", match.get("inplay_total")),
                       ("opening", match.get("opening_total")), ("prematch", match.get("prematch_total")),
                       ("status", match.get("status")), ("score", match.get("score"))):
        if proof.get(key) != value:
            return "market_provenance_mismatch"
    updated = proof.get("provider_updated_at")
    if type(updated) is not int or not -5 <= datetime.now(timezone.utc).timestamp() - updated <= max_age:
        return "provider_history_stale"
    try:
        raw = base64.b64decode(proof.get("raw_history_base64", ""), validate=True)
        if hashlib.sha256(raw).hexdigest() != proof.get("raw_history_sha256"):
            return "provider_history_evidence_mismatch"
        verified, reason = verify_market(
            proof.get("source") or {}, decode_history(raw), match["match_id"],
            match.get("status", ""), match.get("score", ""),
            now=datetime.now(timezone.utc).timestamp(), max_age=max_age,
        )
        if reason:
            return reason
        if any(verified[key] != proof.get(key) for key in (
            "opening", "prematch", "live", "provider_updated_at", "history_latest",
        )):
            return "provider_history_evidence_mismatch"
    except (ValueError, TypeError, KeyError, AttributeError):
        return "provider_history_evidence_invalid"
    return ""
