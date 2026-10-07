"""Verify a selected bookmaker's full-game total against that same source."""

import base64
import hashlib
import math
import re
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit


BOOKMAKER_ID = 2
MARKET = "bs"

LIVE_SOURCE_JS = r"""preferredId => {
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
    // Read the actual rendered row by its component's company order, not the first row.
    const companies = component?.oddListDataArray || [];
    const boxes = component?.$el?.querySelectorAll('.oddsBoxContent') || [];
    const slot = values => Array.isArray(values) && values.length === 4
        && String(values[3]) === '0' && Number(values[1]) >= 100 && Number(values[1]) <= 400;
    const counts = new Map();
    for (const row of rows) counts.set(row.bookmaker_id, (counts.get(row.bookmaker_id) || 0) + 1);
    const candidates = rows.filter(row => Number.isInteger(row.bookmaker_id) && row.bookmaker_id > 0
        && counts.get(row.bookmaker_id) === 1 && slot(row.opening)
        && (!row.live.length || slot(row.live)))
        .sort((a, b) => Number(b.bookmaker_id === 2) - Number(a.bookmaker_id === 2)
            || a.bookmaker_id - b.bookmaker_id);
    const bookmakerId = preferredId ?? candidates[0]?.bookmaker_id ?? null;
    const rendered = companies.filter(row => Number(row.id) === bookmakerId);
    const index = companies.findIndex(row => Number(row.id) === bookmakerId);
    const cell = index >= 0 ? boxes[index]?.querySelector('.border3') : null;
    const locked = !!cell && /(?:^|[^a-z])(?:lock(?:ed|icon)?|islocked|suspend(?:ed)?|unavailable|closed)/i
        .test(`${cell.className || ''} ${cell.innerHTML || ''}`);
    const visible = node => {
        const style = getComputedStyle(node);
        return style.display !== 'none' && style.visibility !== 'hidden'
            && node.getClientRects().length > 0;
    };
    // Read complete visible tokens, not numeric leaves: <span>1<b>35</b></span>
    // is 135, not 1 or 35. Hidden alternate quotes are not evidence.
    const totals = target => {
        if (!target || !visible(target)) return [];
        const read = node => {
            if (!visible(node)) return [];
            const text = (node.innerText || '').trim();
            if ((node !== target || !node.children.length)
                    && /^(?:[ou]\s*)?\d+(?:\.\d+)?$/i.test(text))
                return [text.replace(/\s+/g, '')];
            if (node.children.length) return [...node.children].flatMap(read);
            return text.split(/\s+/).filter(Boolean);
        };
        const tokens = read(target);
        const values = tokens.filter(value => /^(?:[ou])?\d+(?:\.\d+)?$/i.test(value))
            .map(value => Number(value.replace(/^[ou]/i, '')))
            .filter(value => value >= 100 && value <= 400);
        return [...new Set(values)];
    };
    const unique = totals(cell);
    const renderedCells = Object.fromEntries([['opening', '.border1'],
        ['prematch', '.border2'], ['live', '.border3']].map(([name, selector]) =>
        [name, index >= 0 ? boxes[index]?.querySelector(selector) : null]));
    const renderedTotals = Object.fromEntries(Object.entries(renderedCells)
        .map(([name, target]) => [name, totals(target)]));
    const slotVisibility = Object.fromEntries(Object.entries(renderedCells)
        .map(([name, target]) => [name, target ? visible(target) : null]));
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
        market: 'bs', bookmaker_id: bookmakerId,
        source_version: 'aiscore_history_v2',
        candidate_ids: candidates.map(row => row.bookmaker_id),
        source_status_id: source?.statusId,
        source_score: [score(source?.homeScores), score(source?.awayScores)],
        top_text: top,
        dom_score: domScore ? `${domScore[1]} - ${domScore[2]}` : '',
        rows,
        rendered_validation_version: 2,
        rendered_market_label: (component?.$el?.querySelector('.oddsType')?.innerText || '').trim(),
        rendered_totals: renderedTotals,
        rendered_slot_visibility: slotVisibility,
        rendered_row_count: rendered.length,
        rendered_cell_present: !!cell && boxes.length === companies.length,
        rendered_live_locked: locked,
        rendered_live_values: unique,
        rendered_live: rendered.length === 1 && unique.length === 1 ? unique[0] : null,
    };
}"""


FETCH_HISTORY_JS = r"""request => {
    const expectedId = typeof request === 'string' ? request : request.match_id;
    const bookmakerId = typeof request === 'string' ? 2 : request.bookmaker_id;
    const bookmaker = typeof request === 'string' ? 'bet365' : request.bookmaker;
    const component = [...document.querySelectorAll('.oddsBox, .oddsContent')]
        .map(node => node.__vue__)
        .find(value => typeof value?.historyOdd === 'function');
    if (!component || component.match?.match?.id !== expectedId || component.activeTab !== 'bs')
        throw new Error('Unverified total market identity');
    component.allData = [];
    // The raw response is consumed by Python. The UI's rendering promise must
    // not hold it up after a response has already arrived.
    Promise.resolve(component.historyOdd(bookmakerId, bookmaker))
        .catch(() => {})
        .finally(() => { component.isShowModal = false; });
    return true;
}"""


def history_response_matches(url: str, match_id: str, bookmaker_id: int = BOOKMAKER_ID) -> bool:
    parsed = urlsplit(url)
    query = parse_qs(parsed.query)
    return (
        parsed.scheme == "https" and parsed.hostname == "api.aiscore.com"
        and parsed.path == "/v1/m/api/match/odds/detail"
        and query.get("match_id") == [match_id]
        and query.get("odds_type") == [MARKET]
        and query.get("cid") == [str(bookmaker_id)]
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


def decode_history(data: bytes, bookmaker_id: int = BOOKMAKER_ID) -> list[dict]:
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
    if metadata.get(1) != [bookmaker_id]:
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
    generic = source.get("source_version") == "aiscore_history_v2"
    bookmaker_id = source.get("bookmaker_id")
    if (type(bookmaker_id) is not int or bookmaker_id <= 0
            or (not generic and bookmaker_id != BOOKMAKER_ID)):
        return "bookmaker_unverified"
    rows = [row for row in source.get("rows", []) if row.get("bookmaker_id") == bookmaker_id]
    if len(rows) != 1:
        return "bookmaker_missing_or_ambiguous" if generic else "bet365_missing_or_ambiguous"
    selected = rows[0]
    opening, source_live = _slot(selected.get("opening")), _slot(selected.get("live"))
    if opening is None or (selected.get("live") and source_live is None):
        return "bookmaker_odds_unavailable" if generic else "bet365_odds_unavailable"
    if source.get("rendered_row_count") != 1 or source.get("rendered_cell_present") is not True:
        return "rendered_total_unavailable"
    if source.get("rendered_live_locked") is not False:
        return "rendered_total_locked"
    # Old frozen proofs retain their original checks. New captures additionally
    # reconcile visible cells. AIScore mobile hides opening/prematch columns;
    # hidden anchors are not DOM evidence, while the live column must be visible.
    if source.get("rendered_validation_version") is not None:
        version = source["rendered_validation_version"]
        if (version not in (1, 2)
                or source.get("rendered_market_label", "").strip().casefold() != "total points"):
            return "rendered_market_unverified"
        rendered_totals = source.get("rendered_totals")
        if not isinstance(rendered_totals, dict):
            return "rendered_total_unavailable"
        visibility = source.get("rendered_slot_visibility")
        if version == 2 and (
                not isinstance(visibility, dict)
                or any(type(visibility.get(name)) is not bool
                       for name in ("opening", "prematch", "live"))
                or visibility["live"] is not True):
            return "rendered_total_unavailable"
        for name in ("opening", "prematch", "live"):
            if version == 2 and visibility[name] is False:
                if rendered_totals.get(name) != []:
                    return "rendered_" + name + "_mismatch"
                continue
            value = _slot(selected.get(name))
            if rendered_totals.get(name) != ([] if value is None else [value]):
                return "rendered_" + name + "_mismatch"
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
    bookmaker_id = source["bookmaker_id"]
    generic = source.get("source_version") == "aiscore_history_v2"
    selected = next(row for row in source["rows"] if row.get("bookmaker_id") == bookmaker_id)
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
        "version": "aiscore_history_v2" if generic else "bet365_history_v1",
        "match_id": match_id, "market": MARKET,
        "bookmaker_id": bookmaker_id,
        "bookmaker": (selected.get("bookmaker") or str(bookmaker_id)) if generic else "bet365",
        "verified": True,
        "opening": opening, "prematch": _slot(selected.get("prematch")), "live": live,
        "live_source": "bookmaker_history" if generic else "bet365_history", "source_live": source_live,
        "status": status, "score": score, "provider_updated_at": newest,
        "provider_age_seconds": max(0.0, age),
        "captured_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
        "source": source, "history_latest": record,
    }
    return proof, ""


def verify_current_market(source: dict, match_id: str, status: str, score: str,
                          *, now: float) -> tuple[dict | None, str]:
    """Verify the currently rendered quote against same-company application state.

    Odds-history timestamps describe line changes; they are not a heartbeat
    for the current quote. A populated live cell does not need that history.
    """
    if source.get("source_version") != "aiscore_history_v2":
        return None, "bookmaker_unverified"
    reason = source_error(source, match_id, status, score)
    if reason:
        return None, reason
    selected = next(row for row in source["rows"] if row["bookmaker_id"] == source["bookmaker_id"])
    live = _slot(selected.get("live"))
    if live is None:
        return None, "current_live_total_missing"
    return {
        "version": "aiscore_live_v2", "match_id": match_id, "market": MARKET,
        "bookmaker_id": source["bookmaker_id"],
        "bookmaker": selected.get("bookmaker") or str(source["bookmaker_id"]),
        "verified": True, "opening": _slot(selected.get("opening")),
        "prematch": _slot(selected.get("prematch")), "live": live,
        "status": status, "score": score,
        "captured_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
        "live_source": "current_state_and_rendered_cell", "source": source,
        "provider_freshness_verified": False,
    }, ""


def attach_raw_history(proof: dict, raw: bytes) -> dict:
    return {**proof, "raw_history_sha256": hashlib.sha256(raw).hexdigest(),
            "raw_history_base64": base64.b64encode(raw).decode("ascii")}


def provenance_error(match: dict, max_age: float = 30.0) -> str:
    """Recheck frozen source identity and age at the final signal boundary."""
    proof = match.get("market_provenance")
    if (not isinstance(proof, dict) or proof.get("version") not in {"bet365_history_v1", "aiscore_history_v2", "aiscore_live_v2"}
            or proof.get("verified") is not True):
        return "market_provenance_missing"
    bookmaker_id = proof.get("bookmaker_id")
    if (type(bookmaker_id) is not int or bookmaker_id <= 0
            or (proof["version"] == "bet365_history_v1" and bookmaker_id != BOOKMAKER_ID)):
        return "bookmaker_unverified"
    for key, value in (("match_id", match.get("match_id")), ("market", MARKET),
                       ("live", match.get("inplay_total")),
                       ("opening", match.get("opening_total")), ("prematch", match.get("prematch_total")),
                       ("status", match.get("status")), ("score", match.get("score"))):
        if proof.get(key) != value:
            return "market_provenance_mismatch"
    updated = proof.get("provider_updated_at")
    if proof["version"] == "aiscore_live_v2":
        try:
            captured = datetime.fromisoformat(proof["captured_at"])
            if captured.tzinfo is None or not -5 <= datetime.now(timezone.utc).timestamp() - captured.timestamp() <= max_age:
                return "market_observation_stale"
            verified, reason = verify_current_market(
                proof["source"], match["match_id"], match["status"], match["score"], now=captured.timestamp(),
            )
            if reason:
                return reason
            return "" if verified == proof else "market_provenance_mismatch"
        except (KeyError, ValueError, TypeError):
            return "market_provenance_invalid"
    if type(updated) is not int or not -5 <= datetime.now(timezone.utc).timestamp() - updated <= max_age:
        return "provider_history_stale"
    try:
        raw = base64.b64decode(proof.get("raw_history_base64", ""), validate=True)
        if hashlib.sha256(raw).hexdigest() != proof.get("raw_history_sha256"):
            return "provider_history_evidence_mismatch"
        verified, reason = verify_market(
            proof.get("source") or {}, decode_history(raw, bookmaker_id), match["match_id"],
            match.get("status", ""), match.get("score", ""),
            now=datetime.now(timezone.utc).timestamp(), max_age=max_age,
        )
        if reason:
            return reason
        if any(verified[key] != proof.get(key) for key in (
            "version", "bookmaker_id", "opening", "prematch", "live", "provider_updated_at", "history_latest",
        )):
            return "provider_history_evidence_mismatch"
    except (ValueError, TypeError, KeyError, AttributeError):
        return "provider_history_evidence_invalid"
    return ""
