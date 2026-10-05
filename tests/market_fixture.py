"""Synthetic public-provider responses for integration tests (never network data)."""

import re
import time

from live_market import attach_raw_history, verify_market


def _varint(number):
    result = bytearray()
    while number > 127:
        result.append((number & 127) | 128)
        number >>= 7
    result.append(number)
    return bytes(result)


def _field(number, value):
    if isinstance(value, int):
        return _varint(number * 8) + _varint(value)
    value = value.encode() if isinstance(value, str) else value
    return _varint(number * 8 + 2) + _varint(len(value)) + value


def verified_payload(payload):
    payload = dict(payload)
    status = re.fullmatch(r"Q([1-4])\s+(\d{1,2}:\d{2})", payload.get("status", ""))
    score = re.fullmatch(r"\s*(\d+)\s*-\s*(\d+)\s*", payload.get("score", ""))
    if not status or not score:
        return payload
    now = int(time.time())
    period = int(status[1]) * 2
    slot = lambda value: ["0.86", str(value), "0.86", "0"] if value is not None else []
    source = {
        "match_id": payload["match_id"], "component_match_id": payload["match_id"],
        "market": "bs", "active_market": "bs", "bookmaker_id": 2,
        "source_status_id": period, "source_score": list(map(int, score.groups())),
        "rendered_row_count": 1, "rendered_cell_present": True,
        "rendered_live_locked": False, "rendered_live_values": [payload["inplay_total"]],
        "rendered_live": payload["inplay_total"], "rows": [{
            "bookmaker_id": 2, "opening": slot(payload.get("opening_total")),
            "prematch": slot(payload.get("prematch_total")), "live": slot(payload["inplay_total"]),
        }],
    }
    history = [{"clock": status[2], "score": f"{score[1]}-{score[2]}", "over": "0.86",
                "total": str(payload["inplay_total"]), "under": "0.86", "closed": "0",
                "status_id": period, "updated_at": now}]
    record = history[0]
    row = b"".join(_field(index, record[key]) for index, key in enumerate(
        ("clock", "score", "over", "total", "under", "closed", "status_id", "updated_at"), 1
    ))
    company = _field(1, row) + _field(2, _field(1, 2) + _field(2, "bet365"))
    raw = _field(15, _field(1, company))
    proof, reason = verify_market(source, history, payload["match_id"], payload["status"],
                                 payload["score"], now=now)
    assert not reason
    payload["market_provenance"] = attach_raw_history(proof, raw)
    return payload
