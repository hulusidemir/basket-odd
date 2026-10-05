import copy
import json
import sqlite3
import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from db import Database
from live_market import (
    LIVE_SOURCE_JS, _fields, attach_raw_history, decode_history, history_response_matches,
    provenance_error, verify_market,
)


def observation():
    now = int(time.time())
    slot = lambda line: ["0.86", str(line), "0.86", "0"]
    source = {
        "match_id": "m", "component_match_id": "m", "market": "bs", "active_market": "bs",
        "bookmaker_id": 2, "source_status_id": 4, "source_score": [30, 24], "rendered_live": 187.5,
        "rendered_row_count": 1, "rendered_cell_present": True,
        "rendered_live_locked": False, "rendered_live_values": [187.5],
        "rows": [
            {"bookmaker_id": 101, "opening": slot(182.5), "prematch": slot(182.5), "live": slot(197.5)},
            {"bookmaker_id": 2, "opening": slot(182.5), "prematch": slot(182.5), "live": slot(187.5)},
        ],
    }
    history = [{"clock": "07:12", "score": "30-24", "over": "0.86", "total": "187.5",
                "under": "0.86", "closed": "0", "status_id": 4, "updated_at": now}]
    return source, history, now


def verify(source, history, now):
    return verify_market(source, history, "m", "Q2 07:14", "30 - 24", now=now)


def test_bet365_main_total_is_selected_independent_of_row_order():
    source, history, now = observation()
    for _ in range(2):
        proof, reason = verify(source, history, now)
        assert reason == ""
        assert proof["live"] == 187.5 and proof["bookmaker_id"] == 2
        source["rows"].reverse()


@pytest.mark.parametrize("change,reason", [
    (lambda s, h: s.update(match_id="other"), "market_match_identity_mismatch"),
    (lambda s, h: s.update(component_match_id="other"), "market_match_identity_mismatch"),
    (lambda s, h: s.update(active_market="asia"), "total_market_unverified"),
    (lambda s, h: s.update(bookmaker_id=101), "bookmaker_unverified"),
    (lambda s, h: s.update(rows=s["rows"][:1]), "bet365_missing_or_ambiguous"),
    (lambda s, h: s["rows"].append(copy.deepcopy(s["rows"][1])), "bet365_missing_or_ambiguous"),
    (lambda s, h: s["rows"][1].update(live=[".86", "187.5", ".86", "1"]), "bet365_odds_unavailable"),
    (lambda s, h: s.update(rendered_live=197.5), "rendered_total_mismatch"),
    (lambda s, h: s.update(source_score=[32, 24]), "source_score_mismatch"),
    (lambda s, h: h[0].update(total="183.5"), "provider_history_total_mismatch"),
    (lambda s, h: h[0].update(closed="1"), "provider_history_locked"),
    (lambda s, h: h[0].update(updated_at=h[0]["updated_at"]-31), "provider_history_stale"),
    (lambda s, h: h[0].update(updated_at=h[0]["updated_at"]+6), "provider_history_stale"),
    (lambda s, h: h[0].update(updated_at=0), "history_timestamp_missing"),
    (lambda s, h: h[0].update(score="32-24"), "history_score_mismatch"),
    (lambda s, h: h[0].update(clock="08:00"), "history_clock_mismatch"),
    (lambda s, h: h[0].update(status_id=6), "history_period_mismatch"),
    (lambda s, h: s.update(source_status_id=6), "source_period_mismatch"),
])
def test_unverified_provider_data_is_rejected(change, reason):
    source, history, now = observation()
    change(source, history)
    assert verify(source, history, now) == (None, reason)


def test_197_5_cannot_be_sent_when_bet365_history_is_187_5():
    source, history, now = observation()
    source["rows"][1]["live"][1] = "197.5"
    source["rendered_live"] = 197.5
    source["rendered_live_values"] = [197.5]
    assert verify(source, history, now) == (None, "provider_history_total_mismatch")


def test_latest_history_is_used_even_if_newest_record_is_locked():
    source, history, now = observation()
    old = {**history[0], "updated_at": now-1}
    history[0]["closed"] = "1"
    assert verify(source, [old, history[0]], now)[1] == "provider_history_locked"


def test_missing_prematch_never_borrows_another_bookmaker():
    source, history, now = observation()
    source["rows"][1]["prematch"] = []
    assert verify(source, history, now)[0]["prematch"] is None


def test_live_total_comes_from_fresh_history_when_bet365_list_live_is_empty():
    source, history, now = observation()
    source["rows"][1]["live"] = []
    source["rendered_live"] = None
    source["rendered_live_values"] = []
    proof, reason = verify(source, history, now)
    assert not reason
    assert proof["live"] == 187.5 and proof["source_live"] is None
    assert proof["live_source"] == "bet365_history"
    # An empty list column never relaxes any source timestamp/score checks.
    history[0]["updated_at"] -= 31
    assert verify(source, history, now)[1] == "provider_history_stale"


@pytest.mark.parametrize("clock", ["Q3 07:12", "06:74", "21:00"])
def test_history_clock_must_have_valid_seconds_and_matching_period(clock):
    source, history, now = observation()
    history[0]["clock"] = clock
    assert verify(source, history, now)[1] == "history_clock_mismatch"


def test_invalid_dom_clock_cannot_be_verified_by_an_invalid_history_clock():
    source, history, now = observation()
    history[0]["clock"] = "06:74"
    assert verify_market(source, history, "m", "Q2 06:74", "30 - 24", now=now)[1] == "live_clock_or_score_unverified"


@pytest.mark.parametrize("legacy_state", [
    (3, "11:00", 1500, 23, 60, 50, 110, 197.5),
    (2, "07:14", 766, 40 - 766 / 60, 30, 24, 54, 187.5),
])
def test_legacy_snapshots_cannot_override_new_source_duration_or_chronology(tmp_path, legacy_state):
    from config import Config
    from live_signals import evaluate_live_signal
    from main import process_match
    from tests.market_fixture import verified_payload

    db = Database(str(tmp_path / "source-transition.db"))
    db.init()
    # An unproven old clock would both freeze 48 minutes and reject this
    # independently verified, lower-clock/lower-score observation.
    period, clock, elapsed, remaining, home, away, total, live = legacy_state
    db.save_snapshot_if_changed("m", period, clock, elapsed, remaining, home, away, total, 182.5, live)
    legacy = db.get_match_snapshots("m")[0]
    payload = verified_payload({"match_id": "m", "match_name": "Home - Away", "tournament": "FIBA",
        "status": "Q2 07:14", "score": "30 - 24", "opening_total": 182.5, "inplay_total": 187.5})
    notifier = type("Notifier", (), {"send_alert": AsyncMock()})()
    with patch("main.evaluate_live_signal", wraps=evaluate_live_signal) as evaluate:
        asyncio.run(process_match(payload, db, notifier, Config()))
    evaluate.assert_called_once()
    used = evaluate.call_args.args[1]
    assert len(used) == 1 and used[0]["elapsed_game_seconds"] == 766
    assert used[0]["remaining_minutes"] == pytest.approx(40 - 766 / 60)
    assert db.get_match_snapshots("m")[0] == legacy
    assert db.count_match_alerts("m") == 0
    notifier.send_alert.assert_not_awaited()


def test_history_response_url_requires_match_bookmaker_and_market():
    url = "https://api.aiscore.com/v1/m/api/match/odds/detail?match_id=m&odds_type=bs&cid=2"
    assert history_response_matches(url, "m")
    for other in [url.replace("cid=2", "cid=101"), url.replace("type=bs", "type=asia"),
                  url.replace("match_id=m", "match_id=x"), url.replace("api.aiscore.com", "example.com")]:
        assert not history_response_matches(other, "m")


def vi(number):
    output = b""
    while number > 127:
        output += bytes([(number & 127) | 128]); number >>= 7
    return output + bytes([number])


def field(number, value):
    if isinstance(value, str): value = value.encode()
    return vi(number*8) + vi(value) if isinstance(value, int) else vi(number*8+2) + vi(len(value)) + value


def raw_history(bookmaker=2):
    row = b"".join(field(k, v) for k, v in [(1, "07:12"), (2, "30-24"), (3, "0.86"),
                   (4, "187.5"), (5, "0.86"), (6, "0"), (7, 4), (8, int(time.time()))])
    company = field(1, row) + field(2, field(1, bookmaker) + field(2, "bet365"))
    return field(15, field(1, company))


def test_real_wire_schema_decodes_and_rejects_other_bookmaker_and_corrupt_bodies():
    assert decode_history(raw_history())[0]["total"] == "187.5"
    with pytest.raises(ValueError, match="bookmaker_mismatch"):
        decode_history(raw_history(101))
    for body in [b"", b"<html>challenge</html>", raw_history()[:-1], field(1, 403), b"\x7a\xff"]:
        with pytest.raises(ValueError): decode_history(body)


@pytest.mark.parametrize("duplicate", [(4, "197.5"), (7, 6), (8, 1)])
def test_history_wire_rejects_conflicting_totals_periods_and_timestamps(duplicate):
    envelope = _fields(raw_history())
    company = _fields(_fields(envelope[15][0])[1][0])
    row = company[1][0] + field(*duplicate)
    body = field(15, field(1, field(1, row) + field(2, company[2][0])))
    with pytest.raises(ValueError, match="ambiguous_history_field"):
        decode_history(body)


def test_provenance_persists_raw_response_without_rewriting_legacy_rows(tmp_path):
    db = Database(str(tmp_path/"migration.db")); db.init()
    old = db.save_alert("old", "A - B", 182.5, 197.5, "ALT", 15)
    with sqlite3.connect(db.db_path) as conn:
        conn.execute("ALTER TABLE alerts DROP COLUMN market_provenance_json")
        conn.execute("ALTER TABLE match_live_snapshots DROP COLUMN market_provenance_json")
    db.init(); db.init()
    assert db.get_alert(old)["market_provenance_json"] is None
    source, history, now = observation(); proof = attach_raw_history(verify(source, history, now)[0], raw_history())
    new = db.save_alert("m", "A - B", 182.5, 187.5, "ALT", 5, market_provenance=proof)
    assert json.loads(db.get_alert(new)["market_provenance_json"]) == proof
    assert db.get_alert(old)["live"] == 197.5
    db.save_snapshot_if_changed("m", 2, "07:14", 766, 27.2, 30, 24, 54, 182.5, 187.5, market_provenance=proof)
    snapshot = json.loads(db.get_match_snapshots("m")[0]["market_provenance_json"])
    assert snapshot["raw_history_sha256"] == proof["raw_history_sha256"]
    assert "raw_history_base64" not in snapshot


def test_signal_boundary_rejects_missing_changed_and_expired_provenance():
    source, history, now = observation(); proof = attach_raw_history(verify(source, history, now)[0], raw_history())
    match = {"market_provenance": proof, "match_id": "m", "opening_total": 182.5,
             "prematch_total": 182.5, "inplay_total": 187.5, "status": "Q2 07:14", "score": "30 - 24"}
    assert provenance_error(match) == ""
    assert provenance_error({**match, "inplay_total": 197.5}) == "market_provenance_mismatch"
    assert provenance_error({**match, "market_provenance": None}) == "market_provenance_missing"
    proof["provider_updated_at"] -= 31
    assert provenance_error(match) == "provider_history_stale"


@pytest.mark.parametrize("corruption", ["missing", "line", "raw", "expired"])
def test_invalid_source_never_enters_db_or_telegram(tmp_path, corruption):
    from config import Config
    from main import process_match
    from tests.market_fixture import verified_payload
    payload = verified_payload({"match_id": "m", "match_name": "Home - Away", "tournament": "FIBA",
        "status": "Q2 07:14", "score": "30 - 24", "opening_total": 182.5, "inplay_total": 187.5})
    if corruption == "missing": payload.pop("market_provenance")
    if corruption == "line": payload["inplay_total"] = 197.5
    if corruption == "raw": payload["market_provenance"]["raw_history_base64"] = "ZmFrZQ=="
    if corruption == "expired": payload["market_provenance"]["provider_updated_at"] -= 31
    db = Database(str(tmp_path / "reject.db")); db.init()
    notifier = type("Notifier", (), {"send_alert": AsyncMock()})()
    with patch("main.evaluate_live_signal") as evaluate:
        asyncio.run(process_match(payload, db, notifier, Config()))
    evaluate.assert_not_called()
    assert db.get_match_snapshots("m") == [] and db.count_match_alerts("m") == 0
    notifier.send_alert.assert_not_awaited()


@pytest.mark.parametrize("quality", [0, 39, 69, 70])
def test_quality_is_informational_and_raw_evidence_round_trips(tmp_path, quality):
    from config import Config
    from main import process_match
    from live_signals import SignalDecision
    from tests.market_fixture import verified_payload
    payload = verified_payload({"match_id": "m", "match_name": "Home - Away", "tournament": "FIBA",
        "status": "Q2 07:14", "score": "30 - 24", "opening_total": 182.5, "inplay_total": 197.5})
    candidate = SignalDecision("opening", 182.5, 15, 12.8, "ALT", 2, sustainable_projection_center=165.7)
    db = Database(str(tmp_path / "quality.db")); db.init()
    notifier = type("Notifier", (), {"send_alert": AsyncMock(return_value={"recipient": 1})})()
    config = Config(); config.MIN_SIGNAL_QUALITY = 70
    with patch("main.evaluate_live_signal", return_value=candidate), patch(
        "main.score_signal_quality", return_value=(quality, "YÜKSEK", {}),
    ):
        asyncio.run(process_match(payload, db, notifier, config))
    assert db.count_match_alerts("m") == 1
    assert notifier.send_alert.await_count == 1
    stored = db.get_alert(1)
    assert stored["telegram_status"] == "sent"
    assert stored["quality_score"] == quality
    proof = json.loads(stored["market_provenance_json"])
    assert proof == payload["market_provenance"]
    assert proof["history_latest"]["total"] == "197.5"


def test_real_engine_publishes_exact_187_5_and_rejects_197_5_substitution(tmp_path):
    from config import Config
    from main import process_match
    from tests.market_fixture import verified_payload

    db = Database(str(tmp_path / "real-engine.db")); db.init()
    notifier = type("Notifier", (), {"send_alert": AsyncMock(return_value={"recipient": 1})})()
    config = Config()
    config.MIN_SIGNAL_QUALITY = 70  # Retired setting must not silence the actual engine.
    base = {"match_id": "m", "match_name": "Home - Away", "tournament": "FIBA",
            "opening_total": 200, "prematch_total": 200}
    first = verified_payload(dict(base, status="Q2 08:00", score="15 - 15", inplay_total=190))
    asyncio.run(process_match(first, db, notifier, config))
    assert db.count_match_alerts("m") == 0  # No usable tempo history yet.
    second = verified_payload(dict(base, status="Q2 05:00", score="20 - 20", inplay_total=187.5))
    corrupted = dict(second, inplay_total=197.5)
    asyncio.run(process_match(corrupted, db, notifier, config))
    assert db.count_match_alerts("m") == 0
    notifier.send_alert.assert_not_awaited()
    asyncio.run(process_match(second, db, notifier, config))
    stored = db.get_alert(1)
    assert stored["direction"] == "ALT" and stored["live"] == 187.5
    assert stored["quality_score"] < 70
    assert stored["telegram_status"] == "sent"
    assert notifier.send_alert.await_args.args[3] == 187.5
    context = json.loads(stored["prediction_context_json"])
    assert context["decision"]["edge_points"] >= config.MIN_EDGE_POINTS
    proof = json.loads(stored["market_provenance_json"])
    assert proof["history_latest"]["total"] == "187.5"
