import asyncio
import csv
import io
import json
from unittest.mock import AsyncMock, patch

import pytest

from config import Config
from db import Database
from live_signals import SignalDecision, evaluate_live_signal
from main import process_match
from signal_quality import quality_label, score_signal_quality


def match(**values):
    payload = {
        "match_id": "m", "match_name": "Home - Away", "tournament": "FIBA",
        "status": "Q2 05:00", "score": "45 - 45", "opening_total": 160,
        "prematch_total": 160, "inplay_total": 190, "url": "",
        "quarter_scores": {},
    }
    payload.update(values)
    return payload


def decision(payload, paces):
    with patch("live_signals.get_future_paces", return_value=paces):
        return evaluate_live_signal(payload, [], Config())


def test_under_negative_move_keeps_candidate_with_quality_pas():
    payload = match(inplay_total=150, score="25 - 25")
    candidate = decision(payload, [2.5, 2.8])
    score, label, factors = score_signal_quality(payload, candidate, Config())
    assert candidate.direction == "ALT"
    assert candidate.skip_reason == ""
    assert score < 40 and label == "PAS"
    assert factors["market_move"] <= -45

    worse_payload = match(inplay_total=148, score="25 - 25")
    worse = decision(worse_payload, [2.5, 2.8])
    worse_score, worse_label, worse_factors = score_signal_quality(worse_payload, worse, Config())
    assert worse.direction == "ALT"
    assert worse_label == "PAS"
    assert worse_factors["market_move"] < factors["market_move"]
    assert worse_score < score
    assert set(factors) == {"fair_edge", "pace_support", "market_move", "game_state", "data_quality", "repeat_penalty"}


def test_over_pace_margin_and_q2_margin_lower_quality():
    q3_payload = match(status="Q3 05:00", opening_total=200, prematch_total=200,
                       inplay_total=187, score="60 - 60")
    q3 = decision(q3_payload, [5.0, 5.1])
    assert q3.direction == "ÜST" and q3.skip_reason == ""
    q3_score, _, q3_factors = score_signal_quality(q3_payload, q3, Config())
    relaxed = Config()
    relaxed.OVER_MIN_PACE_MARGIN_RATIO = 0
    relaxed_score, _, relaxed_factors = score_signal_quality(q3_payload, q3, relaxed)
    assert q3_factors["pace_support"] < relaxed_factors["pace_support"]
    assert q3_score < relaxed_score

    q2_payload = match(opening_total=210, prematch_total=210, inplay_total=194, score="41 - 40")
    q2 = decision(q2_payload, [5.0, 5.1])
    assert q2.direction == "ÜST" and q2.skip_reason == ""
    q2_score, _, q2_factors = score_signal_quality(q2_payload, q2, Config())
    relaxed.OVER_Q2_MIN_PACE_MARGIN_RATIO = 0
    relaxed_score, _, relaxed_factors = score_signal_quality(q2_payload, q2, relaxed)
    assert q2_factors["pace_support"] < relaxed_factors["pace_support"]
    assert q2_score < relaxed_score


def test_over_fair_edge_below_eight_keeps_candidate_with_penalty():
    payload = match(opening_total=200, prematch_total=200)
    candidate = decision(payload, [4.2, 4.3])
    assert candidate.direction == "ÜST" and candidate.skip_reason == ""
    score, _, factors = score_signal_quality(payload, candidate, Config())
    relaxed = Config()
    relaxed.OVER_MIN_FAIR_EDGE_POINTS = 0
    relaxed_score, _, relaxed_factors = score_signal_quality(payload, candidate, relaxed)
    assert factors["fair_edge"] < relaxed_factors["fair_edge"]
    assert score < relaxed_score


def test_quality_bounds_labels_repeat_and_determinism():
    payload = match()
    candidate = decision(payload, [4.4, 4.5])
    first = score_signal_quality(payload, candidate, Config(), observation_age_seconds=4)
    assert first == score_signal_quality(payload, candidate, Config(), observation_age_seconds=4)
    repeated = score_signal_quality(payload, candidate, Config(), repeated=True, observation_age_seconds=4)
    assert repeated[2]["repeat_penalty"] == -10
    assert repeated[0] == max(0, first[0] - 10)
    assert 0 <= first[0] <= 100
    for value, expected in ((0, "PAS"), (39, "PAS"), (40, "DÜŞÜK"), (54, "DÜŞÜK"),
                            (55, "ORTA"), (69, "ORTA"), (70, "YÜKSEK"), (84, "YÜKSEK"),
                            (85, "ÇOK YÜKSEK"), (100, "ÇOK YÜKSEK")):
        assert quality_label(value) == expected


def test_engine_pas_rules_remain_active():
    for payload, paces, reason in (
        (match(score=""), [4.4, 4.5], "missing_score"),
        (match(inplay_total=90), [4.4, 4.5], "PAS_STALE_DATA_SCORE_HIGH"),
        (match(), [3.0, 6.0], "PAS_VOLATILE_REGIME"),
        (match(), [4.0, 4.1], "PAS_MARKET_INSIDE_PACE_BAND"),
    ):
        candidate = decision(payload, paces)
        assert candidate.direction == "PAS"
        assert candidate.skip_reason == reason


def test_quality_insert_archive_restart_and_legacy_null(tmp_path):
    import dashboard

    database = Database(str(tmp_path / "quality.db"))
    database.init()
    legacy = database.save_alert("old", "Old - Match", 160, 170, "ALT", 10)
    payload = match(inplay_total=150, score="25 - 25")
    candidate = decision(payload, [2.5, 2.8])
    assert candidate.direction == "ALT"
    notifier = type("Notifier", (), {"send_alert": AsyncMock(return_value={"recipient": 1})})()
    with patch("main.evaluate_live_signal", return_value=candidate):
        asyncio.run(process_match(payload, database, notifier, Config()))
    row = database.latest_match_alert_in_direction("m", "ALT")
    notifier.send_alert.assert_awaited_once()
    assert row["quality_label"] == "PAS"
    assert row["telegram_status"] == "sent"
    assert row["quality_version"] == "v1"
    assert 0 <= row["quality_score"] <= 100
    assert json.loads(row["quality_factors"])["repeat_penalty"] == 0
    assert database.get_alert(legacy)["quality_score"] is None

    with patch.object(dashboard, "db", database):
        live = dashboard.app.test_client().get("/api/alerts").get_json()
        assert next(item for item in live if item["id"] == row["id"])["quality_score"] == row["quality_score"]
        dashboard._archive_active_match("m")
        stored = database.get_deleted_alert_by_id(row["id"])
        assert stored["quality_label"] == "PAS"
        from finished_match_service import _empty_result_summary, _settle_deleted_match_from_final_score
        summary = _empty_result_summary(tracked_count=1)
        assert _settle_deleted_match_from_final_score(database, summary, "m", "70 - 70", "Finished")
        settled = database.get_deleted_alert_by_id(row["id"])
        assert settled["result"] == "Başarılı"
        assert settled["quality_label"] == "PAS"
        assert summary["updated_count"] == 1
        snapshot = json.loads(stored["display_snapshot"])
        snapshot["quality_score"] = 0
        with database._conn() as conn:
            conn.execute("UPDATE alerts SET display_snapshot=? WHERE id=?", (json.dumps(snapshot), row["id"]))
        database = Database(database.db_path)
        database.init()
        with patch.object(dashboard, "db", database), patch.object(dashboard, "current_pace_projection", side_effect=AssertionError("recalculated")):
            archived = dashboard.app.test_client().get(f"/api/deleted-matches/{row['id']}/details").get_json()
            exported = list(csv.DictReader(io.StringIO(
                dashboard.app.test_client().get("/api/deleted-matches/export.csv").data.decode("utf-8-sig")
            )))
        assert archived["quality_score"] == row["quality_score"]
        assert archived["quality_label"] == row["quality_label"]
        assert next(item for item in exported if item["id"] == str(row["id"]))["quality_score"] == str(row["quality_score"])
    assert database.get_alert(legacy)["quality_score"] is None
