"""Protect reversal research against hindsight, duplicated matches and bad clocks."""

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from reversal_audit import (
    audit, evaluate, first_signals, fit, future_endpoint, metrics,
    observation, predict, split, valid_snapshot_clock,
)


NOW = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)


def alert(**changes):
    row = {"id": 1, "match_id": "fixture", "match_name": "Home - Away", "tournament": "League",
           "status": "Q2 05:00", "score": "40 - 40", "opening": 160, "prematch": 160,
           "live": 175, "alerted_at": NOW.isoformat(), "prediction_context_json": None,
           "reversal_features_json": None, "market_provenance_json": None}
    return {**row, **changes}


def snapshot(minutes, total, *, at=None, clock=None, period=None):
    period = period or int((minutes-1e-6)//10)+1
    seconds = round((period*10-minutes)*60)
    return {"match_id": "fixture", "recorded_at": (at or NOW).isoformat(), "period": period,
            "game_clock": clock if clock is not None else f"{seconds//60:02d}:{seconds%60:02d}",
            "elapsed_game_seconds": round(minutes*60), "remaining_minutes": 40-minutes,
            "total_score": total}


def state():
    result, error = observation(alert(), [snapshot(15,80)])
    assert error is None
    return result


def test_first_match_record_is_chosen_before_outcome_or_feature_eligibility():
    first = alert(id=1,score="bad",final_total=None)
    later = alert(id=2,alerted_at=(NOW+timedelta(minutes=1)).isoformat(),final_total=200)
    assert first_signals([later,first]) == [first]


def test_future_and_final_fields_cannot_change_signal_time_features():
    rows = [snapshot(8.5,40,at=NOW-timedelta(minutes=7)),
            snapshot(12.5,60,at=NOW-timedelta(minutes=3)),snapshot(15,80)]
    before, error = observation(alert(),rows)
    after, error_after = observation(alert(final_total=220),rows+[snapshot(20,105,at=NOW+timedelta(minutes=6))])
    assert error is error_after is None
    assert before == after
    assert before["recent"] == 8
    assert before["trend"] == 3


def test_prefix_must_match_signal_score_clock_and_capture_time():
    assert observation(alert(),[snapshot(15,79)])[1] == "prefix_state_mismatch"
    assert observation(alert(),[snapshot(14,80)])[1] == "prefix_state_mismatch"
    assert observation(alert(),[snapshot(15,80,at=NOW-timedelta(minutes=3))])[1] == "prefix_state_mismatch"
    assert observation(alert(),[snapshot(15,80,at=NOW+timedelta(seconds=1))])[1] == "missing_prefix"


def test_legacy_missing_clock_is_explicit_and_not_invented():
    result,error = observation(alert(),[snapshot(15,80,clock="")])
    assert error is None
    assert result["legacy_clock_missing"] is True
    assert observation(alert(),[snapshot(15,80,clock="02:00")])[1] == "prefix_clock_mismatch"


def test_future_window_is_label_only_and_uses_real_endpoint_length():
    initial = state()
    rows = [snapshot(15,80),snapshot(20.25,100,at=NOW+timedelta(minutes=7))]
    result = future_endpoint(initial,rows)
    assert result["target_minutes"] == 5.25
    assert result["future"] == pytest.approx(20/5.25)
    assert "future" not in initial


@pytest.mark.parametrize("endpoint",[
    snapshot(21,100,at=NOW+timedelta(minutes=7)),
    snapshot(20,100,at=NOW-timedelta(seconds=1)),
    snapshot(20,100,at=NOW+timedelta(minutes=91)),
    snapshot(20,70,at=NOW+timedelta(minutes=7)),
    snapshot(20,100,at=NOW+timedelta(minutes=7),clock="01:00"),
])
def test_bad_future_clock_score_timestamp_or_horizon_cannot_label(endpoint):
    assert future_endpoint(state(),[snapshot(15,80),endpoint]) is None


@pytest.mark.parametrize("correction",[
    snapshot(16,79,at=NOW+timedelta(minutes=1)),
    snapshot(14,85,at=NOW+timedelta(minutes=1)),
])
def test_score_or_clock_rollback_invalidates_interval(correction):
    rows = [snapshot(15,80),correction,snapshot(20,100,at=NOW+timedelta(minutes=7))]
    assert future_endpoint(state(),rows) is None


def test_regulation_endpoint_excludes_overtime_and_uses_last_minute():
    rows = [snapshot(15,80),snapshot(39.5,160,at=NOW+timedelta(minutes=50)),
            snapshot(44,185,at=NOW+timedelta(minutes=65),period=4)]
    result = future_endpoint(state(),rows,remaining_game=True)
    assert result["target_minutes"] == 24.5
    assert result["future"] == pytest.approx(80/24.5)


def test_clock_format_and_period_mapping_are_checked():
    s = state()
    assert valid_snapshot_clock(snapshot(15,80),s)
    assert not valid_snapshot_clock(snapshot(15,80,period=3),s)
    assert not valid_snapshot_clock(snapshot(15,80,clock="05:60"),s)


def samples(count=160):
    rows = []
    for index in range(count):
        s = {**state(), "at": NOW+timedelta(days=index), "match_id": str(index),
             "current": 3.0+(index%7)*.3,"recent": 3.0+(index%5)*.4,
             "trend": (index%3-1)*.5,"margin": (index%8)*.04,
             "quarter_fraction": (index%9)/10}
        s.update(future=4+.2*(s["current"]-4),target_minutes=5,label_at=s["at"]+timedelta(minutes=6))
        rows.append(s)
    return rows


def test_chronological_split_purges_unobserved_training_outcomes():
    rows = samples()
    train,test,cutoff = split(rows,.7)
    assert {r["match_id"] for r in train}.isdisjoint(r["match_id"] for r in test)
    assert all(r["at"] < cutoff and r["label_at"] < cutoff for r in train)
    train[-1]["label_at"] = cutoff+timedelta(days=1)
    purged,_,_ = split(rows,.7)
    assert train[-1] not in purged


def test_linear_fit_handles_constant_missing_features_and_known_relation():
    rows = samples()
    model = fit(rows,.000001)
    assert max(abs(predict(model,r)-r["future"]) for r in rows) < .00001


def test_outer_test_results_cannot_select_penalty_or_change_fitted_coefficients():
    rows = samples()
    report = evaluate(rows)
    _,_,cutoff = split(rows,.7)
    altered = [{**r,"future": 12} if r["at"] >= cutoff else dict(r) for r in rows]
    other = evaluate(altered)
    for name in ("history_ridge","history_and_market_ridge"):
        before,after = report["holdout"][name],other["holdout"][name]
        assert before["training_fit"] == after["training_fit"]
        assert before["penalty_selected_in_training"] == after["penalty_selected_in_training"]
        assert before["rate_mae"] != after["rate_mae"]


def test_reversal_accuracy_is_distinct_from_material_change_and_bet_direction():
    s = {**state(),"future":4,"final_total":180}
    result = metrics([s],[5])
    assert result["speed_direction_correct"] == 1
    assert result["material_speed_direction_correct"] == 0
    assert result["bet_direction_wins"] == 1
    assert metrics([s],[(175-80)/25])["bet_no_direction"] == 1


def test_cli_audit_reads_database_without_modifying_it_or_exposing_rows(tmp_path):
    path = tmp_path/"research.db"
    row = {**alert(),"direction":"ÜST","quality_version":None,"quality_score":None,
           "final_total":180,"result_source":"automatic_final_score",
           "settled_at":(NOW+timedelta(minutes=70)).isoformat()}
    snap = snapshot(15,80)
    with sqlite3.connect(path) as c:
        c.execute("CREATE TABLE alerts ("+",".join(row.keys())+")")
        c.execute("INSERT INTO alerts VALUES ("+",".join("?" for _ in row)+")",tuple(row.values()))
        c.execute("CREATE TABLE match_live_snapshots (id INTEGER PRIMARY KEY,"+",".join(snap.keys())+")")
        for index,value in enumerate([snap,snapshot(20,100,at=NOW+timedelta(minutes=7))]):
            c.execute("INSERT INTO match_live_snapshots VALUES ("+",".join("?" for _ in range(len(value)+1))+")",(index,*value.values()))
    before = path.read_bytes()
    report = audit(path)
    assert path.read_bytes() == before
    assert report["first_signal_matches"] == 1
    assert report["evaluations"]["next_five_minutes"]["eligible_matches"] == 1
    serialized = json.dumps(report)
    assert "Home - Away" not in serialized and '"fixture"' not in serialized
