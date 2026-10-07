import copy
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from config import Config
from db import Database
from forecast_tracking import forecast_outcome, tracking_summary
from forward_validation import freeze_forecast
from live_signals import direction_for_total, forecast_live_total
from tests.test_continuous_forecasts import payload


@pytest.fixture
def database(tmp_path):
    result = Database(str(tmp_path / "tracking.db"))
    result.init()
    return result


def save(database, match_id="m", line=157.5, center=160):
    match = payload(match_id=match_id, inplay_total=line)
    forecast = forecast_live_total(match, Config())
    forecast.update(predicted_total=center, direction=direction_for_total(center,line))
    context = freeze_forecast(match,forecast,Config())
    database.save_snapshot_if_changed(match_id,2,"05:00",900,25,30,30,60,160,line,
                                      market_provenance=match["market_provenance"],forecast=context)
    return database.get_match_snapshots(match_id)[-1]


def final(database, match_id="m", total=150):
    database.save_forecast_final_observation(match_id,f"75 - {total-75}","Finished",total)


def test_first_loss_is_kept_while_later_winning_forecasts_remain_in_history(database):
    first = save(database)
    save(database,line=149)
    final(database)
    data = database.forecast_tracking_data()
    summary = tracking_summary(data)
    assert summary["matches"] == summary["losses"] == 1
    assert summary["wins"] == 0 and summary["success_rate"] == 0
    assert summary["forecast_count"] == 2
    assert [forecast_outcome(r) for r in data["rows"]] == ["win","loss"]
    assert [r["is_first"] for r in data["rows"]] == [False,True]
    assert database.get_match_snapshots("m")[0] == first


def test_summary_excludes_pending_push_and_equal_direction_from_rate(database):
    save(database,"loss");final(database,"loss",150)
    save(database,"win");final(database,"win",170)
    save(database,"pending")
    save(database,"push",line=160,center=170);final(database,"push",160)
    save(database,"equal",line=160,center=160);final(database,"equal",165)
    summary = tracking_summary(database.forecast_tracking_data())
    assert summary["matches"] == summary["forecast_count"] == 5
    assert summary["wins"] == summary["losses"] == summary["pending"] == summary["pushes"] == summary["no_direction"] == 1
    assert summary["decided"] == 2 and summary["success_rate"] == 50


def test_empty_or_only_pending_history_has_no_invented_success_percentage(database):
    assert tracking_summary(database.forecast_tracking_data())["success_rate"] is None
    save(database)
    summary = tracking_summary(database.forecast_tracking_data())
    assert summary["pending"] == 1 and summary["success_rate"] is None


def test_null_legacy_snapshots_do_not_turn_into_backfilled_predictions(database):
    database.save_snapshot_if_changed("legacy",2,"05:00",900,25,30,30,60,160,157.5)
    before = database.get_match_snapshots("legacy")
    save(database,"new")
    data = database.forecast_tracking_data()
    assert data["forecast_count"] == len(data["first"]) == 1
    assert database.get_match_snapshots("legacy") == before


def test_invalid_first_forecast_is_not_replaced_by_a_later_winner(database):
    save(database)
    with database._conn() as conn:
        conn.execute("UPDATE match_live_snapshots SET forecast_json='broken'")
    save(database,line=149)
    final(database)
    summary = tracking_summary(database.forecast_tracking_data())
    assert summary["invalid"] == summary["matches"] == 1
    assert summary["wins"] == summary["losses"] == 0
    assert summary["success_rate"] is None


def test_cursor_pages_do_not_duplicate_or_skip_old_records_after_new_insert(database):
    for line in (151,152,153,154,155):
        save(database,line=line)
    first = database.forecast_tracking_data(limit=2)
    assert [r["id"] for r in first["rows"]] == [5,4]
    save(database,line=156)
    second = database.forecast_tracking_data(before_id=first["next_cursor"],limit=2)
    third = database.forecast_tracking_data(before_id=second["next_cursor"],limit=2)
    assert [r["id"] for r in second["rows"]] == [3,2]
    assert [r["id"] for r in third["rows"]] == [1]
    assert third["next_cursor"] is None
    assert second["forecast_count"] == 6 and len(second["first"]) == 1


def row_with_times():
    at = datetime(2026,10,1,12,tzinfo=timezone.utc)
    return {"recorded_at":at.isoformat(),"forecast_json":json.dumps({
        "market_captured_at":at.isoformat(),"forecast":{"line":157.5,"predicted_total":160,"direction":"ÜST"}}),
        "final_total":150,"result_source":"automatic_final_score","settled_at":(at+timedelta(hours=1)).isoformat()},at


def test_unobserved_or_nonautomatic_final_does_not_settle_prediction():
    row,at = row_with_times()
    assert forecast_outcome(row,as_of=at+timedelta(minutes=5)) == "pending"
    assert forecast_outcome(row,as_of=at+timedelta(hours=2)) == "loss"
    assert forecast_outcome({**row,"result_source":"manual"},as_of=at+timedelta(hours=2)) == "pending"
    assert forecast_outcome({**row,"settled_at":(at-timedelta(minutes=5)).isoformat()},as_of=at+timedelta(hours=2)) == "invalid"


def test_second_precision_final_is_allowed_without_accepting_later_predictions():
    row,at = row_with_times()
    context = json.loads(row["forecast_json"])
    context["market_captured_at"] = (at+timedelta(milliseconds=500)).isoformat()
    row.update(forecast_json=json.dumps(context),settled_at=at.isoformat())
    assert forecast_outcome(row,as_of=at+timedelta(minutes=1)) == "loss"
    context["market_captured_at"] = (at+timedelta(seconds=2)).isoformat()
    row["forecast_json"] = json.dumps(context)
    assert forecast_outcome(row,as_of=at+timedelta(minutes=1)) == "invalid"


@pytest.mark.parametrize("invalid",[True,None,float("nan"),float("inf"),-1])
def test_bad_final_totals_cannot_count_as_success(invalid):
    row,at = row_with_times()
    assert forecast_outcome({**row,"final_total":invalid},as_of=at+timedelta(hours=2)) == "invalid"


def test_history_api_keeps_frozen_display_and_scenario_requests_out_of_success(database):
    import dashboard
    first = save(database)
    save(database,line=149)
    final(database)
    before = copy.deepcopy(database.get_match_snapshots("m"))
    with patch.object(dashboard,"db",database),patch("main.forecast_live_total",side_effect=AssertionError("recomputed")):
        client = dashboard.app.test_client()
        data = client.get("/api/forecasts/history").get_json()
        assert data["summary"]["losses"] == 1 and data["summary"]["wins"] == 0
        assert data["items"][-1]["forecast"]["line"] == 157.5
        assert data["items"][-1]["score"] == "30 - 30"
        assert data["items"][-1]["forecast"]["predicted_total"] == 160
        assert data["items"][-1]["final_total"] == 150
        assert data["items"][-1]["is_first"] is True
        encoded = json.dumps(data)
        assert "market_provenance" not in encoded and "source" not in encoded and "policy" not in encoded
        client.get("/api/forecasts?line=180")
        assert client.get("/api/forecasts/history").get_json() == data
        html = client.get("/forecasts").get_data(as_text=True)
        assert "Tahmin geçmişi" in html and "forecastSuccessRate" in html
        assert client.post("/api/forecasts/history",json={"result":"win"}).status_code == 405
    assert database.get_match_snapshots("m") == before
    assert before[0] == first


@pytest.mark.parametrize("query",["limit=0","limit=101","limit=x","before=0","before=-1","before=x"])
def test_history_api_bounds_page_requests(database,query):
    import dashboard
    with patch.object(dashboard,"db",database):
        assert dashboard.app.test_client().get("/api/forecasts/history?"+query).status_code == 400


def test_history_indexes_are_additive_idempotent_and_used(database):
    save(database)
    before = database.get_match_snapshots("m")
    database.init();database.init()
    assert database.get_match_snapshots("m") == before
    with database._conn() as conn:
        plans = conn.execute("EXPLAIN QUERY PLAN SELECT id FROM match_live_snapshots WHERE forecast_json IS NOT NULL AND id < 100 ORDER BY id DESC LIMIT 50").fetchall()
        index_names = {r["name"] for r in conn.execute("PRAGMA index_list(match_live_snapshots)")}
    assert {"idx_forecast_snapshots_match","idx_forecast_snapshots_history"} <= index_names
    assert any("idx_forecast_snapshots_history" in r["detail"] for r in plans)
