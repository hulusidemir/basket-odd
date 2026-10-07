import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from config import Config
from db import Database
from forward_validation import freeze_prediction, report
from live_signals import SignalDecision
from main import process_match
from tests.market_fixture import verified_payload


@pytest.fixture
def database(tmp_path):
    db = Database(str(tmp_path / "forward.db"))
    db.init()
    return db


def published(database, match_id="m", direction="ALT", config=None, signal_count=1):
    config = config or Config()
    payload = verified_payload({
        "match_id": match_id, "match_name": "Home - Away", "tournament": "FIBA",
        "status": "Q2 05:00", "score": "30 - 24", "opening_total": 160,
        "prematch_total": 165, "inplay_total": 180,
    })
    decision = SignalDecision("prematch", 165, 15, 0, direction, 2,
                              sustainable_projection_center=150)
    context = freeze_prediction(payload, decision, config, [])
    return database.save_alert(
        match_id, payload["match_name"], 160, 180, direction, 15, prematch=165,
        status=payload["status"], score=payload["score"], signal_count=signal_count,
        quality_score=70, quality_version="v1",
        market_provenance=payload["market_provenance"], prediction_context=context,
    )


def settle(database, alert_id, result="Başarılı", total=170, settled_at=None):
    # Test fixtures only. Production settlement remains the automatic finished-match service.
    with database._conn() as conn:
        conn.execute("UPDATE alerts SET result=?,final_total=?,result_source='automatic_final_score',"
                     "settled_at=? WHERE id=?", (result, total,
                     (settled_at or datetime.now(timezone.utc)).isoformat(), alert_id))


def overall(database, **kwargs):
    cohorts = report(database.db_path, **kwargs)["cohorts"]
    return next(iter(cohorts.values()))["overall"]


def test_only_actual_published_predictions_get_a_frozen_policy(database):
    payload = verified_payload({"match_id": "m", "match_name": "A - B", "tournament": "FIBA",
        "status": "Q2 05:00", "score": "30 - 24", "opening_total": 160, "inplay_total": 180})
    candidate = SignalDecision("opening", 160, 20, 0, "ALT", 2)
    notifier = type("Notifier", (), {"send_alert": AsyncMock(return_value={"recipient": 1})})()
    config = Config()
    config.MIN_SIGNAL_QUALITY = 70
    for direction in ("PAS", "ALT"):
        decision = candidate if direction == "ALT" else SignalDecision("opening", 160, 20, 0, "PAS", 2)
        with patch("main.evaluate_live_signal", return_value=decision), patch(
            "main.score_signal_quality", return_value=(39, "PAS", {}),
        ):
            asyncio.run(process_match(payload, database, notifier, config))
        assert database.count_match_alerts("m") == int(direction == "ALT")
    context = json.loads(database.get_alert(1)["prediction_context_json"])
    assert context["decision"]["direction"] == "ALT"
    assert context["clock"]["quarter_length"] == 10
    assert len(context["policy_id"]) == 64
    assert context["snapshot_ids"]
    assert context["policy"]["publication"] == "verified_future_pace_v7"
    assert "MIN_SIGNAL_QUALITY" not in context["policy"]["parameters"]
    settle(database, 1)
    assert overall(database)["wins"] == 1


def test_legacy_frozen_policy_retains_its_original_quality_gate(database):
    from forward_validation import _digest
    alert_id = published(database)
    row = database.get_alert(alert_id)
    context = json.loads(row["prediction_context_json"])
    context["policy"].pop("publication")
    context["policy"]["parameters"]["MIN_SIGNAL_QUALITY"] = 70
    context["policy_id"] = _digest(context["policy"])
    with database._conn() as conn:
        conn.execute("UPDATE alerts SET prediction_context_json=?,quality_score=69 WHERE id=?",
                     (json.dumps(context), alert_id))
    assert report(database.db_path)["excluded"]["invalid_prediction_or_source"] == 1


def test_config_change_creates_separate_policy_and_secret_values_are_excluded(database):
    first_id = published(database, "a")
    config = Config()
    config.MIN_EDGE_POINTS += 1
    config.TELEGRAM_TOKEN = "private-token-do-not-save"
    config.TELEGRAM_CHAT_ID = "private-recipient-do-not-save"
    second_id = published(database, "b", config=config)
    first = json.loads(database.get_alert(first_id)["prediction_context_json"])
    second = json.loads(database.get_alert(second_id)["prediction_context_json"])
    assert first["policy_id"] != second["policy_id"]
    assert "private-" not in json.dumps(second)
    assert len(report(database.db_path)["cohorts"]) == 2


def test_pending_first_prediction_is_not_replaced_by_a_later_win(database):
    published(database)
    second = published(database, signal_count=2)
    settle(database, second)
    summary = overall(database)
    assert summary["matches"] == summary["pending"] == 1
    assert summary["wins"] == 0 and summary["win_rate"] is None
    assert report(database.db_path)["excluded"]["repeat_prediction"] == 1


def test_invalid_first_prediction_does_not_select_a_later_winner(database):
    first = published(database)
    second = published(database, signal_count=2)
    settle(database, second)
    with database._conn() as conn:
        conn.execute("UPDATE alerts SET market_provenance_json=NULL WHERE id=?", (first,))
    output = report(database.db_path)
    assert output["cohorts"] == {}
    assert output["excluded"]["invalid_prediction_or_source"] == 1


def test_as_of_cutoff_never_uses_a_future_final(database):
    alert_id = published(database)
    cutoff = datetime.now(timezone.utc) + timedelta(seconds=1)
    settle(database, alert_id, settled_at=cutoff + timedelta(hours=1))
    assert overall(database, as_of=cutoff)["pending"] == 1
    assert overall(database, as_of=cutoff + timedelta(hours=2))["wins"] == 1


def test_pushes_and_invalid_settlements_are_not_wins(database):
    settle(database, published(database, "push"), result="İade", total=180)
    settle(database, published(database, "bad"), result="Başarılı", total=190)
    settle(database, published(database, "win", direction="ÜST"), total=190)
    summary = overall(database)
    assert (summary["pushes"], summary["invalid_result"], summary["wins"]) == (1, 1, 1)
    assert summary["win_rate"] == 1
    assert summary["wilson_95"][0] < 0.5


def test_historical_source_is_checked_at_capture_not_reporting_time(database):
    settle(database, published(database))
    assert overall(database, as_of=datetime.now(timezone.utc) + timedelta(days=30))["wins"] == 1


def test_legacy_database_migration_and_read_only_report(database):
    legacy_id = database.save_alert("old", "Old - Match", 160, 170, "ALT", 10)
    with database._conn() as conn:
        conn.execute("ALTER TABLE alerts DROP COLUMN prediction_context_json")
    legacy = database.get_alert(legacy_id)
    from pathlib import Path
    before = Path(database.db_path).read_bytes()
    assert report(database.db_path)["excluded"]["legacy_without_frozen_policy"] == 1
    assert Path(database.db_path).read_bytes() == before
    database.init()
    migrated = database.get_alert(legacy_id)
    assert migrated.pop("prediction_context_json") is None
    assert migrated == legacy
    database.init()
    assert database.get_alert(legacy_id)["prediction_context_json"] is None


@pytest.mark.parametrize("column,value", [
    ("live", 197.5), ("direction", "ÜST"), ("market_provenance_json", "{}"),
    ("prediction_context_json", '{"schema_version":1,"policy_id":"fake","policy":{}}'),
])
def test_inconsistent_prediction_or_source_is_excluded(database, column, value):
    alert_id = published(database)
    with database._conn() as conn:
        conn.execute(f"UPDATE alerts SET {column}=? WHERE id=?", (value, alert_id))
    assert report(database.db_path)["cohorts"] == {}


def test_forecast_error_uses_frozen_prediction_and_market_baseline(database):
    alert_id = published(database)
    settle(database, alert_id, total=170)
    with database._conn() as conn:
        conn.execute('UPDATE alerts SET fair_total=999 WHERE id=?', (alert_id,))
    error = overall(database)['forecast_error']
    assert error['samples'] == 1
    assert (error['model_mae'], error['market_mae'], error['model_bias']) == (20, 10, -20)
    assert (error['model_closer'], error['market_closer'], error['equal_error']) == (0, 1, 0)
    assert error['pregame_baseline_samples'] == 1
    # Frozen score 54, 25 minutes remaining at 165/40 PPM -> 157.125, final 170.
    assert error['pregame_baseline_mae'] == 12.875


def test_unsettled_or_invalid_result_does_not_enter_forecast_error(database):
    published(database, 'pending')
    settle(database, published(database, 'bad'), total=190, result='Başarılı')
    summary = overall(database)
    assert summary['forecast_error']['samples'] == 0
    assert summary['forecast_error']['model_mae'] is None


def test_delivered_subset_does_not_replace_cancelled_first_with_later_win(database):
    first = published(database)
    later = published(database, signal_count=2)
    settle(database, first, total=190, result='Başarısız')
    settle(database, later, total=170)
    with database._conn() as conn:
        conn.execute("UPDATE alerts SET telegram_status='cancelled' WHERE id=?", (first,))
        conn.execute("UPDATE alerts SET telegram_status='sent' WHERE id=?", (later,))
    cohort = next(iter(report(database.db_path)['cohorts'].values()))
    assert cohort['overall']['losses'] == 1
    assert cohort['delivered']['matches'] == 0
    assert cohort['delivery_status'] == {'cancelled': 1}


def test_new_prediction_freezes_actual_intervals_and_merged_labels():
    payload = verified_payload({'match_id': 'm', 'match_name': 'Home - Away', 'tournament': 'FIBA',
                               'status': 'Q2 05:00', 'score': '40 - 35', 'opening_total': 160,
                               'prematch_total': 160, 'inplay_total': 180})
    history = [{'id': 1, 'elapsed_game_seconds': 780, 'total_score': 60, 'period': 2,
                'recorded_at': (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()}]
    decision = SignalDecision('prematch', 160, 20, 0, 'ALT', 2)
    context = freeze_prediction(payload, decision, Config(), history)
    assert len(context['pace_windows']) == 2
    assert context['pace_windows'][1]['labels'] == ['recent_2m', 'observed_period_segment']
