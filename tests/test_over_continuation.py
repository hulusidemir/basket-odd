"""Regressions for hot-start continuation, including the worker's frozen output."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from config import Config
from db import Database
from live_signals import evaluate_live_signal, forecast_live_total
from main import process_match
from tests.market_fixture import current_payload


CAPTURED = datetime.now(timezone.utc)


def match(**changes):
    return {
        "match_id": "m", "match_name": "Home - Away", "tournament": "FIBA",
        "opening_total": 160, "prematch_total": 160, "inplay_total": 185,
        "score": "50 - 50", "status": "Q3 10:00",
        "market_captured_at": CAPTURED.isoformat(), **changes,
    }


def anchor(elapsed=900, score=85, **changes):
    return {
        "elapsed_game_seconds": elapsed, "total_score": score, "period": 2,
        "game_clock": f"{(1200-elapsed)//60:02d}:{(1200-elapsed)%60:02d}",
        "recorded_at": (CAPTURED - timedelta(seconds=1200-elapsed)).isoformat(),
        **changes,
    }


def test_same_hot_start_changes_direction_when_recent_scoring_slows():
    config = Config()
    config.OVER_CALIBRATION_ENABLED = False
    slow = evaluate_live_signal(match(), [anchor(score=85)], config)
    sustained = evaluate_live_signal(match(), [anchor(score=65)], config)
    assert slow.direction == "ALT" and slow.sustainable_projection_center == 180
    assert sustained.direction == "ÜST" and sustained.sustainable_projection_center == 193.3
    assert slow.over_continuation["correction_points"] == pytest.approx(13.333333)
    assert sustained.over_continuation["applied"] is False


def test_short_scoring_burst_does_not_override_available_five_minute_interval():
    # Recent 2m: 15 points; recent 5m: 15 points. The longer interval shows the pause.
    forecast = forecast_live_total(match(), Config(), [anchor(score=85), anchor(1080, 85)])
    assert forecast["predicted_total"] == 180
    assert "recent_5m" in forecast["over_continuation"]["recent_window"]["labels"]
    assert forecast_live_total(match(), Config(), [anchor(score=85)] * 4) == forecast


def test_two_minute_interval_can_support_a_hot_start_without_five_minute_history():
    forecast = forecast_live_total(match(), Config(), [anchor(1080, 82)])
    assert forecast["over_continuation"]["future_ppm"] == pytest.approx(4.6666667)
    assert forecast["predicted_total"] == pytest.approx(179.7974763)
    assert forecast["over_calibration"]["applied"] is True
    assert "recent_2m" in forecast["over_continuation"]["recent_window"]["labels"]


def test_confirmed_score_correction_is_identical_before_and_after_current_row_is_saved():
    history = [anchor(score=95), anchor(1050, 110), anchor(1080, 85)]
    endpoint = {"elapsed_game_seconds": 1200, "total_score": 100, "period": 3,
                "game_clock": "10:00", "recorded_at": CAPTURED.isoformat()}
    before = forecast_live_total(match(), Config(), history)
    assert before == forecast_live_total(match(), Config(), history + [endpoint])
    assert before["over_continuation"]["recent_window"]["start_second"] == 1080


@pytest.mark.parametrize("bad", [
    {"recorded_at": (CAPTURED + timedelta(seconds=1)).isoformat()},
    {"recorded_at": "invalid"}, {"game_clock": "11:00"},
    {"elapsed_game_seconds": float("nan")}, {"period": None},
    {"game_clock": "04:00"}, {"total_score": 101},
])
def test_invalid_or_future_history_cannot_keep_an_unsupported_upper_estimate(bad):
    forecast = forecast_live_total(match(), Config(), [anchor(score=65, **bad)])
    assert forecast["predicted_total"] == 180
    assert forecast["over_continuation"]["source"] == "pregame_prior"


def test_missing_capture_time_cannot_make_local_history_contemporaneous():
    forecast = forecast_live_total(match(market_captured_at=None), Config(), [anchor(score=65)])
    assert forecast["predicted_total"] == 180


def test_one_center_for_every_line_and_cold_scoring_keeps_original_math():
    centers = [forecast_live_total(match(inplay_total=line), Config(), [anchor()])["predicted_total"]
               for line in (170, 180, 185, 200)]
    assert centers == [180] * 4
    cold = forecast_live_total(match(score="30 - 30"), Config(), [anchor(score=58)])
    assert cold["predicted_total"] == pytest.approx(126.6666667)
    assert cold["over_continuation"]["applied"] is False


def test_disable_flag_recovers_whole_game_math_without_losing_forecast():
    config = Config()
    config.OVER_CONTINUATION_ENABLED = False
    forecast = forecast_live_total(match(), config, [anchor()])
    assert forecast["predicted_total"] == pytest.approx(193.333333)
    assert forecast["over_continuation"]["enabled"] is False
    assert forecast_live_total(match(), Config(), [])["predicted_total"] == 180


def test_worker_freezes_same_adjusted_center_as_published_signal(tmp_path):
    database = Database(str(tmp_path / "over.db"))
    database.init()
    old_payload = current_payload(match(status="Q2 05:00", score="43 - 42"))
    database.save_snapshot_if_changed("m", 2, "05:00", 900, 25, 43, 42, 85, 160, 185,
                                     market_provenance=old_payload["market_provenance"])
    captured = datetime.now(timezone.utc)
    with database._conn() as conn:
        conn.execute("UPDATE match_live_snapshots SET recorded_at=?",
                     ((captured - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S"),))
    original = database.get_match_snapshots("m")[0]
    payload = current_payload(match(market_captured_at=captured.isoformat()))
    notifier = type("Notifier", (), {"send_alert": AsyncMock(return_value={"recipient": 1})})()
    asyncio.run(process_match(payload, database, notifier, Config()))
    frozen = json.loads(database.get_match_snapshots("m")[-1]["forecast_json"])["forecast"]
    signal = database.get_alert(1)
    assert frozen["engine"] == "future_pace_v9"
    assert frozen["predicted_total"] == signal["fair_total"] == 180
    assert frozen["over_continuation"]["source"] == "recent_scoring"
    assert frozen["direction"] == signal["direction"] == "ALT"
    context = json.loads(signal["prediction_context_json"])
    assert context["decision"]["over_continuation"] == frozen["over_continuation"]
    assert context["decision"]["over_calibration"] == frozen["over_calibration"]
    assert database.get_match_snapshots("m")[0] == original
    notifier.send_alert.assert_awaited_once()
