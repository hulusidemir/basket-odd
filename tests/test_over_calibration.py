import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from config import Config
from db import Database
from live_signals import evaluate_live_signal, forecast_live_total
from main import process_match
from over_calibration import MODEL, MODEL_SHA256
from over_calibration_audit import fit_bias
from tests.market_fixture import current_payload


def observation(line=185, score="50 - 50", **changes):
    captured = datetime.now(timezone.utc)
    match = {"match_id": "m", "match_name": "Home - Away", "tournament": "FIBA",
             "status": "Q3 10:00", "score": score, "prematch_total": 160,
             "opening_total": 160, "inplay_total": line, "market_captured_at": captured.isoformat(),
             **changes}
    history = [{"elapsed_game_seconds": 900, "period": 2, "total_score": 65,
                "game_clock": "05:00", "recorded_at": (captured-timedelta(minutes=5)).isoformat()}]
    return match, history


def test_empirical_correction_changes_an_unsupported_upper_without_a_new_filter():
    match, history = observation()
    corrected = evaluate_live_signal(match, history, Config())
    baseline = Config()
    baseline.OVER_CALIBRATION_ENABLED = False
    original = evaluate_live_signal(match, history, baseline)
    assert original.direction == "ÜST" and original.sustainable_projection_center == 193.3
    assert corrected.direction == "ALT" and corrected.sustainable_projection_center == 179.8
    assert corrected.over_calibration["model_sha256"] == MODEL_SHA256
    assert corrected.skip_reason == original.skip_reason == ""


def test_large_observed_score_can_still_support_an_upper_and_all_lines_share_one_center():
    match, history = observation(score="58 - 57")
    decision = evaluate_live_signal(match, history, Config())
    assert decision.direction == "ÜST" and decision.sustainable_projection_center == 194.6
    centers = [forecast_live_total({**match, "inplay_total": line}, Config(), history)["predicted_total"]
               for line in (175, 185, 200)]
    assert centers[0] == centers[1] == centers[2]


def test_slow_recent_scoring_missing_history_and_changed_prior_strength_use_v8_fallback():
    match, history = observation()
    history[0]["total_score"] = 85
    slow = forecast_live_total(match, Config(), history)
    assert slow["predicted_total"] == 180 and slow["over_calibration"]["applied"] is False
    assert forecast_live_total(match, Config(), [])["over_calibration"]["applied"] is False
    config = Config()
    config.PRIOR_EQUIV_MINUTES = 20
    history[0]["total_score"] = 65
    changed = forecast_live_total(match, config, history)
    assert changed["over_calibration"]["applied"] is False
    assert changed["predicted_total"] == 190


def test_coefficient_fit_excludes_cold_regimes_and_normalizes_by_hot_excess():
    row = {"duration": 40, "elapsed": 20, "remaining": 20, "prior": 4,
           "continuation": {"enabled": True, "raw_future_ppm": 4.667,
                            "future_ppm": 4.4, "recent_window": {"future_ppm": 4.5}},
           "adjusted": 188, "final": 183}
    cold = {**row, "final": -99999, "continuation": {**row["continuation"], "raw_future_ppm": 3}}
    value, n = fit_bias([row, cold], "excess", Config())
    assert value == pytest.approx(.625) and n == 1
    assert MODEL["training_cutoff_utc"] == "2026-09-29T00:00:00+00:00"


def test_worker_freezes_empirical_calibration_in_signal_and_forecast(tmp_path):
    db = Database(str(tmp_path / "calibration.db"))
    db.init()
    match, history = observation(line=170)
    previous = current_payload({**match, "status": "Q2 05:00", "score": "33 - 32"})
    db.save_snapshot_if_changed("m", 2, "05:00", 900, 25, 33, 32, 65, 160, 170,
                                market_provenance=previous["market_provenance"])
    with db._conn() as conn:
        conn.execute("UPDATE match_live_snapshots SET recorded_at=?",
                     ((datetime.now(timezone.utc)-timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S"),))
    notifier = type("Notifier", (), {"send_alert": AsyncMock(return_value={"recipient": 1})})()
    asyncio.run(process_match(current_payload(match), db, notifier, Config()))
    frozen = json.loads(db.get_match_snapshots("m")[-1]["forecast_json"])
    signal = db.get_alert(1)
    context = json.loads(signal["prediction_context_json"])
    assert frozen["forecast"]["engine"] == "future_pace_v10"
    assert frozen["forecast"]["over_calibration"]["applied"] is True
    assert context["decision"]["over_calibration"] == frozen["forecast"]["over_calibration"]
    assert signal["fair_total"] == round(frozen["forecast"]["predicted_total"], 1)
    assert signal["direction"] == frozen["forecast"]["direction"] == "ÜST"
    assert frozen["policy"]["over_calibration_model_sha256"] == MODEL_SHA256
    notifier.send_alert.assert_awaited_once()


@pytest.fixture(autouse=True)
def isolate_pace_math_from_final_error_bias(monkeypatch):
    # These legacy cases verify the pace stage and its source/history rules.
    # The separate v10 distribution tests cover the fitted error correction
    # and the default probability publication floor.
    from win_probability import MODEL
    monkeypatch.setitem(MODEL, "mean_normalized_error", 0)
    monkeypatch.setattr(Config, "MIN_SIGNAL_WIN_PROBABILITY", .5)
