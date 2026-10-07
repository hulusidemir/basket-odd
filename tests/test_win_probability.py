import copy
from datetime import datetime, timedelta, timezone

import pytest

from probability_audit import fit_and_validate, fit_for_future_matches, read_observations, read_reconstructed_observations
from win_probability import MODEL, MODEL_SHA256, assess_forecast, outcome_probabilities


def forecast(direction="ALT", **changes):
    return {"engine": "future_pace_v9", "prior_equivalent_minutes": 10,
        "over_continuation": {"enabled": True}, "over_calibration": {"enabled": True},
        "elapsed_minutes": 20, "remaining_minutes": 20,
        "predicted_total": 150, "line": 155.5, "direction": direction, **changes}


def accepted_model():
    model = copy.deepcopy(MODEL)
    model.update(mean_normalized_error=0, normalized_error_scale=2)
    model["validation"] = {direction: {"accepted": True, "matches": 30,
        "probability_range": [.05, .95]} for direction in ("ALT", "ÜST")}
    return model


def test_integer_barem_keeps_push_separate_from_winning_probability():
    values = outcome_probabilities(150, 150, 20, 0, 2)
    assert values["ALT"] == pytest.approx(values["ÜST"])
    assert values["ALT"] < .5 and values["push"] > 0
    assert sum(values.values()) == pytest.approx(1)
    half = outcome_probabilities(150, 150.5, 20, 0, 2)
    assert half["push"] == pytest.approx(0)
    assert half["ALT"] > .5


def test_probability_responds_to_line_and_error_uncertainty():
    low = outcome_probabilities(150, 155.5, 20, 0, 2)["ALT"]
    high = outcome_probabilities(150, 165.5, 20, 0, 2)["ALT"]
    uncertain = outcome_probabilities(150, 155.5, 20, 0, 5)["ALT"]
    assert .5 < uncertain < low < high < 1


@pytest.mark.parametrize("values", [
    (True, 150, 20, 0, 2), (150, float("nan"), 20, 0, 2),
    (150, 150, -1, 0, 2), (150, 150, 20, 0, 0),
])
def test_invalid_probability_input_is_rejected(values):
    with pytest.raises(ValueError):
        outcome_probabilities(*values)


def test_estimated_probability_is_separate_from_validation_status():
    for direction in ("ALT", "ÜST"):
        result = assess_forecast(forecast(direction))
        assert result["model_sha256"] == MODEL_SHA256
        assert 0 < result["probability"] < 1 and result["validated"] is False
        assert result["reason"] == "model_estimate"
        assert result["under_probability"] + result["over_probability"] + result["push_probability"] == pytest.approx(1)


def test_invalid_inputs_are_rejected_and_extrapolation_is_marked_unvalidated():
    model = accepted_model()
    result = assess_forecast(forecast(), model=model)
    assert .5 < result["probability"] < .9 and result["validated"] is True
    assert result["prospective_validated"] is False
    for changed in (forecast(prior_equivalent_minutes=5),
                    forecast(over_calibration={"enabled": False}),
                    forecast(elapsed_minutes=None)):
        assert assess_forecast(changed, model=model)["probability"] is None
    model["validation"]["ALT"]["probability_range"] = [.8, .9]
    assert assess_forecast(forecast(), model=model)["validated"] is False
    assert assess_forecast(forecast(remaining_minutes=4, elapsed_minutes=36), model=model)["probability"] is not None


def test_distribution_cannot_remove_points_already_scored():
    result = outcome_probabilities(151, 149.5, 5, 0, 3, score=150)
    assert result == {"ALT": 0, "ÜST": 1, "push": 0}
    equal = outcome_probabilities(151, 150, 5, 0, 3, score=150)
    assert equal["ALT"] == 0 and equal["push"] > 0
    assert equal["ÜST"] + equal["push"] == pytest.approx(1)


def test_small_training_sample_widens_predictive_uncertainty():
    small = outcome_probabilities(150, 165.5, 20, 0, 2, training_matches=2)
    large = outcome_probabilities(150, 165.5, 20, 0, 2, training_matches=200)
    assert .5 < small["ALT"] < large["ALT"] < 1


def test_training_excludes_results_that_were_unavailable_at_validation_start():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [{"forecast": forecast(), "final": 149 + index % 8,
             "captured": start + timedelta(hours=index),
             "settled": start + timedelta(hours=index + 1)} for index in range(100)]
    # The 70th observation starts validation. A slow-to-settle training match
    # cannot contribute a label learned afterwards.
    rows[0]["settled"] = start + timedelta(hours=80)
    result = fit_and_validate(rows)
    assert result["training_matches"] == 68
    assert result["validation_matches"] == 30
    assert result["validation"]["ÜST"]["accepted"] is False


def test_bad_probability_model_is_rejected_even_with_enough_matches():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [{"forecast": forecast(), "final": (149 + index % 8) if index < 70 else 180,
             "captured": start + timedelta(hours=index),
             "settled": start + timedelta(hours=index, minutes=30)} for index in range(100)]
    result = fit_and_validate(rows)
    assert result["validation"]["ALT"]["reason"] == "probability_validation_failed"
    assert result["validation"]["ALT"]["accepted"] is False


def test_production_refit_uses_known_finals_beyond_the_validation_cutoff():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [{"forecast": forecast(direction="ÜST"), "final": 149 + index % 8,
             "captured": start + timedelta(hours=index),
             "settled": start + timedelta(hours=index + 1)} for index in range(100)]
    result = fit_for_future_matches(rows)
    assert result["training_matches"] == 100
    assert result["chronological_validation"]["training_matches"] == 69
    assert result["chronological_validation"]["validation_matches"] == 30
    assert result["chronological_validation"]["validation"]["ALT"]["matches"] == 30
    assert len(result["training_data_sha256"]) == 64
    assert result["prospective_validated"] is False
    assert all(not check["accepted"] for check in result["validation"].values())


def test_legacy_reconstruction_uses_proven_snapshot_and_automatic_archive_final(tmp_path):
    import json
    from db import Database
    from tests.test_forecast_tracking import save

    database = Database(str(tmp_path / "legacy.db"))
    database.init()
    save(database, "legacy", line=170, center=999)
    alert_id = database.save_alert("legacy", "Home - Away", 160, 170, "ALT", 10,
                                  tournament="FIBA", score="30 - 30", status="Q2 05:00")
    with database._conn() as connection:
        connection.execute("UPDATE match_live_snapshots SET forecast_json=NULL")
        connection.execute("""UPDATE alerts SET final_total=150,result_source='automatic_final_score',
                           settled_at=CURRENT_TIMESTAMP WHERE id=?""", (alert_id,))
    before = database.get_match_snapshots("legacy")
    rows, excluded = read_reconstructed_observations(database.db_path)
    assert len(rows) == 1 and rows[0]["reconstructed"] is True
    assert rows[0]["forecast"]["base_predicted_total"] == 160
    assert rows[0]["final"] == 150
    assert database.get_match_snapshots("legacy") == before
    with database._conn() as connection:
        proof = json.loads(before[0]["market_provenance_json"])
        proof["source"]["rows"][0]["live"][1] = "100"
        connection.execute("UPDATE match_live_snapshots SET market_provenance_json=?",
                           (json.dumps(proof),))
    rows, excluded = read_reconstructed_observations(database.db_path)
    assert rows == [] and excluded["invalid_first_observation"] == 1


def test_read_only_audit_keeps_first_loss_and_does_not_replace_invalid_source(tmp_path):
    import json
    from db import Database
    from tests.test_forecast_tracking import save, final

    database = Database(str(tmp_path / "audit.db"))
    database.init()
    save(database, "loss", line=157.5)
    save(database, "loss", line=149)
    final(database, "loss", 150)
    invalid = save(database, "invalid", line=157.5)
    with database._conn() as connection:
        proof = json.loads(invalid["market_provenance_json"])
        proof["source"]["rows"][0]["live"][1] = "100"
        connection.execute("UPDATE match_live_snapshots SET market_provenance_json=? WHERE id=?",
                           (json.dumps(proof), invalid["id"]))
    save(database, "invalid", line=149)
    final(database, "invalid", 150)
    save(database, "pending")
    before = {name: database.get_match_snapshots(name) for name in ("loss", "invalid", "pending")}
    rows, excluded = read_observations(database.db_path)
    assert len(rows) == 1
    assert rows[0]["forecast"]["direction"] == "ÜST"
    assert rows[0]["forecast"]["line"] == 157.5 and rows[0]["final"] == 150
    assert excluded == {"invalid_first_observation": 1, "pending_first_observation": 1}
    assert before == {name: database.get_match_snapshots(name) for name in before}


def test_v10_total_and_direction_share_the_fitted_outcome_distribution():
    import math
    from config import Config
    from live_signals import forecast_live_total
    from tests.test_continuous_forecasts import payload

    result = forecast_live_total(payload(inplay_total=170.5), Config())
    expected = result["base_predicted_total"] + math.sqrt(25) * MODEL["mean_normalized_error"]
    sigma = math.sqrt(25) * MODEL["normalized_error_scale"] * math.sqrt(1 + 1 / MODEL["training_matches"])
    expected_under = (1 + math.erf((170.5 - expected) / (sigma * math.sqrt(2)))) / 2
    assert result["engine"] == "future_pace_v10"
    assert result["predicted_total"] == pytest.approx(expected)
    assert result["direction"] == result["win_probability"]["preferred_direction"] == "ALT"
    assert result["win_probability"]["probability"] == pytest.approx(expected_under)


def test_probability_never_vetoes_an_otherwise_eligible_signal(monkeypatch):
    from config import Config
    from live_signals import evaluate_live_signal
    from tests.test_continuous_forecasts import payload
    import win_probability

    model = copy.deepcopy(MODEL)
    model["mean_normalized_error"] = 0
    monkeypatch.setattr(win_probability, "MODEL", model)
    config = Config()
    config.MIN_EDGE_POINTS = config.MIN_EDGE_RATIO = 0
    config.MIN_SIGNAL_WIN_PROBABILITY = .60
    marginal = evaluate_live_signal(payload(inplay_total=161.5), [], config)
    strong = evaluate_live_signal(payload(inplay_total=180.5), [], config)
    assert marginal.direction == "ALT" and marginal.skip_reason == ""
    assert .5 < marginal.win_probability["probability"] < .6
    assert strong.direction == "ALT" and strong.win_probability["probability"] >= .6


def test_low_probability_signal_is_saved_and_delivered(monkeypatch, tmp_path):
    import asyncio
    from unittest.mock import AsyncMock
    from config import Config
    from db import Database
    from main import process_match
    from tests.test_continuous_forecasts import payload
    import win_probability

    model = copy.deepcopy(MODEL)
    model.update(mean_normalized_error=0, normalized_error_scale=10)
    monkeypatch.setattr(win_probability, "MODEL", model)
    database = Database(str(tmp_path / "all-probabilities.db"))
    database.init()
    notifier = type("Notifier", (), {"send_alert": AsyncMock(return_value={"recipient": 1})})()
    config = Config()
    config.MIN_SIGNAL_WIN_PROBABILITY = .99  # obsolete callers cannot silently filter
    asyncio.run(process_match(payload(inplay_total=170), database, notifier, config))
    alert = database.get_alert(1)
    assert alert is not None and alert["telegram_status"] == "sent"
    import json
    estimate = json.loads(alert["prediction_context_json"])["decision"]["win_probability"]
    assert .5 < estimate["probability"] < .6
    notifier.send_alert.assert_awaited_once()


def test_saved_probability_check_does_not_depend_on_current_model(monkeypatch):
    from win_probability import frozen_probability_direction
    estimate = {"under_probability": .7, "over_probability": .3,
                "push_probability": 0, "preferred_direction": "ALT"}
    monkeypatch.setitem(MODEL, "mean_normalized_error", 999)
    assert frozen_probability_direction(estimate) == "ALT"
    estimate["preferred_direction"] = "ÜST"
    with pytest.raises(ValueError):
        frozen_probability_direction(estimate)
