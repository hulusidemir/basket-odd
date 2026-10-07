import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from config import Config
from db import Database
from directional_audit import alert_threshold, calibration_experiment
from forecast_tracking import signal_outcome, signal_tracking_summary, tracking_summary
from tests.test_forecast_tracking import save, final


@pytest.fixture
def database(tmp_path):
    db = Database(str(tmp_path / "directional.db"))
    db.init()
    return db


def test_direction_change_does_not_move_a_loss_to_the_later_direction(database):
    save(database, match_id="changes", line=157.5, center=160)
    save(database, match_id="changes", line=157.5, center=150)
    save(database, match_id="under", line=157.5, center=150)
    final(database, "changes", 150)
    final(database, "under", 150)
    summary = tracking_summary(database.forecast_tracking_data())
    assert summary["by_direction"]["ÜST"]["losses"] == 1
    assert summary["by_direction"]["ALT"]["wins"] == 1
    assert sum(group["matches"] for group in summary["by_direction"].values()) == summary["matches"] == 2


def signal_row(**changes):
    at = datetime(2026, 10, 1, tzinfo=timezone.utc)
    return {"alerted_at": at.isoformat(), "direction": "ÜST", "live": 160,
            "result_source": "automatic_final_score", "settled_at": (at + timedelta(hours=1)).isoformat(),
            "final_total": 150, **changes}


def test_signal_results_require_automatic_final_and_valid_chronology():
    row = signal_row()
    assert signal_outcome(row) == "loss"
    assert signal_outcome({**row, "direction": "ALT"}) == "win"
    assert signal_outcome({**row, "final_total": 160}) == "push"
    assert signal_outcome({**row, "result_source": "manual"}) == "pending"
    assert signal_outcome({**row, "settled_at": "2026-09-30"}) == "invalid"
    assert signal_outcome(row, as_of=datetime(2026, 10, 1, tzinfo=timezone.utc)) == "pending"


@pytest.mark.parametrize("total", [True, None, float("nan"), float("inf"), -1])
def test_signal_bad_final_is_not_success(total):
    assert signal_outcome(signal_row(final_total=total)) == "invalid"


def test_signal_summary_empty_direction_has_no_fake_percentage():
    summary = signal_tracking_summary([signal_row()])
    assert summary["by_direction"]["ALT"]["success_rate"] is None
    assert summary["by_direction"]["ÜST"]["success_rate"] == 0
    assert summary["losses"] == 1


def test_signal_api_uses_first_row_before_direction_and_outcome_without_writes(database):
    first = database.save_alert("same", "Home - Away", 160, 160, "ÜST", 0)
    database.save_alert("same", "Home - Away", 160, 160, "ALT", 0, signal_count=2)
    with database._conn() as conn:
        conn.execute("UPDATE alerts SET final_total=150,result_source='automatic_final_score',settled_at=CURRENT_TIMESTAMP WHERE match_id='same'")
    before = database.get_alert(first)
    import dashboard
    with patch.object(dashboard, "db", database):
        client = dashboard.app.test_client()
        summary = client.get("/api/signals/performance").get_json()
        assert summary["matches"] == 1 and summary["by_direction"]["ÜST"]["losses"] == 1
        assert summary["by_direction"]["ALT"]["matches"] == 0
        assert summary["model_scope"] == "mixed_recorded_policies"
        assert "match_id" not in json.dumps(summary) and "match_name" not in json.dumps(summary)
        assert client.post("/api/signals/performance", json={"result": "win"}).status_code == 405
    assert database.get_alert(first) == before


def experiment_rows():
    at = datetime(2026, 9, 1, tzinfo=timezone.utc)
    return [{"match_id": str(i), "at": at + timedelta(days=i),
             "label_at": at + timedelta(days=i, hours=2), "score": 60,
             "center": 180, "line": 170, "final": 160, "threshold": 4,
             "source_verified_at_recording": i % 2 == 0} for i in range(10)]


def test_calibration_fit_does_not_read_holdout_labels_and_reports_lost_coverage():
    rows = experiment_rows()
    result = calibration_experiment(rows)
    assert result["over_correction_points"] == 20
    assert result["training_labels_before_test"] and result["training_test_match_overlap"] == 0
    assert result["holdout"]["upper_alerts_before"]["matches"] == 4
    assert result["holdout"]["upper_alerts_with_correction_margin"]["matches"] == 0
    assert result["holdout"]["upper_predictions_bias_corrected"]["ALT"] == 4
    for row in rows[6:]:
        row["final"] = 300
    changed = calibration_experiment(rows)
    assert changed["over_correction_points"] == result["over_correction_points"]
    assert changed["holdout"]["upper_predictions_bias_corrected"]["wins"] == 0


def test_training_label_not_observed_before_cutoff_is_excluded():
    rows = experiment_rows()
    rows[0]["label_at"] = rows[7]["at"]
    rows[0]["final"] = 1000
    result = calibration_experiment(rows)
    assert result["training"] == 5 and result["over_correction_points"] == 20


def test_replay_threshold_retains_blowout_and_repricing_rules():
    state = {"elapsed": 20, "remaining": 20, "duration": 40, "score": 80,
             "margin": 30 / 80, "line": 160, "prior": 4}
    config = Config()
    assert alert_threshold(state, config) == config.MIN_EDGE_POINTS * config.EXTREME_BLOWOUT_EDGE_MULTIPLIER
    assert alert_threshold({**state, "elapsed": 2}, config) == float("inf")
