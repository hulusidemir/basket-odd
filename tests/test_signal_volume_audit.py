import copy
import json
from zoneinfo import ZoneInfo

from db import Database
from forward_validation import _timestamp
from signal_volume_audit import replay_day
from tests.test_forecast_tracking import save
import win_probability


def test_read_only_volume_replay_applies_optional_floor_and_real_repeat_rules(monkeypatch, tmp_path):
    model = copy.deepcopy(win_probability.MODEL)
    model.update(mean_normalized_error=0, normalized_error_scale=10)
    monkeypatch.setattr(win_probability, "MODEL", model)
    database = Database(str(tmp_path / "volume.db"))
    database.init()
    first = save(database, "m", line=170)
    save(database, "m", line=171)
    invalid = save(database, "invalid", line=170)
    with database._conn() as connection:
        proof = json.loads(invalid["market_provenance_json"])
        proof["source"]["rows"][0]["live"][1] = "100"
        connection.execute("UPDATE match_live_snapshots SET market_provenance_json=? WHERE id=?",
                           (json.dumps(proof), invalid["id"]))
    before = {name: database.get_match_snapshots(name) for name in ("m", "invalid")}
    context = json.loads(first["forecast_json"])
    day = _timestamp(context["market_captured_at"]).astimezone(ZoneInfo("Europe/Istanbul")).date()
    report = replay_day(database.db_path, day.isoformat())
    all_signals, above_sixty = report["thresholds"]
    assert report["saved_observations"] == 3
    assert report["excluded_observations"]["invalid_source"] == 1
    assert all_signals["signals"] == all_signals["matches"] == 1
    assert above_sixty["signals"] == above_sixty["matches"] == 0
    assert database.count_match_alerts("m") == 0
    assert before == {name: database.get_match_snapshots(name) for name in before}
