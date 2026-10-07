from pathlib import Path

from db import Database
from over_signal_audit import audit, compare


def test_fixed_upper_subset_keeps_losing_predictions_when_direction_changes():
    rows = [
        {"center": 190, "adjusted": 175, "line": 180, "score": 80, "final": 170,
         "threshold": 4, "continuation_source": "recent_scoring", "source_verified_at_recording": True},
        {"center": 190, "adjusted": 175, "line": 180, "score": 80, "final": 200,
         "threshold": 4, "continuation_source": "recent_scoring", "source_verified_at_recording": True},
        {"center": 190, "adjusted": 190, "line": 180, "score": 80, "final": 200,
         "threshold": 4, "continuation_source": "recent_scoring", "source_verified_at_recording": False},
    ]
    result = compare(rows)["original_upper_predictions"]
    assert result["before"]["matches"] == result["after"]["matches"] == 3
    assert result["before"]["wins"] == result["after"]["wins"] == 2
    assert result["changed_direction"] == 2
    assert result["upper_alerts_after"]["matches"] == 1
    assert compare(rows)["source_verified_original_upper"]["after"]["losses"] == 1


def test_audit_is_read_only_and_does_not_replace_invalid_first_signal(tmp_path):
    db = Database(str(tmp_path / "audit.db"))
    db.init()
    db.save_alert("m", "Home - Away", 160, 180, "ÜST", 20)
    db.save_alert("m", "Home - Away", 160, 180, "ALT", 20, signal_count=2)
    before = Path(db.db_path).read_bytes()
    result = audit(db.db_path)
    assert result["eligible"] == 0
    assert result["excluded"] == {"missing_prefix": 1}
    assert Path(db.db_path).read_bytes() == before
