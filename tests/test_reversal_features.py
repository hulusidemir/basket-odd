"""Signal-time collection tests; no direction or Quality v1 rule changes."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from config import Config
from db import Database
from finished_match_service import _empty_result_summary, _settle_deleted_match_from_final_score
from live_signals import SignalDecision, evaluate_live_signal
from main import process_match
from match_state import confirmed_12_minute_quarters
from reversal_features import collect_reversal_features


AS_OF = datetime(2026, 10, 2, 12, tzinfo=timezone.utc)


def match(**changes):
    value = {
        "match_id": "m", "match_name": "Home - Away", "tournament": "League",
        "status": "Q2 05:00", "score": "35 - 35", "opening_total": 160,
        "prematch_total": 170, "inplay_total": 175, "url": "", "quarter_scores": {},
    }
    value.update(changes)
    return value


def decision():
    return SignalDecision("prematch", 170, 5, 0, "ALT", 2,
                          sustainable_projection_center=170)


def snapshot(elapsed, score, period, clock, *, recorded_minutes_ago=3):
    return {
        "elapsed_game_seconds": elapsed, "total_score": score, "period": period,
        "game_clock": clock,
        "recorded_at": (AS_OF - timedelta(minutes=recorded_minutes_ago)).strftime("%Y-%m-%d %H:%M:%S"),
    }


def history(previous_score=50):
    return [
        snapshot(510, previous_score, 1, "01:30", recorded_minutes_ago=7),
        snapshot(750, 60, 2, "07:30", recorded_minutes_ago=3),
        snapshot(900, 70, 2, "05:00", recorded_minutes_ago=0),
    ]


def features(payload=None, snapshots=None):
    return collect_reversal_features(payload or match(), snapshots if snapshots is not None else history(),
                                     decision(), as_of=AS_OF)


def test_prematch_prior_market_moves_and_disjoint_acceleration_windows():
    result = features(match(market_captured_at="2026-10-02T11:59:50Z"))
    assert result["pregame_source"] == "prematch"
    assert result["pregame_ppm"] == pytest.approx(170 / 40)
    assert result["current_ppm"] == pytest.approx(70 / 15)
    assert result["current_vs_pregame_ppm"] == pytest.approx(70 / 15 - 170 / 40)
    assert result["opening_to_prematch_move"] == 10
    assert result["prematch_to_live_move"] == 5
    assert result["opening_to_live_move"] == 15
    assert result["recent_window_minutes"] == 2.5
    assert result["previous_window_minutes"] == 4
    assert result["recent_window_points"] == 10
    assert result["previous_window_points"] == 10
    assert result["recent_ppm"] == 4
    assert result["previous_ppm"] == 2.5
    assert result["pace_delta"] == 1.5
    assert result["pace_delta_pct"] == pytest.approx(0.6)
    assert result["reversal_feature_status"] == "FULL"
    assert result["recent_snapshot_count"] == 2
    assert result["previous_snapshot_count"] == 2
    assert result["required_ppm"] == pytest.approx(105 / 25)
    assert result["fair_edge"] == -5
    assert result["projection"] == pytest.approx((70 / 15) * 40)
    assert result["elapsed_fraction"] == pytest.approx(15 / 40)
    assert result["market_captured_at"] == "2026-10-02T11:59:50+00:00"
    assert result["feature_frozen_at"] == AS_OF.isoformat()


def test_opening_fallback_deceleration_and_missing_prematch_move():
    result = features(match(prematch_total=None), history(previous_score=40))
    assert result["pregame_source"] == "opening"
    assert result["pregame_ppm"] == 4
    assert result["opening_to_prematch_move"] is None
    assert result["prematch_to_live_move"] is None
    assert result["opening_to_live_move"] == 15
    assert result["previous_ppm"] == 5
    assert result["recent_ppm"] == 4
    assert result["pace_delta"] == -1


def test_missing_history_never_becomes_zero():
    recent_only = features(snapshots=history()[1:])
    assert recent_only["reversal_feature_status"] == "RECENT_ONLY"
    assert recent_only["recent_ppm"] == 4
    for field in ("previous_ppm", "previous_window_minutes", "pace_delta", "pace_delta_pct"):
        assert recent_only[field] is None
    insufficient = features(snapshots=history()[-1:])
    assert insufficient["reversal_feature_status"] == "INSUFFICIENT_HISTORY"
    assert insufficient["recent_ppm"] is None
    assert insufficient["pace_delta"] is None


def test_stale_future_regressed_and_wrong_format_snapshots_are_excluded():
    rows = history()
    rows[0] = snapshot(510, 50, 1, "01:30", recorded_minutes_ago=30)
    rows.insert(2, snapshot(700, 65, 2, "08:20", recorded_minutes_ago=1))
    rows.append(snapshot(660, 40, 2, "09:00", recorded_minutes_ago=-1))
    result = features(snapshots=rows)
    assert result["reversal_feature_status"] == "RECENT_ONLY"
    assert result["recent_ppm"] == 4
    assert result["previous_ppm"] is None


def test_confirmed_score_correction_discards_earlier_segment():
    rows = history()[:2]
    rows.extend([
        snapshot(770, 55, 2, "07:10", recorded_minutes_ago=2),
        snapshot(800, 56, 2, "06:40", recorded_minutes_ago=1),
        history()[-1],
    ])
    result = features(snapshots=rows)
    assert result["reversal_feature_status"] == "RECENT_ONLY"
    assert result["previous_ppm"] is None


def test_48_minute_format_excludes_old_40_minute_anchor():
    payload = match(status="Q2 07:00", score="40 - 40")
    rows = [
        snapshot(630, 50, 1, "01:30", recorded_minutes_ago=7),
        snapshot(780, 55, 2, "07:00", recorded_minutes_ago=5),  # 40-minute mapping
        snapshot(870, 68, 2, "09:30", recorded_minutes_ago=3),
        snapshot(1020, 80, 2, "07:00", recorded_minutes_ago=0),
    ]
    with confirmed_12_minute_quarters(True):
        result = features(payload, rows)
    assert result["regulation_minutes"] == 48
    assert result["elapsed_minutes"] == 17
    assert result["reversal_feature_status"] == "FULL"
    assert result["trusted_snapshot_count"] == 3
    assert result["recent_ppm"] == pytest.approx(12 / 2.5)
    assert result["previous_ppm"] == pytest.approx(18 / 4)


def test_final_fields_and_future_snapshots_cannot_enter_features_or_v5():
    payload = match()
    with patch("live_signals.get_future_paces", return_value=[4.5, 4.6]):
        raw_before = evaluate_live_signal(payload, history(), Config())
        before = collect_reversal_features(payload, history(), raw_before, as_of=AS_OF)
        later = snapshot(1050, 99, 2, "02:30", recorded_minutes_ago=-1)
        raw_after = evaluate_live_signal(payload, history(), Config())
        after = collect_reversal_features({**payload, "final_total": 220, "result": "Başarısız"},
                                          history() + [later], raw_after, as_of=AS_OF)
    assert raw_before == raw_after
    assert before == after
    assert "final_total" not in before and "result" not in before


def test_new_signal_is_frozen_and_legacy_row_stays_null(tmp_path):
    import dashboard

    database = Database(str(tmp_path / "features.db"))
    database.init()
    legacy_id = database.save_alert("old", "A - B", 160, 170, "ALT", 10)
    notifier = type("Notifier", (), {"send_alert": AsyncMock(return_value={"chat": 1})})()
    payload = match()
    from tests.market_fixture import verified_payload
    config = Config()
    config.MIN_SIGNAL_QUALITY = 0
    for row in history()[:2]:
        database.save_snapshot_if_changed(
            "m", row["period"], row["game_clock"], row["elapsed_game_seconds"], 25,
            row["total_score"] // 2, row["total_score"] - row["total_score"] // 2,
            row["total_score"], 170, 175, heartbeat_seconds=0,
            market_provenance=verified_payload({**payload,
                "score": f'{row["total_score"] // 2} - {row["total_score"] - row["total_score"] // 2}',
                "status": f'Q{row["period"]} {row["game_clock"]}',
            })["market_provenance"],
        )
    with patch("main.evaluate_live_signal", return_value=decision()):
        asyncio.run(process_match(verified_payload(payload), database, notifier, config))
    row = database.latest_match_alert_in_direction("m", "ALT")
    assert row["reversal_features_version"] == "v1"
    frozen = row["reversal_features_json"]
    assert json.loads(frozen)["reversal_feature_status"] == "FULL"
    assert database.get_alert(legacy_id)["reversal_features_json"] is None
    database.archive_match_with_display_snapshots("m", {row["id"]: {"id": row["id"]}})
    summary = _empty_result_summary(tracked_count=1)
    assert _settle_deleted_match_from_final_score(database, summary, "m", "90 - 90", "Finished")
    database = Database(database.db_path)
    database.init()
    assert database.get_deleted_alert_by_id(row["id"])["reversal_features_json"] == frozen
    assert database.get_deleted_alert_by_id(row["id"])["reversal_features_version"] == "v1"
    assert database.get_alert(legacy_id)["reversal_features_json"] is None
    archived = database.get_deleted_alert_by_id(row["id"])
    archived["display_snapshot"] = json.dumps({"reversal_features_json": "tampered"})
    assert dashboard._frozen_deleted_alert(archived)["reversal_features_json"] == frozen


def test_additive_migration_can_run_twice_on_existing_database(tmp_path):
    database = Database(str(tmp_path / "legacy.db"))
    database.init()
    alert_id = database.save_alert("old", "A - B", 160, 170, "ALT", 10)
    with database._conn() as conn:
        conn.execute("ALTER TABLE alerts DROP COLUMN reversal_features_version")
        conn.execute("ALTER TABLE alerts DROP COLUMN reversal_features_json")
    database.init()
    database.init()
    assert database.get_alert(alert_id)["reversal_features_version"] is None
    assert database.get_alert(alert_id)["reversal_features_json"] is None
