import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from config import Config
from live_signals import evaluate_live_signal, forecast_live_total


def make_match(pregame, live, score, status, tournament="FIBA"):
    return {
        "match_id": "m", "match_name": "A - B", "tournament": tournament,
        "opening_total": pregame, "prematch_total": pregame,
        "inplay_total": live, "score": score, "status": status,
    }


@pytest.mark.parametrize("live,score,anchors,direction,reason,center", [
    (120, 80, [(900, 60), (1080, 72)], "ÜST", "", 160),
    (200, 80, [(900, 60), (1080, 72)], "ALT", "", 160),
    (160, 80, [(900, 60), (1080, 72)], "PAS", "PAS_NO_EDGE", 160),
    (184, 100, [(1080, 82)], "ALT", "", 179.8),
    (136, 60, [(1080, 58)], "ALT", "", 126.7),
    (220, 140, [(900, 135), (1080, 136)], "PAS", "PAS_NO_EDGE", 220),
    (200, 80, [(1200, 80)], "ALT", "", 160),
])
def test_real_history_scenarios(live, score, anchors, direction, reason, center):
    payload = make_match(160, live, f"{score // 2} - {score - score // 2}", "Q3 10:00")
    captured = datetime.now(timezone.utc)
    payload["market_captured_at"] = captured.isoformat()
    snapshots = [{"elapsed_game_seconds": elapsed, "total_score": points,
                  "recorded_at": (captured - timedelta(seconds=1200 - elapsed)).isoformat(),
                  "period": 2 if elapsed < 1200 else 3}
                 for elapsed, points in anchors]
    decision = evaluate_live_signal(payload, snapshots, Config())
    assert decision.direction == direction
    assert decision.skip_reason == reason
    assert decision.sustainable_projection_center == center


def test_overlapping_windows_are_diagnostics_and_cannot_multiply_the_score():
    payload = make_match(160, 168, "45 - 40", "Q2 05:00")
    with patch("live_signals.get_future_paces", return_value=[3.0, 6.0]):
        divergent = evaluate_live_signal(payload, [], Config())
    with patch("live_signals.get_future_paces", return_value=[4.2] * 4):
        repeated = evaluate_live_signal(payload, [], Config())
    assert divergent.direction == repeated.direction == "ÜST"
    assert divergent.sustainable_projection_center == repeated.sustainable_projection_center == 185
    assert divergent.edge_points == repeated.edge_points == 17


def test_advantage_below_publication_threshold_keeps_a_visible_forecast():
    payload = make_match(160, 157.5, "30 - 30", "Q2 05:00")
    forecast = forecast_live_total(payload, Config())
    decision = evaluate_live_signal(payload, [], Config())
    assert forecast['predicted_total'] == 160
    assert forecast['direction'] == 'ÜST'
    assert decision.direction == 'PAS' and decision.skip_reason == 'PAS_NO_EDGE'
    assert decision.sustainable_projection_center == 160


def test_blowout_requires_more_advantage_without_changing_the_forecast():
    # Without continuation evidence, the remaining 20m use the 4 PPM prior.
    payload = make_match(160, 184, "75 - 35", "Q3 10:00")
    balanced = {**payload, 'score': '55 - 55'}
    assert forecast_live_total(payload, Config()) == forecast_live_total(balanced, Config())
    assert evaluate_live_signal(balanced, [], Config()).direction == 'ÜST'
    assert evaluate_live_signal(payload, [], Config()).skip_reason == 'PAS_NO_EDGE'


def test_nba_regulation_duration():
    from match_state import game_clock
    clock = game_clock("Q2 05:00", "A - B", "NBA")
    assert clock["quarter_length"] == 12
    assert clock["period_count"] == 4


@pytest.fixture(autouse=True)
def isolate_pace_math_from_final_error_bias(monkeypatch):
    # These legacy cases verify the pace stage and its source/history rules.
    # The separate v10 distribution tests cover the fitted error correction
    # and the default probability publication floor.
    from win_probability import MODEL
    monkeypatch.setitem(MODEL, "mean_normalized_error", 0)
    monkeypatch.setattr(Config, "MIN_SIGNAL_WIN_PROBABILITY", .5)
