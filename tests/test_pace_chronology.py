from config import Config
from pace_calculator import chronological_snapshots, get_future_paces, get_future_pace_windows


def test_recorded_future_anchor_cannot_enter_signal_time_pace():
    state = {"elapsed_game_seconds": 900, "total_score": 65, "period": 2,
             "observed_at": "2026-10-04T12:00:00+00:00"}
    past = {"elapsed_game_seconds": 780, "total_score": 40, "period": 2,
            "recorded_at": "2026-10-04 11:59:00"}
    future = {"elapsed_game_seconds": 600, "total_score": 20, "period": 2,
              "recorded_at": "2026-10-04 12:01:00"}
    assert get_future_paces([future, past], state, 4.0, Config()) == get_future_paces([past], state, 4.0, Config())


def test_no_eligible_history_cannot_manufacture_a_pace_window():
    state = {"elapsed_game_seconds": 900, "total_score": 65, "period": 2}
    future = {"elapsed_game_seconds": 960, "total_score": 70, "period": 2}
    assert get_future_paces([future], state, 4.0, Config()) == []


def test_pace_windows_ignore_regressed_and_future_snapshots():
    clean = [
        {"elapsed_game_seconds": 600, "total_score": 40, "period": 2},
        {"elapsed_game_seconds": 720, "total_score": 50, "period": 2},
        {"elapsed_game_seconds": 840, "total_score": 60, "period": 2},
    ]
    dirty = [
        clean[0], clean[1],
        {"elapsed_game_seconds": 700, "total_score": 55, "period": 2},
        clean[2],
        {"elapsed_game_seconds": 870, "total_score": 59, "period": 2},
        {"elapsed_game_seconds": 960, "total_score": 70, "period": 2},
    ]
    state = {"elapsed_game_seconds": 900, "total_score": 65, "period": 2}

    assert chronological_snapshots(dirty, state) == clean
    assert get_future_paces(dirty, state, 4.0, Config()) == get_future_paces(clean, state, 4.0, Config())


def test_confirmed_score_correction_starts_new_pace_segment():
    history = [
        {"elapsed_game_seconds": 780, "total_score": 70, "period": 2},
        {"elapsed_game_seconds": 840, "total_score": 80, "period": 2},
        {"elapsed_game_seconds": 870, "total_score": 75, "period": 2},
        {"elapsed_game_seconds": 900, "total_score": 76, "period": 2},
        {"elapsed_game_seconds": 960, "total_score": 82, "period": 2},
    ]
    state = {"elapsed_game_seconds": 960, "total_score": 82, "period": 2}
    assert chronological_snapshots(history, state) == history[2:]


def test_single_stale_score_dip_is_not_treated_as_correction():
    history = [
        {"elapsed_game_seconds": 780, "total_score": 70, "period": 2},
        {"elapsed_game_seconds": 840, "total_score": 80, "period": 2},
        {"elapsed_game_seconds": 870, "total_score": 75, "period": 2},
        {"elapsed_game_seconds": 900, "total_score": 82, "period": 2},
    ]
    state = {"elapsed_game_seconds": 900, "total_score": 82, "period": 2}
    assert chronological_snapshots(history, state) == [history[0], history[1], history[3]]


def test_same_interval_with_two_labels_counts_once():
    state = {"elapsed_game_seconds": 900, "total_score": 75, "period": 2}
    rows = [{"elapsed_game_seconds": 780, "total_score": 60, "period": 2}]
    windows = get_future_pace_windows(rows, state, 4, Config())
    assert len(windows) == 2
    assert windows[1]['labels'] == ['recent_2m', 'observed_period_segment']
    assert windows[1]['start_second'] == 780
    assert len(get_future_paces(rows, state, 4, Config())) == 2


def test_whole_game_and_period_from_tipoff_are_one_interval():
    rows = [{"elapsed_game_seconds": 0, "total_score": 0, "period": 1}]
    state = {"elapsed_game_seconds": 720, "total_score": 70, "period": 1}
    windows = get_future_pace_windows(rows, state, 4, Config())
    assert len(windows) == 1
    assert windows[0]['labels'] == ['whole_game', 'observed_period_segment']


def test_equal_rates_from_different_intervals_remain_distinct():
    rows = [{"elapsed_game_seconds": 900, "total_score": 60, "period": 2},
            {"elapsed_game_seconds": 1080, "total_score": 72, "period": 2}]
    state = {"elapsed_game_seconds": 1200, "total_score": 80, "period": 3}
    windows = get_future_pace_windows(rows, state, 4, Config())
    assert len(windows) == 3
    assert get_future_paces(rows, state, 4, Config()) == [4, 4, 4]
