from config import Config
from pace_calculator import chronological_snapshots, get_future_paces


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
