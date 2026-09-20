from copy import deepcopy

import pytest

from upcoming_signals import analyze_upcoming, team_attack_average
from upcoming_history_scraper import normalize_histories


def history(team="A", scores=(50, 60, 70, 80, 90, 100, 110)):
    return [{"match_id": f"{team}-{index}", "date": f"2026-01-{index + 1:02}",
             "status": "FT", "home_team": "Other" if index % 2 else team,
             "away_team": team if index % 2 else "Other",
             "home_score": 77 if index % 2 else points,
             "away_score": points if index % 2 else 77}
            for index, points in enumerate(scores)]


def match(opening=160):
    return {"match_id": "upcoming", "home_team": "A", "away_team": "B",
            "match_name": "A - B", "kickoff": "2026-02-01 12:00", "opening_total": opening}


def test_disjoint_extremes_use_own_points_in_both_venues():
    result = team_attack_average("A", history(), before="2026-02-01")
    assert result["average"] == 78
    assert [row["points"] for row in result["selected_matches"]] == [50, 60, 70, 100, 110]


@pytest.mark.parametrize("opening,signal,edge,status", [
    (160, "ALT", -4, "ready"), (150, "ÜST", 6, "ready"), (156, None, 0, "equal"),
    (None, None, None, "missing_opening"), (float("nan"), None, None, "missing_opening"),
])
def test_direction_and_edge(opening, signal, edge, status):
    result = analyze_upcoming({**match(opening), "prematch_total": 200},
                              {"home": history(), "away": history("B")})
    assert result["estimated_total"] == 156
    assert (result["signal"], result["edge"], result["status"]) == (signal, edge, status)


def test_equal_scores_are_distinct_games_and_duplicates_do_not_fill_sample():
    rows = history(scores=[80] * 5)
    result = team_attack_average("A", rows + rows, before="2026-02-01")
    assert len({row["match_id"] for row in result["selected_matches"]}) == 5
    assert result["average"] == 80
    result = team_attack_average("A", rows[:4] * 3, before="2026-02-01")
    assert result["average"] is None


def test_invalid_future_live_and_current_games_are_excluded():
    rows = history(scores=[80] * 5)
    rows += [{**rows[0], "match_id": "live", "status": "Q4", "home_score": 500},
             {**rows[0], "match_id": "future", "date": "2099-01-01"},
             {**rows[0], "match_id": "same-day", "date": "2026-02-01"},
             {**rows[0], "match_id": "unknown-team", "home_team": "A Juniors"},
             {**rows[0], "match_id": "bad", "home_score": None},
             {**rows[0], "match_id": "current", "home_score": 500}]
    result = team_attack_average("A", rows, before="2026-02-01", current_match_id="current")
    assert result["sample_count"] == 5
    assert result["average"] == 80


def test_latest_ten_are_selected_before_ranking():
    result = team_attack_average("A", history(scores=[1, 999] + [80] * 10), before="2026-02-01")
    assert result["sample_count"] == 10
    assert result["average"] == 80


def test_same_day_final_requires_a_verified_earlier_timestamp():
    rows = history(scores=[80] * 4)
    rows.append({**rows[0], "match_id": "early", "date": "2026-02-01",
                 "played_at": "2026-02-01T10:00:00+03:00"})
    assert team_attack_average("A", rows, before="2026-02-01 18:00")["average"] == 80
    rows[-1]["played_at"] = "2026-02-01T20:00:00+03:00"
    assert team_attack_average("A", rows, before="2026-02-01 18:00")["average"] is None


def test_insufficient_one_team_prevents_signal_and_inputs_are_unchanged():
    histories = {"home": history(), "away": history("B")[:4]}
    original = deepcopy(histories)
    result = analyze_upcoming(match(), histories)
    assert result["estimated_total"] is None
    assert result["signal"] is None
    assert histories == original


def test_decimal_comparison_has_no_false_signal_at_equality():
    result = analyze_upcoming(match(160.4), {"home": history(scores=[80, 80, 80, 80, 81]),
                                            "away": history("B", scores=[80, 80, 80, 80, 81])})
    assert result["edge"] == 0
    assert result["status"] == "equal"


def test_embedded_source_checks_identity_status_and_sums_quarters_with_overtime():
    row = {"id": "past", "sportId": 2, "statusId": 10, "matchTime": 1767268800,
           "homeTeam": {"id": "b"}, "awayTeam": {"id": "a"},
           "homeScores": [20, 20, 20, 20, 10], "awayScores": [19, 20, 20, 21, 12]}
    payload = {"match_id": "upcoming", "home_team": {"id": "a", "name": "A"},
               "away_team": {"id": "b", "name": "B"},
               "teams": {"a": {"name": "A"}, "b": {"name": "B"}},
               "home": [row, {**row, "statusId": 8}, {**row, "homeScores": [90]}], "away": []}
    result = normalize_histories(payload, match(), "Europe/Istanbul")
    assert len(result["home"]) == 1
    assert result["home"][0]["away_score"] == 92
    assert result["home"][0]["home_score"] == 90
    with pytest.raises(ValueError):
        normalize_histories({**payload, "match_id": "wrong"}, match(), "UTC")
