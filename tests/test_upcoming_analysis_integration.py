import asyncio
from unittest.mock import AsyncMock, patch

from db import Database
from upcoming_scraper import UpcomingScraper


def test_analysis_persists_without_schema_or_live_alert_changes(tmp_path):
    db = Database(str(tmp_path / "existing.db"))
    db.init()
    alert_id = db.save_alert("live", "C - D", 160, 175, "ALT", 15)
    row = {"match_id": "future", "match_name": "A - B", "home_team": "A", "away_team": "B",
           "opening_total": 160, "kickoff": "", "upcoming_analysis": {
               "home": {"average": 75}, "away": {"average": 80},
               "estimated_total": 155, "edge": -5, "signal": "ALT", "status": "ready"}}
    db.save_upcoming_matches([row])
    db.set_upcoming_match_statuses("future", followed=True)
    reopened = Database(db.db_path)
    reopened.init()
    saved = reopened.list_upcoming_matches()[0]
    assert saved["upcoming_analysis"] == row["upcoming_analysis"]
    assert saved["followed"] == 1
    reopened.clear_upcoming_matches()
    with reopened._conn() as conn:
        assert conn.execute("SELECT id FROM alerts").fetchall()[0][0] == alert_id
        assert conn.execute("SELECT count(*) FROM alerts").fetchone()[0] == 1


def test_failed_history_keeps_schedule_and_opening():
    scraper = UpcomingScraper()
    row = {"match_id": "future", "match_name": "A - B", "opening_total": 160}
    with patch.object(scraper, "_extract_one", new=AsyncMock(return_value=row)), patch(
        "upcoming_scraper.fetch_team_histories", new=AsyncMock(side_effect=TimeoutError)
    ):
        result = asyncio.run(scraper._extract_one_with_timeout(object(), "https://m.aiscore.com/basketball/match-a-b/future"))
    assert result["opening_total"] == 160
    assert result["upcoming_analysis"]["status"] == "history_unavailable"
    assert result["upcoming_analysis"]["signal"] is None


def test_analysis_is_attached_to_fetch_result():
    scraper = UpcomingScraper()
    row = {"match_id": "future", "match_name": "A - B", "opening_total": 160}
    with patch.object(scraper, "_extract_one", new=AsyncMock(return_value=row)), patch(
        "upcoming_scraper.fetch_team_histories", new=AsyncMock(return_value={"home": [], "away": []})
    ):
        result = asyncio.run(scraper._extract_one_with_timeout(object(), "ignored"))
    assert result["upcoming_analysis"]["status"] == "insufficient_history"
    assert result["upcoming_analysis"]["edge"] is None
