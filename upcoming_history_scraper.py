"""Read the two independent team histories embedded in AiScore's mobile H2H page."""

import asyncio
from datetime import datetime
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from upcoming_signals import normalize_name


HISTORY_TIMEOUT_SECONDS = 40

# This is the data backing the home/away tabs, not the h2hDatas_h2h matchup list.
# Return only public sports fields, never the rest of the page/application state.
HISTORY_STATE_JS = r"""() => {
    const candidates = [window.$nuxt?.$store?.state?.basketball, window.__NUXT__?.state?.basketball];
    const state = candidates.find(state => state?.h2hMaps?.homeTeam?.id);
    const maps = state?.h2hMaps;
    if (!maps) return null;
    const rows = key => (Array.isArray(maps[key]) ? maps[key] : []).map(row => ({
        id:row.id, sportId:row.sportId, statusId:row.statusId, matchTime:row.matchTime,
        homeTeam:row.homeTeam, awayTeam:row.awayTeam,
        homeScores:row.homeScores, awayScores:row.awayScores
    }));
    return {match_id:state.detailMatchId, home_team:maps.homeTeam, away_team:maps.awayTeam,
        teams:maps.teamsMap, home:rows('h2hDatas_home'), away:rows('h2hDatas_away')};
}"""


async def read_history_state(page, timeout_seconds: float = 8) -> dict:
    """Camoufox's isolated world cannot read site JavaScript globals."""
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        payload = await page.evaluate("mw:(" + HISTORY_STATE_JS + ")()")
        if payload and (payload.get("home_team") or {}).get("id") and (payload.get("away_team") or {}).get("id"):
            return payload
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("Team history state did not become ready")
        await asyncio.sleep(0.25)


def normalize_histories(payload: dict | None, match: dict, timezone_id: str) -> dict:
    if not isinstance(payload, dict) or str(payload.get("match_id")) != str(match.get("match_id")):
        raise ValueError("History page does not match the requested game")
    teams = payload.get("teams") or {}
    result = {"home": [], "away": []}
    for side in result:
        expected = payload.get(f"{side}_team") or {}
        if (not expected.get("id") or normalize_name(expected.get("name") or "")
                != normalize_name(match.get(f"{side}_team") or "")):
            raise ValueError("History team identity could not be verified")
        for row in payload.get(side) or []:
            if row.get("sportId") != 2 or row.get("statusId") not in (10, 105):
                continue
            home_id = str((row.get("homeTeam") or {}).get("id") or "")
            away_id = str((row.get("awayTeam") or {}).get("id") or "")
            if str(expected["id"]) not in (home_id, away_id):
                continue
            scores = [row.get("homeScores"), row.get("awayScores")]
            # Mobile source: Q1, Q2, Q3, Q4, combined overtime points.
            if any(not isinstance(score, list) or len(score) != 5
                   or any(type(value) is not int or value < 0 for value in score) for score in scores):
                continue
            try:
                played = datetime.fromtimestamp(float(row["matchTime"]), ZoneInfo(timezone_id))
            except (KeyError, ValueError, TypeError, OverflowError, OSError):
                continue
            home = teams.get(home_id) or {}
            away = teams.get(away_id) or {}
            if not home.get("name") or not away.get("name"):
                continue
            result[side].append({"match_id": str(row.get("id") or ""),
                                 "date": played.date().isoformat(), "played_at": played.isoformat(),
                                 "status": "FT",
                                 "home_team": home["name"], "away_team": away["name"],
                                 "home_score": sum(scores[0]), "away_score": sum(scores[1])})
    return result


async def fetch_team_histories(context, match: dict, timezone_id: str) -> dict:
    page = await context.new_page()
    try:
        path = urlsplit(match.get("url") or "").path.rstrip("/")
        if not path.startswith("/basketball/match-"):
            raise ValueError("Invalid match path")
        if path.rsplit("/", 1)[-1] in ("h2h", "odds", "summary"):
            path = path.rsplit("/", 1)[0]
        response = await page.goto("https://m.aiscore.com" + path + "/h2h",
                                   wait_until="domcontentloaded", timeout=25000)
        if response and response.status >= 400:
            raise ValueError("History source is unavailable")
        return normalize_histories(await read_history_state(page), match, timezone_id)
    finally:
        if not page.is_closed():
            await page.close()
