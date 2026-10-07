"""Read public basketball analysis data without changing the signal engine.

The reader uses the existing page and performs no navigation or DB writes.
Availability and score reconciliation are explicit: missing box scores are
never replaced with zero attempts or inferred from rounded percentages.
"""

import asyncio
import re


BASKETBALL_IDENTITY_JS = r"""expectedId => {
    const state = window.$nuxt?.$store?.state?.basketball
        || window.__NUXT__?.state?.basketball;
    const match = state?.basketballDetailMatchData?.match;
    const path = location.pathname.replace(/\/(odds|stats|boxscore|h2h|summary)\/?$/, '');
    return {
        source_match_id: !!match && String(match.id) === expectedId,
        detail_match_id: String(state?.detailMatchId || '') === expectedId,
        url_match_id: path.split('/').filter(Boolean).pop() === expectedId,
    };
}"""

BASKETBALL_READY_JS = "id => Object.values((" + BASKETBALL_IDENTITY_JS + ")(id)).every(Boolean)"

BASKETBALL_DATA_JS = r"""expectedId => {
    const state = window.$nuxt?.$store?.state?.basketball
        || window.__NUXT__?.state?.basketball;
    const match = state?.basketballDetailMatchData?.match;
    const identityChecks = (""" + BASKETBALL_IDENTITY_JS + r""")(expectedId);
    if (!Object.values(identityChecks).every(Boolean)) {
        return {error: 'match_identity_mismatch', identity_checks: identityChecks};
    }
    const lineup = state.lineupData?.lineup || state._boxscoreData?.lineup || {};
    const team = side => lineup[side + 'PlayerTotals']?.bkDetail || null;
    const events = [];
    for (const [index, period] of (state._tliveData?.lives || []).entries()) {
        for (const item of period.items || []) {
            events.push({period: index + 1, clock: item.time,
                content: item.content, score: item.score, goal: item.goal,
                team_hint: item.number});
        }
    }
    return {match_id: String(match.id), captured_at: new Date().toISOString(),
        top_text: (document.querySelector('.topBox')?.innerText || '').replace(/\s+/g, ' ').trim(),
        status_id: match.statusId, home_scores: match.homeScores,
        away_scores: match.awayScores, team_stats: state.detailStats || {},
        boxscore: {home: team('home'), away: team('away')}, events,
        identity_checks: identityChecks};
}"""


# Research access path: resolve the codec from the site's own loaded API module.
# No fixed webpack IDs, credentials, subscriptions or site LRU cache are used.
FETCH_BASKETBALL_DETAILS_JS = r"""async expectedId => {
    const read = (""" + BASKETBALL_DATA_JS + r""");
    const before = read(expectedId);
    if (before.error) return before;
    let require;
    const captureId = 'basket_stats_read_' + Date.now();
    if (!window.webpackJsonp) return {error: 'site_codec_unavailable'};
    window.webpackJsonp.push([[captureId], {
        [captureId]: function(module, exports, loader) { require = loader; }
    }, [[captureId]]]);
    if (!require) return {error: 'site_codec_unavailable'};
    delete require.c[captureId];
    delete require.m[captureId];
    const apiId = Object.keys(require.m).find(id => {
        const source = String(require.m[id]);
        return source.includes('fetchBasketballtlive') && source.includes('getDetailStats');
    });
    if (!apiId) return {error: 'site_codec_unavailable'};
    const codecIds = String(require.m[apiId]).match(
        /\w+\((\d+)\)\.Root\.fromJSON\(\w+\((\d+)\)\)/
    );
    if (!codecIds) return {error: 'site_codec_unavailable'};
    const root = require(Number(codecIds[1])).Root.fromJSON(require(Number(codecIds[2])));
    const envelopeType = root.lookup('onescore.app.v1.Response');
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 8000);
    const load = async (route, type) => {
        const url = 'https://api.aiscore.com/v1/m/api/match/' + route
            + '?match_id=' + encodeURIComponent(expectedId) + '&lang=2';
        const response = await fetch(url, {signal: controller.signal, cache: 'no-store',
            credentials: 'omit'});
        if (!response.ok) throw new Error('statistics_http_error');
        const envelope = envelopeType.decode(new Uint8Array(await response.arrayBuffer()));
        if (!envelope.data?.length) return {};
        const schema = root.lookup('onescore.app.v1.' + type);
        return schema.toObject(schema.decode(envelope.data));
    };
    try {
        const [lineups, timeline] = await Promise.all([
            load('lineups', 'MatchLineup'), load('tlive', 'TextLives')
        ]);
        const current = read(expectedId);
        if (current.error) return current;
        current.boxscore = Object.fromEntries(['home', 'away'].map(side => [side,
            lineups.lineup?.[side + 'PlayerTotals']?.bkDetail || null]));
        current.events = [];
        for (const [index, period] of (timeline.lives || []).entries()) {
            for (const item of period.items || []) {
                current.events.push({period: index + 1, clock: item.time,
                    content: item.content, score: item.score, goal: item.goal,
                    team_hint: item.number});
            }
        }
        current.fetch_method = 'uncached_public_api';
        return current;
    } catch (error) {
        return {error: error.name === 'AbortError' ? 'statistics_fetch_timeout'
            : 'statistics_fetch_failed'};
    } finally {
        clearTimeout(timer);
    }
}"""


def _integer(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str) and re.fullmatch(r"\d+", value.strip()):
        return int(value)
    return None


def _shot_pair(value):
    match = re.fullmatch(r"\s*(\d+)\s*[-/]\s*(\d+)\s*", str(value or ""))
    if not match:
        return None
    made, attempted = map(int, match.groups())
    return (made, attempted) if made <= attempted else None


def _score_total(values):
    # Source slots: Q1-Q4, combined OT; any later slots are not scoring periods.
    if not isinstance(values, list) or not 5 <= len(values) <= 7:
        return None
    scores = [_integer(value) for value in values[:5]]
    return sum(scores) if all(value is not None for value in scores) else None


def _boxscore(raw, expected_points):
    if not isinstance(raw, dict) or not raw:
        return None, "boxscore_missing"
    points = _integer(raw.get("points"))
    fg = _shot_pair(raw.get("fieldGoals"))
    three = _shot_pair(raw.get("threePoints"))
    free = _shot_pair(raw.get("freeThrows"))
    if points is None or fg is None or three is None or free is None:
        return None, "boxscore_incomplete"
    if points != expected_points:
        return None, "boxscore_score_mismatch"
    if three[0] > fg[0] or three[1] > fg[1]:
        return None, "boxscore_shots_invalid"
    two = (fg[0] - three[0], fg[1] - three[1])
    if two[0] > two[1] or 2 * fg[0] + three[0] + free[0] != points:
        return None, "boxscore_points_mismatch"
    result = {"points": points}
    for label, pair in (("field_goals", fg), ("two_points", two),
                        ("three_points", three), ("free_throws", free)):
        result[label + "_made"], result[label + "_attempted"] = pair
    for output, source in (("offensive_rebounds", "offensiveRebounds"),
                           ("defensive_rebounds", "defensiveRebounds"),
                           ("turnovers", "turnovers"), ("personal_fouls", "personalFouls"),
                           ("assists", "assists"), ("blocks", "blocks"), ("steals", "steals")):
        result[output] = _integer(raw.get(source))
    return result, ""


def normalize_basketball_data(payload, expected_id, *, expected_score=None):
    """Expose reconciled facts; this is neither a forecast nor a freshness proof.

    A capture timestamp proves when we read the page, not when the provider
    updated each statistic. Event subjects must not be inferred from team_hint:
    an event can describe both an offensive player and the defending team.
    """
    if not isinstance(payload, dict):
        return {"available": False, "error": "invalid_payload"}
    if payload.get("error"):
        result = {"available": False, "error": payload["error"]}
        if isinstance(payload.get("identity_checks"), dict):
            result["identity_checks"] = payload["identity_checks"]
        return result
    if str(payload.get("match_id")) != str(expected_id):
        return {"available": False, "error": "match_identity_mismatch"}
    scores = [_score_total(payload.get(side + "_scores")) for side in ("home", "away")]
    if any(score is None for score in scores):
        return {"available": False, "error": "score_unverified"}
    score = f"{scores[0]} - {scores[1]}"
    if expected_score is not None:
        expected = re.fullmatch(r"\s*(\d+)\s*[-–]\s*(\d+)\s*", str(expected_score))
        if not expected or list(map(int, expected.groups())) != scores:
            return {"available": False, "error": "observation_score_mismatch"}
    output = {"available": True, "match_id": str(expected_id), "score": score,
              "status_id": payload.get("status_id"), "captured_at": payload.get("captured_at"),
              "top_text": payload.get("top_text", ""),
              "teams": {}, "issues": [], "events": payload.get("events") or [],
              "fetch_method": payload.get("fetch_method", "page_state"),
              "provider_freshness_verified": False,
              "identity_checks": payload.get("identity_checks") or {}}
    stats = payload.get("team_stats") or {}
    boxscores = payload.get("boxscore") or {}
    for side, points in zip(("home", "away"), scores):
        detailed, issue = _boxscore(boxscores.get(side), points)
        if issue:
            output["issues"].append(side + ":" + issue)
        basic = {name: _integer((stats.get(str(key)) or {}).get(side))
                 for key, name in ((1, "three_points_made"), (2, "two_points_made"),
                                   (3, "free_throws_made"))}
        basic_valid = (all(value is not None for value in basic.values())
                       and 3 * basic["three_points_made"] + 2 * basic["two_points_made"]
                       + basic["free_throws_made"] == points)
        if not basic_valid:
            basic = {key: None for key in basic}
            output["issues"].append(side + ":basic_stats_score_mismatch")
        output["teams"][side] = {"points": points, "basic_shots_reconciled": basic_valid,
                                  "boxscore_reconciled": detailed is not None,
                                  "basic_shots": basic, "boxscore": detailed,
                                  "reported_team_fouls": _integer((stats.get("5") or {}).get(side)),
                                  "team_foul_scope_verified": False}
    output["full_boxscore_available"] = all(team["boxscore_reconciled"]
                                              for team in output["teams"].values())
    return output


async def read_basketball_data(page, expected_id, *, expected_score=None):
    """Use Patchright's main context; its default isolated context hides Nuxt."""
    payload = await page.evaluate(BASKETBALL_DATA_JS, str(expected_id), isolated_context=False)
    return normalize_basketball_data(payload, expected_id, expected_score=expected_score)


async def fetch_basketball_data(page, expected_id, *, expected_score=None):
    """Fetch fresh public boxscore/PBP on the existing page, for research only.

    The odds page does not preload full boxscores. This method avoids opening
    another page and bypasses the site's 30-minute JavaScript response cache.
    It remains outside publication: provider freshness is not proven by HTTP.
    """
    try:
        payload = await asyncio.wait_for(page.evaluate(
            FETCH_BASKETBALL_DETAILS_JS, str(expected_id), isolated_context=False,
        ), timeout=12)
    except asyncio.TimeoutError:
        return {"available": False, "error": "statistics_fetch_timeout"}
    except Exception:
        return {"available": False, "error": "statistics_reader_failed"}
    return normalize_basketball_data(payload, expected_id, expected_score=expected_score)
