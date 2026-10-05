"""Exercise the production Patchright world and scraper/history boundary offline."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from patchright.async_api import async_playwright

from aiscore_scraper import AiscoreScraper
from live_market import LIVE_SOURCE_JS, provenance_error
from tests.test_live_market import observation, raw_history


@pytest.mark.parametrize("rendering,reason", [
    ("normal", ""),
    ("hydrating", ""),
    ("pending_ui_promise", ""),
    ("blank_empty", ""),
    ("missing_bookmaker", "bet365_missing_or_ambiguous"),
    ("locked", "rendered_total_locked"),
    ("locked_empty", "rendered_total_locked"),
    ("ambiguous_empty", "rendered_total_ambiguous"),
    ("missing_empty", "rendered_total_unavailable"),
])
def test_scraper_reads_main_world_and_intercepts_exact_bet365_history(rendering, reason):
    async def scenario():
        async with async_playwright() as driver:
            browser = await driver.chromium.launch(headless=True)
            try:
                page = await browser.new_page()
                await page.set_content('''<title>Home vs Away - AiScore</title>
                    <div class="topBox">Q2 07:14 30 - 24</div>
                    <a href="/basketball/tournament-fiba/id">FIBA</a>
                    <div class="oddsBox"><div class="oddsContent">
                    <p class="oddsType">Total Points</p></div></div>''')
                source, _, _ = observation()
                await page.evaluate(r"""rows => {
                    const el = document.querySelector('.oddsBox');
                    window.$nuxt = {$store: {state: {basketball: {basketballDetailMatchData: {match: {
                        id:'m', statusId:4, homeScores:[27,3,0,0,0], awayScores:[24,0,0,0,0]
                    }}}}}};
                    const companies = rows.map(r=>({id:r.bookmaker_id,name:r.bookmaker_id===2?'bet365':'1xbet'}));
                    el.querySelector('.oddsContent').innerHTML += rows.map(r=>`<div class="oddsBoxContent">
                        <div class="border1"><span>${r.opening[1]}</span></div>
                        <div class="border2"><span>${r.prematch[1]}</span></div>
                        <div class="border3"><span>${r.live[0]}</span><span>${r.live[1]}</span><span>${r.live[2]}</span></div>
                        </div>`).join('');
                    el.__vue__ = {$el:el, match:window.$nuxt.$store.state.basketball.basketballDetailMatchData,
                        activeTab:'bs', oddListDataArray:companies, copyOddsListData:{companies,
                        bs:rows.map(r=>({company:{id:r.bookmaker_id},f:{odd:r.opening},l:{odd:r.prematch},s:{odd:r.live}}))},
                        async historyOdd(id,name) {
                            await fetch(`https://api.aiscore.com/v1/m/api/match/odds/detail?match_id=m&odds_type=bs&cid=${id}`);
                        }};
                }""", source["rows"], isolated_context=False)
                assert (await page.evaluate(LIVE_SOURCE_JS))["match_id"] == ""
                assert (await page.evaluate(LIVE_SOURCE_JS, isolated_context=False))["match_id"] == "m"
                await page.evaluate(r"""mode => {
                    const el = document.querySelector('.oddsBox');
                    const cell = el.querySelectorAll('.border3')[1];
                    if (mode.startsWith('locked')) cell.classList.add('locked');
                    if (mode.endsWith('_empty')) el.__vue__.copyOddsListData.bs[1].s.odd = [];
                    if (mode === 'blank_empty' || mode === 'locked_empty') cell.innerHTML = '';
                    if (mode === 'ambiguous_empty') cell.innerHTML = '<span>187.5</span><span>197.5</span>';
                    if (mode === 'missing_empty') cell.remove();
                    if (mode === 'missing_bookmaker') el.__vue__.copyOddsListData.bs.pop();
                    if (mode === 'pending_ui_promise') {
                        const fetchHistory = el.__vue__.historyOdd;
                        el.__vue__.historyOdd = async function(id, name) {
                            await fetchHistory.call(this, id, name);
                            await new Promise(() => {});
                        };
                    }
                    if (mode === 'hydrating') {
                        const match = window.$nuxt.$store.state.basketball.basketballDetailMatchData.match;
                        match.id = 'previous-page';
                        setTimeout(() => { match.id = 'm'; }, 1000);
                    }
                }""", rendering, isolated_context=False)
                requests = []

                async def respond(route):
                    requests.append(route.request.url)
                    await route.fulfill(status=200, body=raw_history(),
                                        headers={"Access-Control-Allow-Origin": "*"},
                                        content_type="application/octet-stream")

                await page.route("https://api.aiscore.com/v1/m/api/match/odds/detail?*", respond)
                scraper = AiscoreScraper("https://m.aiscore.com/basketball")
                scraper._goto_detail_with_retry = AsyncMock()
                scraper._wait_for_odds_ready = AsyncMock()
                result = await scraper._extract_match(page, "https://m.aiscore.com/basketball/match-home-away/m")
                if reason:
                    assert requests == []
                    assert result.reason == reason
                else:
                    assert len(requests) == 1 and requests[0].endswith("odds_type=bs&cid=2")
                    assert isinstance(result, dict), result
                    assert result["inplay_total"] == 187.5 and result["bookmaker"] == "bet365"
                    assert provenance_error(result) == ""
            finally:
                await browser.close()

    asyncio.run(scenario())
