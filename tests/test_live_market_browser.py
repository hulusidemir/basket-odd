"""Exercise the production Patchright world and scraper/history boundary offline."""

import asyncio
import time
from urllib.parse import parse_qs, urlsplit
from unittest.mock import AsyncMock

import pytest

from patchright.async_api import async_playwright

from aiscore_scraper import AiscoreScraper
from live_market import LIVE_SOURCE_JS, provenance_error, verify_current_market
from tests.test_live_market import observation, raw_history


@pytest.mark.parametrize("rendering,reason,selected", [
    ("normal", "", 2),
    ("hydrating", "", 2),
    ("pending_ui_promise", "", 2),
    ("blank_empty", "", 2),
    ("missing_bookmaker", "", 101),
    ("locked", "", 101),
    ("locked_empty", "", 101),
    ("ambiguous_empty", "", 101),
    ("missing_empty", "", 101),
    ("preferred_stale", "", 2),
    ("all_locked", "rendered_total_locked", None),
])
def test_scraper_reads_main_world_and_uses_a_verified_available_company(rendering, reason, selected):
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
                    if (mode === 'all_locked') el.querySelectorAll('.border3').forEach(c => c.classList.add('locked'));
                    if (mode.endsWith('_empty')) el.__vue__.copyOddsListData.bs[1].s.odd = [];
                    if (mode === 'blank_empty' || mode === 'locked_empty') cell.innerHTML = '';
                    if (mode === 'ambiguous_empty') cell.innerHTML = '<span>187.5</span><span>197.5</span>';
                    if (mode === 'missing_empty') cell.remove();
                    if (mode === 'missing_bookmaker') el.__vue__.copyOddsListData.bs.pop();
                    if (mode === 'pending_ui_promise') {
                        el.__vue__.copyOddsListData.bs[1].s.odd = [];
                        cell.innerHTML = '';
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
                    company = int(parse_qs(urlsplit(route.request.url).query)['cid'][0])
                    timestamp = int(time.time()) - (31 if rendering == 'preferred_stale' and company == 2 else 0)
                    await route.fulfill(status=200, body=raw_history(company, '187.5' if company == 2 else '197.5', timestamp),
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
                    uses_history = rendering in {'blank_empty', 'pending_ui_promise'}
                    assert len(requests) == int(uses_history)
                    if uses_history:
                        assert requests[-1].endswith(f"odds_type=bs&cid={selected}")
                    assert isinstance(result, dict), result
                    assert result["inplay_total"] == (187.5 if selected == 2 else 197.5)
                    assert result["bookmaker"] == ("bet365" if selected == 2 else "1xbet")
                    assert result["market_provenance"]["bookmaker_id"] == selected
                    assert result["market_provenance"]["version"] == ('aiscore_history_v2' if uses_history else 'aiscore_live_v2')
                    assert provenance_error(result) == ""
            finally:
                await browser.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("mode,reason", [
    ("normal", ""), ("nested", ""), ("hidden_quote", ""),
    ("wrong_live", "rendered_live_mismatch"),
    ("wrong_opening", "rendered_opening_mismatch"),
    ("mobile_hidden_anchors", ""),
    ("hidden_live", "rendered_total_unavailable"),
    ("missing_opening", "rendered_total_unavailable"),
    ("wrong_prematch", "rendered_prematch_mismatch"),
    ("legacy_render_validation", ""),
    ("ambiguous", "rendered_live_mismatch"),
    ("wrong_market", "rendered_market_unverified"),
])
def test_visible_135_is_not_confused_with_100_or_another_total(mode, reason):
    async def scenario():
        async with async_playwright() as driver:
            browser = await driver.chromium.launch(headless=True)
            try:
                page = await browser.new_page()
                await page.set_content('''<div class="topBox">Q2 07:14 30 - 24</div>
                    <div class="oddsBox"><div class="oddsContent"><p class="oddsType">Total Points</p>
                    <div class="oddsBoxContent"><div class="border1"><span>145</span></div>
                    <div class="border2"><span>140</span></div>
                    <div class="border3"><span>0.86</span><span>135</span><span>0.86</span></div>
                    </div></div></div>''')
                await page.evaluate(r"""mode => {
                    const el = document.querySelector('.oddsBox');
                    const match = {id:'m',statusId:4,homeScores:[30,0,0,0,0],awayScores:[24,0,0,0,0]};
                    window.$nuxt = {$store:{state:{basketball:{basketballDetailMatchData:{match}}}}};
                    const companies = [{id:101,name:'Any Company'}];
                    const slot = total => ({odd:['0.86',String(total),'0.86','0']});
                    el.__vue__ = {$el:el,match:{match},activeTab:'bs',oddListDataArray:companies,historyOdd() {},
                        copyOddsListData:{companies,bs:[{company:{id:101},f:slot(145),l:slot(140),s:slot(135)}]}};
                    const live = el.querySelector('.border3');
                    if(mode==='nested') live.innerHTML='<span>0.86</span><span>1<b>35</b></span><span>0.86</span>';
                    if(mode==='hidden_quote') live.innerHTML+='<span style="display:none">100</span>';
                    if(mode==='wrong_live') live.innerHTML='<span>100</span>';
                    if(mode==='wrong_opening') el.querySelector('.border1').innerHTML='<span>100</span>';
                    if(mode==='wrong_prematch') el.querySelector('.border2').innerHTML='<span>100</span>';
                    if(mode==='mobile_hidden_anchors') {
                        el.querySelector('.border1').style.display='none';
                        el.querySelector('.border2').style.display='none';
                    }
                    if(mode==='hidden_live') live.style.display='none';
                    if(mode==='missing_opening') el.querySelector('.border1').remove();
                    if(mode==='ambiguous') live.innerHTML+='<span>100</span>';
                    if(mode==='wrong_market') el.querySelector('.oddsType').innerText='1st Quarter Total Points';
                }""", mode, isolated_context=False)
                source = await page.evaluate(LIVE_SOURCE_JS, 101, isolated_context=False)
                if mode == "mobile_hidden_anchors":
                    assert source["rendered_slot_visibility"] == {
                        "opening": False, "prematch": False, "live": True}
                    assert source["rendered_totals"] == {
                        "opening": [], "prematch": [], "live": [135]}
                if mode == "legacy_render_validation":
                    source["rendered_validation_version"] = 1
                    source.pop("rendered_slot_visibility")
                proof, actual = verify_current_market(source, 'm', 'Q2 07:14', '30 - 24', now=time.time())
                assert actual == reason
                if not reason:
                    assert proof['live'] == 135 and proof['bookmaker_id'] == 101
                else:
                    assert proof is None
            finally:
                await browser.close()
    asyncio.run(scenario())


def test_second_capture_uses_latest_consistent_quote_and_rejects_new_mismatch():
    import copy
    from types import SimpleNamespace

    source, _, _ = observation()
    source.update(candidate_ids=[2], top_text='Q2 07:14 30 - 24', dom_score='30 - 24',
                  source_version='aiscore_history_v2')
    latest = copy.deepcopy(source)
    latest['rows'][1]['live'][1] = '189.5'
    latest.update(rendered_live=189.5, rendered_live_values=[189.5])
    page = SimpleNamespace(evaluate=AsyncMock(side_effect=[source, latest]))
    scraper = AiscoreScraper('https://m.aiscore.com/basketball')
    result = asyncio.run(scraper._capture_verified_market(page, 'm', source))
    assert result['live'] == 189.5 and page.evaluate.await_count == 2
    latest['rendered_live_values'] = [100]
    page.evaluate = AsyncMock(side_effect=[source, latest])
    result = asyncio.run(scraper._capture_verified_market(page, 'm', source))
    assert result.reason == 'rendered_total_mismatch'
