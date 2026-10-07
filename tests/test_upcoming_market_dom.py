import pytest
from camoufox.sync_api import Camoufox

from aiscore_scraper import LIVE_TOTAL_MARKET_JS
from upcoming_odds import TOTAL_MARKET_JS
from live_market import FETCH_HISTORY_JS, LIVE_SOURCE_JS, verify_market
from tests.test_live_market import observation


@pytest.fixture(scope="module")
def page():
    with Camoufox(headless=True) as browser:
        page = browser.new_page()
        yield page
        page.close()


def market(*boxes, label="Total Points"):
    return f'<div class="oddsContent"><p class="oddsType">{label}</p>{"".join(boxes)}</div>'


def box(opening, prematch, company="Book A"):
    return (f'<div class="oddsBox"><span class="companyName">{company}</span><div class="oddsBoxContent">'
            f'<div class="border1">{opening}</div><div class="border2">{prematch}</div></div></div>')


def line(value):
    return f'<span>0.85</span><span>{value}</span><span>0.95</span>'


def live_box(opening, prematch, inplay, company="Book A"):
    return (
        f'<div class="oddsBox"><span class="companyName">{company}</span>'
        f'<div class="oddsBoxContent"><div class="border1">{opening}</div>'
        f'<div class="border2">{prematch}</div><div class="border3">{inplay}</div>'
        '</div></div>'
    )


def test_live_source_selects_native_bet365_row_when_company_order_changes(page):
    source, history, now = observation()
    for reverse in (False, True):
        page.set_content('<div class="topBox">Q2 07:14 30 - 24</div><div class="oddsBox"></div>')
        page.evaluate(r"""rows => {
            const el = document.querySelector('.oddsBox');
            window.$nuxt = {$store: {state: {basketball: {basketballDetailMatchData: {match: {
                id: 'm', statusId: 4, homeScores: [27,3,0,0,0], awayScores: [24,0,0,0,0]
            }}}}}};
            const companies = rows.map(r => ({id:r.bookmaker_id, name:r.bookmaker_id===2?'bet365':'1xbet'}));
            el.innerHTML = '<p class="oddsType">Total Points</p>' + rows.map(r => `<div class="oddsBoxContent">
                <div class="border1"><span>${r.opening[1]}</span></div>
                <div class="border2"><span>${r.prematch[1]}</span></div><div class="border3">
                <span>${r.live[0]}</span><span>${r.live[1]}</span><span>${r.live[2]}</span>
                </div></div>`).join('');
            el.__vue__ = {
                $el: el, match: window.$nuxt.$store.state.basketball.basketballDetailMatchData,
                activeTab: 'bs', copyOddsListData: {companies, bs: rows.map(r => ({company:{id:r.bookmaker_id},
                    f:{odd:r.opening}, l:{odd:r.prematch}, s:{odd:r.live}}))},
                oddListDataArray: companies, allData: ['stale'], isShowModal: false,
                async historyOdd(id, name) {this.lastRequest = [id, name]; this.isShowModal = true;}
            };
        }""", list(reversed(source["rows"])) if reverse else source["rows"])
        captured = page.evaluate(LIVE_SOURCE_JS)
        assert verify_market(captured, history, 'm', 'Q2 07:14', '30 - 24', now=now)[0]['live'] == 187.5
        page.evaluate(FETCH_HISTORY_JS, 'm')
        assert page.evaluate("() => {const c=document.querySelector('.oddsBox').__vue__; return [c.lastRequest,c.allData,c.isShowModal]}") == [[2, 'bet365'], [], False]
        page.evaluate("() => document.querySelector('.oddsBoxContent:last-child .border3').innerHTML += '<span>200.5</span>'")
        # An alternate or ambiguous line in the selected cell never supplies a live total.
        if not reverse:
            assert page.evaluate(LIVE_SOURCE_JS)["rendered_live"] is None


def test_only_total_market_opening_and_same_bookmaker_are_read(page):
    page.set_content(market(box(line(190), line(191)), label="Spread") + market(
        box(line(160.5), '<i class="lock"></i>'), box(line(170.5), line(180.5), "Book B")))
    result = page.evaluate(TOTAL_MARKET_JS)
    assert result["opening"] == 160.5
    assert result["prematch"] is None
    assert result["bookmaker"] == "Book A"


def test_locked_opening_never_uses_current_or_arbitrary_numbers(page):
    page.set_content(market(box('<i class="lock"></i>', line(165.5)))
                     + '<div class="newOdds">Total Points 155 166 177</div>')
    result = page.evaluate(TOTAL_MARKET_JS)
    assert result["opening"] is None
    assert result["prematch"] == 165.5


def test_complete_bookmaker_is_preferred_over_missing_opening(page):
    page.set_content(market(box('-', line(155)), box(line(171), line(173), "Book B")))
    result = page.evaluate(TOTAL_MARKET_JS)
    assert (result["opening"], result["prematch"], result["bookmaker"]) == (171, 173, "Book B")


def test_unverified_market_and_ambiguous_cells_remain_empty(page):
    page.set_content(market(box(line(171), line(173)), label="1st Quarter Total Points"))
    assert page.evaluate(TOTAL_MARKET_JS)["opening"] is None
    page.set_content(market(box('<span>150</span><span>180</span>', '-')))
    assert page.evaluate(TOTAL_MARKET_JS)["opening"] is None


def test_live_reader_uses_only_exact_full_game_total_market(page):
    page.set_content(
        market(live_box(line(150), line(151), line(152)), label="1st Quarter Total Points")
        + market(live_box(line(170), line(171), line(175.5)), label="Total Points")
    )
    result = page.evaluate(LIVE_TOTAL_MARKET_JS)
    assert result["market_verified"] is True
    assert result["opening_lines"] == [170]
    assert result["prematch_lines"] == [171]
    assert result["inplay_lines"] == [175.5]


def test_live_reader_rejects_locked_numeric_and_uses_active_bookmaker(page):
    locked_live = '<span>175.5</span><i class="lockIcon"></i>'
    page.set_content(market(
        live_box(line(170), line(171), locked_live),
        live_box(line(180), '-', line(188.5), "Book B"),
    ))
    result = page.evaluate(LIVE_TOTAL_MARKET_JS)
    assert result["has_locked_rows"] is True
    assert result["opening_lines"] == [180]
    assert result["prematch_lines"] == [None]
    assert result["inplay_lines"] == [188.5]
    assert result["bookmaker_lines"] == ["Book B"]


def test_live_reader_rejects_ambiguous_market_cell(page):
    page.set_content(market(live_box(
        line(170),
        line(171),
        '<span>175.5</span><span>180.5</span>',
    )))
    result = page.evaluate(LIVE_TOTAL_MARKET_JS)
    assert result["inplay_lines"] == []
    assert result["has_locked_rows"] is True
