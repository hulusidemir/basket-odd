import pytest
from camoufox.sync_api import Camoufox

from aiscore_scraper import LIVE_TOTAL_MARKET_JS
from upcoming_odds import TOTAL_MARKET_JS


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
