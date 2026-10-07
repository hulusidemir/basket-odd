import asyncio
import copy
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from aiscore_basketball_data import normalize_basketball_data
from db import Database
from motor2 import evaluate_m2, pending_m2
from motor2_worker import M2Collector, run_m2_worker
from tests.test_basketball_data import observation


NOW = datetime(2026, 10, 6, 0, 0, tzinfo=timezone.utc)


def sample():
    context = {"match_id": "sample", "score": "48-48", "status": "Q2 00:00", "line": 200,
               "observed_at": NOW.isoformat(), "quarter_length": 10, "period_count": 4}
    payload = {**observation(), "captured_at": NOW.isoformat(), "status_id": 4,
               "top_text": "Home Away Q2 00:00 48-48"}
    data = normalize_basketball_data(payload, "sample")
    data.update(fetch_method="uncached_public_api",
                events=[{"period": 2, "clock": "00:00", "score": "48-48"}])
    return context, data


@pytest.mark.parametrize("line,direction", [(120, "ÜST"), (250, "ALT"), (200, "PAS")])
def test_m2_is_independent_of_m1_and_can_pass(line, direction):
    context, data = sample()
    context["line"] = line
    before = copy.deepcopy(data)
    for original_direction in ("ALT", "ÜST", "PAS"):
        result = evaluate_m2({**context, "direction": original_direction}, data, now=NOW)
        assert result["direction"] == direction
        assert result["quality"] == "ORTA"
        assert result["reasons"] and result["limitations"]
        assert result["teams"][0]["shooting"]["two_points"]["attempted"] == 18
    assert data == before


@pytest.mark.parametrize("change,expected", [
    ({"available": False, "error": "statistics_fetch_timeout"}, "zaman aşımı"),
    ({"match_id": "another"}, "kimliği"),
    ({"score": "50-48"}, "Skor"),
    ({"fetch_method": "page_state"}, "Güncel"),
    ({"captured_at": (NOW - timedelta(minutes=2)).isoformat()}, "güncel veri"),
    ({"top_text": "Q3 10:00"}, "Oyun saati"),
    ({"status_id": 6}, "Oyun saati"),
    ({"events": []}, "Olay akışı"),
    ({"full_boxscore_available": False}, "şut verisi"),
])
def test_unusable_data_never_generates_direction(change, expected):
    context, data = sample()
    result = evaluate_m2(context, {**data, **change}, now=NOW)
    assert result["state"] == "unavailable"
    assert result["direction"] is None
    assert expected in result["message"]


def test_missing_optional_possession_field_is_not_assumed_zero():
    context, data = sample()
    data["teams"]["home"]["boxscore"]["turnovers"] = None
    assert evaluate_m2(context, data, now=NOW)["direction"] is None


def test_same_score_different_shot_volume_changes_analysis():
    context, fast = sample()
    slow = copy.deepcopy(fast)
    for team in slow['teams'].values():
        team['boxscore'].update(field_goals_attempted=29, two_points_attempted=8,
                               three_points_attempted=21, offensive_rebounds=4)
    fast_result = evaluate_m2(context, fast, now=NOW)
    slow_result = evaluate_m2(context, slow, now=NOW)
    assert fast_result['state'] == slow_result['state'] == 'ready'
    assert fast_result['possessions_per_team'] > slow_result['possessions_per_team']
    assert fast_result['projection'] > slow_result['projection']


def test_nba_duration_is_frozen_not_reinterpreted_as_forty():
    context, data = sample()
    context.update(status="Q2 11:00", quarter_length=12)
    data.update(top_text="Q2 11:00", events=[{"period": 2, "clock": "11:00", "score": "48-48"}])
    result = evaluate_m2(context, data, now=NOW)
    assert result["state"] == "ready"
    assert result["remaining_minutes"] == 35


def test_empty_stat_values_and_possession_imbalance_reject():
    context, data = sample()
    data["teams"]["home"]["boxscore"]["offensive_rebounds"] = 500
    assert evaluate_m2(context, data, now=NOW)["direction"] is None
    context, data = sample()
    data["teams"]["home"]["boxscore"]["turnovers"] = 50
    assert evaluate_m2(context, data, now=NOW)["direction"] is None


@pytest.fixture
def database(tmp_path):
    db = Database(str(tmp_path / "m2.db"))
    db.init()
    return db


def save(database, context=None, match_id="sample"):
    return database.save_alert(match_id, "Home - Away", 180, 200, "ALT", 20,
                               status="Q2 00:00", score="48-48", tournament="EuroLeague",
                               m2_analysis={"state": "pending", "context": context or {}})


def test_migration_preserves_old_data_and_never_backfills(database):
    alert_id = database.save_alert("legacy", "Home - Away", 160, 180, "ALT", 20)
    with sqlite3.connect(database.db_path) as conn:
        conn.execute("ALTER TABLE alerts DROP COLUMN m2_analysis_json")
    database.init()
    database.init()
    alert = database.get_alert(alert_id)
    assert alert["m2_analysis_json"] is None
    assert (alert["direction"], alert["live"]) == ("ALT", 180)
    assert database.pending_m2_alerts() == []


def test_only_m2_is_updated_once_and_archive_wins(database):
    alert_id = save(database)
    before = database.get_alert(alert_id)
    assert database.complete_m2_analysis(alert_id, {"state": "ready", "direction": "ÜST"})
    assert not database.complete_m2_analysis(alert_id, {"state": "ready", "direction": "ALT"})
    after = database.get_alert(alert_id)
    assert {k: v for k, v in before.items() if k != "m2_analysis_json"} == {
        k: v for k, v in after.items() if k != "m2_analysis_json"}
    pending_id = save(database, match_id="archive-first")
    database.archive_match_with_display_snapshots("archive-first", {pending_id: {"m2": {"state": "pending"}}})
    assert not database.complete_m2_analysis(pending_id, {"state": "ready", "direction": "ÜST"})


def test_dashboard_removes_legacy_m2_without_rewriting_archive(database):
    import dashboard
    alert_id = save(database)
    context, data = sample()
    analysis = evaluate_m2({**context, "line": 120}, data, now=NOW)
    database.complete_m2_analysis(alert_id, analysis)
    with patch.object(dashboard, "db", database):
        client = dashboard.app.test_client()
        live = client.get('/api/alerts').get_json()[0]
        assert live['direction'] == 'ALT' and 'm2' not in live
        assert 'm2_analysis_json' not in live
        live_html = client.get('/').get_data(as_text=True)
        assert '<th>M2</th>' not in live_html and 'id="m2Modal"' not in live_html
        assert 'Motor2.' not in live_html and 'motor2.js' not in live_html
        assert 'id="signalModal"' in live_html
        archive_html = client.get('/deleted-matches').get_data(as_text=True)
        assert '<th>M2</th>' not in archive_html and 'id="m2Modal"' not in archive_html
        assert 'Motor2.' not in archive_html and 'motor2.js' not in archive_html
        assert 'motor2.css' not in archive_html and 'openM2Modal' not in archive_html
        assert 'id="signalModal"' in archive_html
        database.archive_match_with_display_snapshots('sample', {alert_id: {**live, 'm2': analysis}})
        stored = database.get_deleted_alert_by_id(alert_id)
        with patch('dashboard._raw_alert', side_effect=AssertionError('Recalculation')):
            archived = client.get('/api/deleted-matches').get_json()[0]
            details = client.get(f'/api/deleted-matches/{alert_id}/details').get_json()
        assert 'm2' not in archived and 'm2_analysis_json' not in archived
        assert 'm2' not in details and details['direction'] == live['direction']
        assert database.get_deleted_alert_by_id(alert_id) == stored
        old = dashboard._frozen_deleted_alert({'m2_analysis_json': json.dumps(analysis), 'display_snapshot': '{}'})
        assert 'm2' not in old


def test_worker_expiry_cancellation_and_no_historical_reads(database):
    context, _ = sample()
    database.save_alert('old', 'Home - Away', 180, 200, 'ALT', 20)
    alert_id = save(database, context)
    collector = type('Collector', (), {'read': AsyncMock(), 'close': AsyncMock()})()
    with patch('motor2_worker.asyncio.sleep', new=AsyncMock(side_effect=asyncio.CancelledError)):
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(run_m2_worker(database, collector=collector))
    collector.read.assert_not_awaited()
    collector.close.assert_awaited_once()
    assert json.loads(database.get_alert(alert_id)['m2_analysis_json'])['state'] == 'unavailable'


def test_worker_real_path_persists_assessment_and_shuts_down(database):
    context, data = sample()
    context['observed_at'] = datetime.now(timezone.utc).isoformat()
    data['captured_at'] = context['observed_at']
    alert_id = save(database, context)
    collector = type('Collector', (), {'read': AsyncMock(return_value=data), 'close': AsyncMock()})()
    with patch('motor2_worker.asyncio.sleep', new=AsyncMock(side_effect=asyncio.CancelledError)):
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(run_m2_worker(database, collector=collector))
    collector.read.assert_awaited_once()
    collector.close.assert_awaited_once()
    assert json.loads(database.get_alert(alert_id)['m2_analysis_json'])['state'] == 'ready'
    assert database.get_alert(alert_id)['direction'] == 'ALT'


def test_failed_browser_close_still_stops_owned_driver():
    driver = type('Driver', (), {'stop': AsyncMock()})()
    session = type('Session', (), {'__aexit__': AsyncMock(side_effect=RuntimeError('close failed')),
                                  'playwright': driver})()
    collector = M2Collector()
    collector.session = session
    asyncio.run(collector.close())
    driver.stop.assert_awaited_once()
    assert collector.session is None


def test_collector_uses_challenge_solver_and_reads_before_load_finishes():
    context, data = sample()
    url = 'https://m.aiscore.com/basketball/match-home-away/sample/odds'
    page = type('Page', (), {'url': url, 'evaluate': AsyncMock(return_value=True),
                             'set_default_timeout': lambda self, value: None})()
    cleaned = []

    async def fetch(url, **kwargs):
        assert kwargs['solve_cloudflare'] is True
        await kwargs['page_setup'](page)
        try:
            await asyncio.Event().wait()  # A load event that never finishes.
        finally:
            cleaned.append(True)

    collector = M2Collector()
    collector.session = type('Session', (), {'fetch': AsyncMock(side_effect=fetch)})()
    with patch('motor2_worker.fetch_basketball_data', new=AsyncMock(return_value=data)) as read:
        result = asyncio.run(collector.read({'match_id': 'sample', 'url': url}, context))
    assert result == data
    assert cleaned == [True]
    read.assert_awaited_once_with(page, 'sample', expected_score='48-48')
    assert page.evaluate.call_args.kwargs['isolated_context'] is False


def test_collector_keeps_pool_page_alive_until_api_read_completes():
    context, data = sample()
    url = 'https://m.aiscore.com/basketball/match-home-away/sample/odds'
    page = type('Page', (), {'url': url, 'evaluate': AsyncMock(return_value=True),
                             'set_default_timeout': lambda self, value: None})()
    alive = []

    async def fetch(url, **kwargs):
        alive.append(True)
        try:
            await kwargs['page_setup'](page)
            await kwargs['page_action'](page)
        finally:
            alive.clear()

    async def read(*args, **kwargs):
        await asyncio.sleep(0)
        assert alive == [True]
        return data

    collector = M2Collector()
    collector.session = type('Session', (), {'fetch': AsyncMock(side_effect=fetch)})()
    with patch('motor2_worker.fetch_basketball_data', new=AsyncMock(side_effect=read)):
        result = asyncio.run(collector.read({'match_id': 'sample', 'url': url}, context))
    assert result == data
    assert alive == []


def test_collector_waits_for_complete_identity_hydration_before_api_read():
    context, data = sample()
    url = 'https://m.aiscore.com/basketball/match-home-away/sample/odds'
    page = type('Page', (), {'url': url, 'evaluate': AsyncMock(side_effect=[False, True]),
                             'set_default_timeout': lambda self, value: None})()

    async def fetch(url, **kwargs):
        await kwargs['page_setup'](page)
        await kwargs['page_action'](page)

    async def read(*args, **kwargs):
        assert page.evaluate.await_count == 2
        return data

    collector = M2Collector()
    collector.session = type('Session', (), {'fetch': AsyncMock(side_effect=fetch)})()
    with patch('motor2_worker.fetch_basketball_data', new=AsyncMock(side_effect=read)):
        result = asyncio.run(collector.read({'match_id': 'sample', 'url': url}, context))
    assert result == data


@pytest.mark.parametrize('url,matched', [
    ('https://m.aiscore.com/basketball/match-home-away/previous/odds', True),
    ('https://m.aiscore.com/basketball/match-home-away/sample/odds', False),
])
def test_collector_does_not_read_previous_or_blocked_page(url, matched):
    context, _ = sample()
    page = type('Page', (), {'url': url, 'evaluate': AsyncMock(return_value=matched),
                             'set_default_timeout': lambda self, value: None})()
    cleaned = []

    async def fetch(url, **kwargs):
        await kwargs['page_setup'](page)
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.append(True)

    collector = M2Collector()
    collector.session = type('Session', (), {'fetch': AsyncMock(side_effect=fetch),
                                            '__aexit__': AsyncMock()})()
    with patch('motor2_worker.READ_BUDGET_SECONDS', .02), \
            patch('motor2_worker.fetch_basketball_data', new=AsyncMock()) as read:
        result = asyncio.run(collector.read({
            'match_id': 'sample', 'url': 'https://m.aiscore.com/basketball/match-home-away/sample'}, context))
    assert result == {'available': False, 'error': 'statistics_fetch_timeout'}
    assert cleaned == [True]
    read.assert_not_awaited()
    assert collector.session is None


@pytest.mark.parametrize('seconds,direction', [(15, 'PAS'), (16, None)])
def test_clock_skew_boundary_uses_whole_seconds(seconds, direction):
    context, data = sample()
    context['status'] = 'Q2 01:13'
    remaining = 73 - seconds
    clock = f'00:{remaining:02}'
    data.update(top_text=f'Q2 {clock}', events=[{'period': 2, 'clock': clock, 'score': '48-48'}])
    result = evaluate_m2(context, data, now=NOW)
    assert result['direction'] == direction
    if seconds == 16:
        assert result['reason_code'] == 'clock_mismatch'


def test_missing_boxscore_is_reported_before_missing_timeline():
    context, data = sample()
    data.update(full_boxscore_available=False, events=[], issues=['home:boxscore_missing'])
    result = evaluate_m2(context, data, now=NOW)
    assert result['reason_code'] == 'boxscore_missing'
    assert result['data_issues'] == ['home:boxscore_missing']
    data['issues'] = ['home:boxscore_score_mismatch']
    result = evaluate_m2(context, data, now=NOW)
    assert result['reason_code'] == 'boxscore_mismatch'
