import asyncio
import copy
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from config import Config
from db import Database
from forward_validation import report
from live_market import provenance_error
from live_signals import direction_for_total, forecast_live_total
from main import process_match, retry_pending_telegram_deliveries
from tests.market_fixture import current_payload


def payload(**changes):
    return current_payload({
        'match_id': 'm', 'match_name': 'Home - Away', 'tournament': 'FIBA',
        'status': 'Q2 05:00', 'score': '30 - 30', 'opening_total': 160,
        'prematch_total': 160, 'inplay_total': 157.5, **changes,
    })


@pytest.fixture
def database(tmp_path):
    db = Database(str(tmp_path / 'forecasts.db'))
    db.init()
    return db


def test_every_line_uses_the_same_unrounded_forecast():
    forecast = forecast_live_total(payload(), Config())
    assert forecast['predicted_total'] == 160
    assert forecast['direction'] == 'ÜST'
    assert direction_for_total(160, 159.999) == 'ÜST'
    assert direction_for_total(160, 160) == 'EŞİT'
    assert direction_for_total(160, 160.001) == 'ALT'


@pytest.mark.parametrize('status,score,expected', [
    ('Q1 10:00', '0 - 0', 160),
    ('Q1 05:00', '10 - 10', 160),
    ('Q4 01:00', '78 - 78', 160),
])
def test_early_and_late_forecasts_are_saved_without_telegram(database, status, score, expected):
    match = payload(status=status, score=score)
    notifier = type('Notifier', (), {'send_alert': AsyncMock()})()
    asyncio.run(process_match(match, database, notifier, Config()))
    rows = database.latest_live_forecasts()
    assert len(rows) == 1
    frozen = json.loads(rows[0]['forecast_json'])
    assert frozen['forecast']['predicted_total'] == expected
    assert frozen['forecast']['direction'] == 'ÜST'
    assert frozen['bookmaker_id'] == 101
    assert database.count_match_alerts('m') == 0
    notifier.send_alert.assert_not_awaited()


def test_small_advantage_visible_and_large_advantage_delivered_for_other_company(database):
    notifier = type('Notifier', (), {'send_alert': AsyncMock(return_value={'recipient': 1})})()
    asyncio.run(process_match(payload(), database, notifier, Config()))
    first = database.get_match_snapshots('m')[0]
    assert database.count_match_alerts('m') == 0
    asyncio.run(process_match(payload(inplay_total=170), database, notifier, Config()))
    alert = database.get_alert(1)
    assert alert['direction'] == 'ALT' and alert['telegram_status'] == 'sent'
    assert alert['fair_total'] == 160
    assert json.loads(alert['market_provenance_json'])['bookmaker_id'] == 101
    assert database.get_match_snapshots('m')[0] == first
    assert json.loads(first['forecast_json'])['forecast']['line'] == 157.5
    notifier.send_alert.assert_awaited_once()


def test_current_quote_does_not_require_bet365_or_an_odds_change_timestamp():
    match = payload()
    assert 'provider_updated_at' not in match['market_provenance']
    assert 'raw_history_base64' not in match['market_provenance']
    assert match['market_provenance']['provider_freshness_verified'] is False
    assert provenance_error(match) == ''
    for change in ('inplay_total', 'score', 'status'):
        altered = copy.deepcopy(match)
        altered[change] = {'inplay_total': 180, 'score': '40 - 30', 'status': 'Q3 05:00'}[change]
        assert provenance_error(altered) == 'market_provenance_mismatch'
    match['market_provenance']['source']['bookmaker_id'] = 2
    assert provenance_error(match) == 'bookmaker_missing_or_ambiguous'


def test_retry_accepts_a_current_quote_from_another_company_and_rejects_expiry(database):
    notifier = type('Notifier', (), {'send_alert': AsyncMock(return_value={'recipient': 1})})()
    with patch('main._deliver_signal', new=AsyncMock()):
        asyncio.run(process_match(payload(inplay_total=170), database, notifier, Config()))
    assert database.get_alert(1)['telegram_status'] == 'pending'
    result = asyncio.run(retry_pending_telegram_deliveries(database, notifier))
    assert result['sent'] == 1
    assert database.get_alert(1)['telegram_status'] == 'sent'
    with patch('main._deliver_signal', new=AsyncMock()):
        asyncio.run(process_match(payload(match_id='expired', inplay_total=170), database, notifier, Config()))
    alert = database.get_alert(2)
    proof = json.loads(alert['market_provenance_json'])
    proof['captured_at'] = (datetime.now(timezone.utc) - timedelta(seconds=21)).isoformat()
    with database._conn() as conn:
        conn.execute('UPDATE alerts SET market_provenance_json=? WHERE id=2', (json.dumps(proof),))
    result = asyncio.run(retry_pending_telegram_deliveries(database, notifier))
    assert result['cancelled'] == 1


def test_other_company_signal_is_evaluated_at_capture_time(database):
    notifier = type('Notifier', (), {'send_alert': AsyncMock(return_value={'recipient': 1})})()
    asyncio.run(process_match(payload(inplay_total=170), database, notifier, Config()))
    with database._conn() as conn:
        conn.execute("UPDATE alerts SET result='Başarılı', final_total=160, result_source='automatic_final_score', settled_at=? WHERE id=1", (datetime.now(timezone.utc).isoformat(),))
    output = report(database.db_path)
    cohort = next(iter(output['cohorts'].values()))
    assert cohort['overall']['wins'] == 1
    assert cohort['overall']['forecast_error']['model_mae'] == 0


def test_additive_migration_preserves_old_snapshots_and_alerts(database):
    database.save_snapshot_if_changed('old', 2, '05:00', 900, 25, 30, 30, 60, 160, 157.5)
    before = database.get_match_snapshots('old')[0]
    with database._conn() as conn:
        conn.execute('DROP INDEX idx_forecast_snapshots_match')
        conn.execute('DROP INDEX idx_forecast_snapshots_history')
        conn.execute('ALTER TABLE match_live_snapshots DROP COLUMN forecast_json')
    database.init()
    database.init()
    after = database.get_match_snapshots('old')[0]
    assert after == before and after['forecast_json'] is None
    assert database.latest_live_forecasts() == []


def test_forecast_api_reads_frozen_values_and_scenarios_do_not_change_them(database):
    import dashboard
    notifier = type('Notifier', (), {'send_alert': AsyncMock()})()
    asyncio.run(process_match(payload(), database, notifier, Config()))
    before = database.get_match_snapshots('m')
    with patch.object(dashboard, 'db', database), patch('main.forecast_live_total', side_effect=AssertionError('recomputed')):
        client = dashboard.app.test_client()
        original = client.get('/api/forecasts').get_json()[0]
        lower = client.get('/api/forecasts?line=140').get_json()[0]
        upper = client.get('/api/forecasts?line=180').get_json()[0]
        equal = client.get('/api/forecasts?line=160').get_json()[0]
        assert lower['scenario']['direction'] == 'ÜST'
        assert upper['scenario']['direction'] == 'ALT'
        assert equal['scenario']['direction'] == 'EŞİT'
        assert lower['forecast'] == upper['forecast'] == original['forecast']
        assert 'market_provenance_json' not in original
        for line in ('NaN', 'Infinity', '-1', '0', '1001'):
            assert client.get('/api/forecasts?line=' + line).status_code == 400
        assert client.get('/forecasts').status_code == 200
    assert database.get_match_snapshots('m') == before


def test_old_and_uncorrected_score_dip_forecasts_are_not_shown(database):
    notifier = type('Notifier', (), {'send_alert': AsyncMock()})()
    asyncio.run(process_match(payload(), database, notifier, Config()))
    with database._conn() as conn:
        conn.execute("UPDATE match_live_snapshots SET recorded_at=datetime('now','-181 seconds')")
    assert database.latest_live_forecasts() == []
    asyncio.run(process_match(payload(status='Q2 04:00', score='29 - 30'), database, notifier, Config()))
    assert database.latest_live_forecasts() == []
    assert database.get_match_snapshots('m')[-1]['forecast_json'] is None


def test_automatic_final_check_covers_forecasts_without_alerts_and_keeps_first_loss(database):
    from finished_match_service import run_active_match_finished_scan
    notifier = type('Notifier', (), {'send_alert': AsyncMock()})()
    config = Config()
    # Early observations have forecasts but cannot create alerts.
    first = payload(status='Q1 05:00', score='10 - 10', url='https://m.aiscore.com/basketball/match-home-away/m')
    asyncio.run(process_match(first, database, notifier, config))
    before = database.get_match_snapshots('m')[0]
    asyncio.run(process_match({**payload(status='Q1 04:00', score='12 - 12', inplay_total=149), 'url': first['url']}, database, notifier, config))
    assert database.count_match_alerts('m') == 0
    assert len(database.forecast_matches_for_final_check()) == 1
    checker = type('Checker', (), {'last_report': {}, 'check_matches': AsyncMock(return_value=[{
        'match_id': 'm', 'match_name': 'Home - Away', 'is_finished': True,
        'status': 'Finished', 'score': '75 - 75',
    }])})()
    with patch('finished_match_service._finished_checker', return_value=checker):
        checked = asyncio.run(run_active_match_finished_scan(database, config))
    assert checked['forecast_finished_count'] == 1
    assert checked['archive_failed_count'] == checked['moved_count'] == 0
    assert database.forecast_matches_for_final_check() == []
    assert database.get_match_snapshots('m')[0] == before
    summary = next(iter(report(database.db_path)['all_forecasts']['cohorts'].values()))
    assert summary['matches'] == summary['losses'] == 1
    assert summary['wins'] == summary['pending'] == 0
    assert summary['forecast_error']['model_mae'] == 10
    assert summary['forecast_error']['market_mae'] == 7.5
    notifier.send_alert.assert_not_awaited()


def test_all_forecast_report_respects_a_final_received_after_the_cutoff(database):
    notifier = type('Notifier', (), {'send_alert': AsyncMock()})()
    asyncio.run(process_match(payload(), database, notifier, Config()))
    cutoff = datetime.now(timezone.utc) + timedelta(seconds=1)
    database.save_forecast_final_observation('m', '75 - 75', 'Finished', 150)
    with database._conn() as conn:
        conn.execute('UPDATE forecast_match_results SET settled_at=?', ((cutoff + timedelta(hours=1)).isoformat(),))
    summary = next(iter(report(database.db_path, as_of=cutoff)['all_forecasts']['cohorts'].values()))
    assert summary['pending'] == 1 and summary['losses'] == 0
    summary = next(iter(report(database.db_path, as_of=cutoff + timedelta(hours=2))['all_forecasts']['cohorts'].values()))
    assert summary['losses'] == 1
