import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from db import Database
from signal_lists import build_signal_blacklist_matches, build_signal_list_profile


@pytest.fixture
def database(tmp_path):
    database = Database(str(tmp_path / 'lists.db'))
    database.init()
    return database


def test_switch_is_exclusive_idempotent_and_keeps_entity_id(database):
    first = database.add_signal_list_entry('black', 'team', '  İSTANBUL   GÜCÜ ')
    moved = database.add_signal_list_entry('white', 'team', 'istanbul gucu')
    again = database.add_signal_list_entry('white', 'team', 'Istanbul Gucu')
    assert first['id'] == moved['id'] == again['id']
    assert len(database.list_signal_list_entries()) == 1
    assert again['list_type'] == 'white'
    assert database.delete_signal_list_entry(again['id'])
    assert database.list_signal_list_entries() == []


def test_team_and_league_with_same_name_stay_independent(database):
    database.add_signal_list_entry('black', 'team', 'Same')
    database.add_signal_list_entry('white', 'league', 'Same')
    assert len(database.list_signal_list_entries()) == 2


def test_concurrent_switches_cannot_create_conflicting_entries(database):
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda kind: database.add_signal_list_entry(kind, 'team', 'Home'), ['black', 'white'] * 6))
    assert all(row is not None for row in results)
    assert len(database.list_signal_list_entries()) == 1


def test_old_conflicts_are_migrated_without_changing_signals(database):
    alert_id = database.save_alert('legacy', 'Home - Away', 160, 171, 'ALT', 11)
    original = database.get_alert(alert_id)
    with database._conn() as conn:
        conn.execute('DROP INDEX idx_signal_lists_entity')
        conn.executemany('INSERT INTO signal_lists (list_type, scope, value, normalized_value) VALUES (?, ?, ?, ?)', [
            ('white', 'team', 'İstanbul Gücü', 'i\u0307stanbul gucu'),
            ('black', 'team', 'Istanbul Gucu', 'istanbul gucu'),
            ('black', 'team', 'ISTANBUL  GUCU', 'istanbul  gucu'),
            ('white', 'league', 'Independent', 'independent'),
        ])
    database.init()
    rows = database.list_signal_list_entries()
    assert len(rows) == 2
    assert next(row for row in rows if row['scope'] == 'team')['list_type'] == 'black'
    assert database.get_alert(alert_id) == original
    database.init()
    assert database.list_signal_list_entries() == rows
    with pytest.raises(sqlite3.IntegrityError), database._conn() as conn:
        conn.execute("INSERT INTO signal_lists (list_type, scope, value, normalized_value) VALUES ('white', 'team', 'Istanbul Gucu', 'istanbul gucu')")


@pytest.mark.parametrize('entries,blocked', [
    ([], False),
    ([('black', 'league', 'League')], True),
    ([('white', 'team', 'Unrelated')], False),
    ([('black', 'league', 'League'), ('white', 'team', 'Home')], False),
    ([('black', 'league', 'League'), ('white', 'team', 'Away')], False),
    ([('black', 'team', 'Away'), ('white', 'team', 'Home')], True),
    ([('black', 'team', 'Home'), ('white', 'league', 'League')], True),
    ([('black', 'team', 'Hom')], False),
])
def test_rules_apply_to_new_signals_and_pending_deliveries(database, entries, blocked):
    from main import process_match, retry_pending_telegram_deliveries
    from config import Config
    from live_signals import SignalDecision
    for kind, scope, value in entries:
        database.add_signal_list_entry(kind, scope, value)
    profile = build_signal_list_profile(database.list_signal_list_entries())
    match = {'match_id': 'new', 'match_name': 'Home - Away', 'tournament': 'League',
             'status': 'Q2 05:00', 'score': '40 - 35', 'opening_total': 160, 'inplay_total': 171}
    assert bool(build_signal_blacklist_matches(match, profile)) is blocked
    config = Config()
    notifier = SimpleNamespace(send_alert=AsyncMock(return_value={'recipient': 1}))

    with patch('main.evaluate_live_signal', return_value=SignalDecision("opening", 160, 11, 10, "ALT", 2, "")):
        asyncio.run(process_match(match, database, notifier, config))

    assert (database.get_alert(1) is None) is blocked
    assert notifier.send_alert.await_count == (0 if blocked else 1)
    pending = database.save_alert('pending', 'Home - Away', 160, 171, 'ALT', 11,
                                  tournament='League', status='Q2 05:00', telegram_required=True)
    notifier.send_alert.reset_mock()
    asyncio.run(retry_pending_telegram_deliveries(database, notifier))
    assert database.get_alert(pending)['telegram_status'] == ('cancelled' if blocked else 'sent')
    assert notifier.send_alert.await_count == (0 if blocked else 1)


def test_api_add_move_remove_and_validation(database):
    import dashboard
    with patch.object(dashboard, 'db', database):
        client = dashboard.app.test_client()
        for payload in [['bad'], 'bad', {'list_type':'black', 'scope':'team', 'value':['bad']},
                        {'list_type':'black', 'scope':'team', 'value':'x' * 201},
                        {'list_type':'unknown', 'scope':'team', 'value':'Home'},
                        {'list_type':'white', 'scope':'team', 'value':'   '}]:
            assert client.post('/api/signal-lists', json=payload).status_code == 400
        first = client.post('/api/signal-lists', json={'list_type':'black', 'scope':'team', 'value':'Home'}).get_json()['entry']
        moved = client.post('/api/signal-lists', json={'list_type':'white', 'scope':'team', 'value':'HOME'}).get_json()['entry']
        assert moved['id'] == first['id']
        assert client.get('/api/signal-lists').get_json() == [moved]
        assert client.delete(f"/api/signal-lists/{first['id']}").status_code == 200
        assert client.get('/api/signal-lists').get_json() == []
        assert client.delete(f"/api/signal-lists/{first['id']}").status_code == 404
