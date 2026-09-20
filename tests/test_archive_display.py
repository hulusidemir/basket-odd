import json
from contextlib import ExitStack
from unittest.mock import patch

import pytest

from db import Database


@pytest.fixture
def context(tmp_path):
    import dashboard
    database = Database(str(tmp_path / 'archive.db'))
    database.init()
    with patch.object(dashboard, 'db', database):
        yield dashboard, database, dashboard.app.test_client()


def seed(database, match_id='current', tournament='EuroLeague', opening=142, live=132):
    return database.save_alert(match_id, 'Home - Away', opening, live, 'ÜST', 10,
                               tournament=tournament, score='32 - 28', status='Q3 10:00')


@pytest.mark.parametrize('bulk', [False, True])
def test_archiving_copies_live_fields_and_frozen_team_history(context, bulk):
    dashboard, database, client = context
    previous = seed(database, 'previous')
    dashboard._archive_active_match('previous')
    with database._conn() as conn:
        conn.execute("UPDATE alerts SET result='Başarılı', final_score='80 - 70' WHERE id=?", (previous,))
    alert_id = seed(database)
    database.set_match_statuses('current', followed=True)
    entry = database.add_signal_list_entry('white', 'team', 'Home')
    live = next(row for row in client.get('/api/alerts').get_json() if row['id'] == alert_id)
    history = client.get(f'/api/alerts/{alert_id}/team-history').get_json()
    response = client.post('/api/clear', json={}) if bulk else client.delete(f'/api/alerts/{alert_id}')
    assert response.status_code == 200
    stored = database.get_deleted_alert_by_id(alert_id)
    snapshot = json.loads(stored['display_snapshot'])
    for key, value in live.items():
        assert snapshot[key] == value, key
    assert snapshot['opening_ppm'] == 3.55
    assert snapshot['ppm_comparison']['remaining_time'] == '20:00'
    assert snapshot['ppm_comparison']['required_change_pct'] == 20
    assert snapshot['team_history']['teams'] == history['teams']
    assert 'team_history' not in snapshot['team_history']['teams'][0]['matches'][0]
    assert snapshot['team_history']['teams'][0]['matches'][0]['final_total'] == 150
    # Changes to source facts, lists, and old team results must not change snapshots.
    database.delete_signal_list_entry(entry['id'])
    with database._conn() as conn:
        conn.execute("UPDATE alerts SET opening=999, score='99 - 99', status='Q4 00:01', followed=0, final_score='100 - 90', result='Başarılı' WHERE id=?", (alert_id,))
        conn.execute("UPDATE alerts SET result='Başarısız', final_score='60 - 60' WHERE id=?", (previous,))
    with ExitStack() as stack:
        for name in ['current_pace_projection', '_raw_alert', '_build_live_dashboard_rows',
                     '_team_history', '_history_display_row', 'build_signal_list_markers']:
            stack.enter_context(patch.object(dashboard, name, side_effect=AssertionError('Archive recalculated: '+name)))
        archived = next(row for row in client.get('/api/deleted-matches').get_json() if row['id'] == alert_id)
        single = client.get(f'/api/deleted-matches/{alert_id}/details').get_json()
        archived_history = client.get(f'/api/alerts/{alert_id}/team-history').get_json()
    for key, value in live.items():
        if key not in {'final_score', 'final_status', 'result', 'settled_at', 'deleted_at', 'result_source'}:
            assert archived[key] == value, key
    assert single == archived
    assert archived_history['teams'] == history['teams']
    assert archived['final_score'] == '100 - 90'
    assert database.get_deleted_alert_by_id(alert_id)['display_snapshot'] == stored['display_snapshot']


def test_old_snapshots_keep_available_ppm_and_never_fill_missing_fields(context):
    dashboard, database, client = context
    alert_id = seed(database)
    old = {'id':alert_id, 'opening':142, 'pace_game_minutes':40, 'pace_projection':120,
           'pace_ppm':3, 'ppm_comparison':{'required_ppm':3.6, 'remaining_minutes':20}}
    database.archive_match_with_display_snapshots('current', {alert_id:old})
    with patch.object(dashboard, 'current_pace_projection', side_effect=AssertionError('Recalculation')):
        row = client.get(f'/api/deleted-matches/{alert_id}/details').get_json()
    assert row['opening_ppm'] is None
    assert row['team_history'] is None
    assert row['ppm_comparison'] == old['ppm_comparison']
    assert 'required_change_pct' not in row['ppm_comparison']
    assert 'remaining_time' not in row['ppm_comparison']


@pytest.mark.parametrize('tournament,opening,expected', [('EuroLeague',160,4), ('NBA',240,5)])
def test_opening_ppm_is_live_fact_independent_of_score_and_clock(context, tournament, opening, expected):
    dashboard, _, _ = context
    for score, status in [('', ''), ('32 - 28','Q3 10:00'), ('90 - 90','OT1 01:00')]:
        row=dashboard._raw_alert({'opening':opening, 'live':opening-10, 'tournament':tournament,
                                  'match_name':'Home - Away', 'score':score,'status':status})
        assert row['opening_ppm'] == expected


def test_signal_clock_label_is_prepared_live_and_frozen(context):
    dashboard, _, _ = context
    row = dashboard._raw_alert({'alerted_at':'2026-09-05 23:15:00'})
    assert row['signal_time'] == '02:15'
    old = dashboard._frozen_deleted_alert({'alerted_at':'2026-09-05 23:15:00'})
    assert old['signal_time'] is None
