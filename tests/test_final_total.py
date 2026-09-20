import json
from unittest.mock import patch
from db import Database


def test_final_total_is_saved_updated_and_cleared(tmp_path):
    db=Database(str(tmp_path/'test.db')); db.init()
    alert_id=db.save_alert('m','Home - Away',160,170,'ALT',10)
    db.archive_match_with_display_snapshots('m',{alert_id:{'id':alert_id,'opening_ppm':4}})
    assert db.update_deleted_alert_final_result(alert_id,result='Başarılı',final_score='80 - 75',final_status='FT')
    assert db.get_deleted_alert_by_id(alert_id)['final_total']==155
    db.update_deleted_match_final_observation('m',final_score='81 – 75',final_status='FT')
    assert db.get_deleted_alert_by_id(alert_id)['final_total']==156
    db.update_deleted_match_final_observation('m',final_score='',final_status='FT')
    assert db.get_deleted_alert_by_id(alert_id)['final_total']==156
    db.update_deleted_match_final_observation('m',final_score='Unknown',final_status='FT')
    assert db.get_deleted_alert_by_id(alert_id)['final_total'] is None
    with db._conn() as conn: conn.execute("UPDATE alerts SET result='',final_score='80 - 75',final_total=155 WHERE id=?",(alert_id,))
    db.mark_deleted_match_in_progress('m')
    assert db.get_deleted_alert_by_id(alert_id)['final_total'] is None


def test_legacy_totals_are_prepared_once_without_changing_snapshots(tmp_path):
    db=Database(str(tmp_path/'legacy.db')); db.init()
    alert_id=db.save_alert('m','Home - Away',160,170,'ALT',10)
    snapshot={'id':alert_id,'opening_ppm':4,'final_total':999}
    db.archive_match_with_display_snapshots('m',{alert_id:snapshot})
    with db._conn() as conn:
        conn.execute('ALTER TABLE alerts DROP COLUMN final_total')
        conn.execute("UPDATE alerts SET final_score='80 - 75' WHERE id=?",(alert_id,))
    db.init()
    row=db.get_deleted_alert_by_id(alert_id)
    assert row['final_total']==155
    assert json.loads(row['display_snapshot'])==snapshot
    with patch.object(db,'_final_score_total',side_effect=AssertionError('Recalculation')):
        db.init()
        import dashboard
        assert dashboard._frozen_deleted_alert(row)['final_total']==155
