import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from db import Database
from finished_scan_jobs import FinishedScanJobs
import finished_scan_jobs


class DashboardFinishedCheckRouteTests(unittest.TestCase):
    def setUp(self):
        import dashboard
        self.dashboard = dashboard
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Database(str(Path(self.temp.name) / 'test.db'))
        self.db.init()
        self.jobs = FinishedScanJobs()
        for target, name, value in (
            (dashboard, 'db', self.db),
            (dashboard, 'active_scan_jobs', self.jobs),
            (finished_scan_jobs, 'active_scan_jobs', self.jobs),
        ):
            p = patch.object(target, name, value)
            p.start()
            self.addCleanup(p.stop)
        self.client = dashboard.app.test_client()

    def finish(self):
        self.jobs._thread.join(timeout=3)
        self.assertFalse(self.jobs._thread.is_alive())
        return self.client.get('/api/alerts/check-finished').get_json()

    def test_button_starts_job_and_poll_returns_archive_summary(self):
        alert_id = self.db.save_alert('match-1', 'Home - Away', 160, 170, 'ALT', 10,
                                     score='70 - 70', status='Q4 01:00')

        async def scan(db, config, before_delete, on_progress):
            on_progress({'tracked_count': 1, 'processed_count': 1})
            before_delete('match-1')
            return {'moved_count': 1, 'checked_count': 1, 'message': '1 maç arşivlendi.'}

        with patch.object(finished_scan_jobs, 'run_active_match_finished_scan', scan):
            response = self.client.post('/api/alerts/check-finished')
            self.assertEqual(response.status_code, 202)
            self.assertEqual(response.get_json()['state'], 'running')
            result = self.finish()
        self.assertEqual(result['state'], 'completed')
        self.assertEqual(result['result']['moved_count'], 1)
        self.assertEqual(result['progress']['processed_count'], 1)
        self.assertTrue(self.db.get_deleted_alert_by_id(alert_id)['display_snapshot'])

    def test_repeated_click_and_worker_attach_to_same_job(self):
        entered, release = threading.Event(), threading.Event()

        async def scan(db, config, before_delete, on_progress):
            entered.set()
            await __import__('asyncio').to_thread(release.wait, 3)
            return {'checked_count': 0, 'message': 'Bitti.'}

        with patch.object(finished_scan_jobs, 'run_active_match_finished_scan', scan):
            try:
                first = self.client.post('/api/alerts/check-finished').get_json()
                self.assertTrue(entered.wait(1))
                second = self.client.post('/api/alerts/check-finished').get_json()
                worker = finished_scan_jobs.start_active_finished_scan(
                    self.db, None, None, source='scheduled',
                )
                self.assertEqual(first['job_id'], second['job_id'])
                self.assertEqual(first['job_id'], worker['job_id'])
                self.assertEqual(self.client.get('/api/alerts/check-finished').get_json()['state'], 'running')
            finally:
                release.set()
                self.finish()

    def test_failed_source_is_visible_and_a_new_scan_can_start(self):
        summary = {'checked_count': 0, 'check_failed_count': 2,
                   'message': 'Maçların bitiş durumu doğrulanamadı.'}
        with patch.object(finished_scan_jobs, 'run_active_match_finished_scan', new=AsyncMock(return_value=summary)):
            self.client.post('/api/alerts/check-finished')
            first = self.finish()
            self.assertEqual(first['state'], 'failed')
            self.assertEqual(first['error'], summary['message'])
            self.client.post('/api/alerts/check-finished')
            second = self.finish()
            self.assertNotEqual(first['job_id'], second['job_id'])

    def test_partial_scan_is_completed_with_explicit_failure_counts(self):
        summary = {'checked_count': 1, 'check_failed_count': 1, 'moved_count': 1,
                   'message': '1 arşivlendi; 1 okunamadı.'}
        with patch.object(finished_scan_jobs, 'run_active_match_finished_scan', new=AsyncMock(return_value=summary)):
            self.client.post('/api/alerts/check-finished')
            job = self.finish()
        self.assertEqual(job['state'], 'completed')
        self.assertEqual(job['result']['check_failed_count'], 1)

    def test_job_errors_are_safe_and_terminal(self):
        for error in (TimeoutError(), RuntimeError('private browser details'),
                      self.dashboard.FinishedCheckBusy('Başka bir final kontrolü çalışıyor.')):
            with self.subTest(error=type(error).__name__), patch.object(
                finished_scan_jobs, 'run_active_match_finished_scan', new=AsyncMock(side_effect=error),
            ):
                self.client.post('/api/alerts/check-finished')
                job = self.finish()
                self.assertEqual(job['state'], 'failed')
                self.assertTrue(job['finished_at'])
                self.assertNotIn('private', job['error'])


if __name__ == '__main__':
    unittest.main()
