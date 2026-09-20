import tempfile
import unittest
from pathlib import Path
from urllib.parse import urlsplit
from unittest.mock import patch

from playwright.sync_api import sync_playwright

from db import Database


class FinishedScanBrowserTests(unittest.TestCase):
    def test_progress_reload_and_partial_completion(self):
        import dashboard
        with tempfile.TemporaryDirectory() as directory:
            db = Database(str(Path(directory) / 'test.db'))
            db.init()
            job = {'state': 'idle', 'job_id': None}
            posts = []
            errors = []
            with patch.object(dashboard, 'db', db), sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                try:
                    page = browser.new_page()
                    client = dashboard.app.test_client()

                    def respond(route):
                        path = urlsplit(route.request.url).path
                        if path == '/api/alerts/check-finished':
                            if route.request.method == 'POST':
                                posts.append(True)
                                job.update(state='running', job_id='scan-1', progress={
                                    'phase': 'checking', 'tracked_count': 3,
                                    'processed_count': 1, 'moved_count': 1,
                                })
                            route.fulfill(status=202 if route.request.method == 'POST' else 200, json=job)
                            return
                        response = client.open(path, method=route.request.method)
                        route.fulfill(status=response.status_code, body=response.data,
                                      content_type=response.content_type)

                    page.route('**/*', respond)
                    page.on('pageerror', lambda error: errors.append(str(error)))
                    page.goto('http://basket.test/')
                    page.locator('#finishedBtn').click()
                    page.wait_for_function("document.querySelector('#finishedStatus').textContent.includes('1/3')")
                    self.assertTrue(page.locator('#finishedBtn').is_disabled())
                    self.assertEqual(len(posts), 1)
                    page.reload()
                    page.wait_for_function("document.querySelector('#finishedStatus').textContent.includes('1/3')")
                    self.assertTrue(page.locator('#finishedBtn').is_disabled())
                    self.assertEqual(len(posts), 1)
                    job.update(state='completed', result={
                        'moved_count': 2, 'check_failed_count': 1,
                        'message': '2 maç arşivlendi; 1 maça ulaşılamadı.',
                    })
                    page.wait_for_function("!document.querySelector('#finishedBtn').disabled")
                    self.assertIn('2 maç arşivlendi', page.locator('#finishedStatus').inner_text())
                    self.assertIn('error', page.locator('#finishedStatus').get_attribute('class'))
                    # Refreshing the table cannot erase the scan result.
                    page.locator('#refreshBtn').click()
                    self.assertIn('2 maç arşivlendi', page.locator('#finishedStatus').inner_text())
                    self.assertEqual(errors, [])
                finally:
                    browser.close()
