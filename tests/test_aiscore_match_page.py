import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from aiscore_final_scraper import AiscoreFinishedMatchChecker
from aiscore_match_page import mobile_match_url, normalize_match_page


class MatchPageTests(unittest.TestCase):
    def test_completed_source_overrides_quarter_tabs_and_includes_overtime(self):
        parsed = normalize_match_page({
            'status': 'Q1', 'score': '', 'isFinished': False,
            'sourceMatch': {'id': 'game', 'statusId': 105,
                            'homeScores': [20, 20, 20, 20, 12],
                            'awayScores': [20, 20, 20, 20, 8]},
        }, 'game')
        self.assertTrue(parsed['isFinished'])
        self.assertEqual(parsed['status'], 'Full Time')
        self.assertEqual(parsed['score'], '92 - 88')

    def test_live_source_cannot_be_settled_by_an_unrelated_final_label(self):
        parsed = normalize_match_page({
            'status': 'FT', 'score': '80 - 70', 'isFinished': True,
            'sourceMatch': {'id': 'game', 'statusId': 4, 'matchStatus': 3},
        }, 'game')
        self.assertFalse(parsed['isFinished'])
        self.assertNotEqual(parsed['status'], 'FT')

    def test_wrong_match_is_rejected(self):
        parsed = normalize_match_page({'sourceMatch': {'id': 'other', 'statusId': 10}}, 'game')
        self.assertEqual(parsed['_check_error'], 'match_identity_mismatch')

    def test_bad_period_scores_never_replace_scoreboard(self):
        for values in ([20, 20], [20, 20, 20, 20, None], [20, 20, 20, 20, True]):
            parsed = normalize_match_page({
                'score': '90 - 80', 'sourceMatch': {'id': 'game', 'statusId': 10,
                                                  'homeScores': values, 'awayScores': values},
            }, 'game')
            self.assertEqual(parsed['score'], '90 - 80')

    def test_url_uses_match_overview_and_rejects_other_hosts(self):
        self.assertEqual(mobile_match_url('https://www.aiscore.com/basketball/match-a-b/game/odds?x=1'),
                         'https://m.aiscore.com/basketball/match-a-b/game')
        with self.assertRaises(ValueError):
            mobile_match_url('https://other.test/basketball/match-a-b/game')

    def test_scoreboard_is_read_before_full_page_load_and_fetch_is_cancelled(self):
        page = MagicMock()
        page.url = 'https://m.aiscore.com/basketball/match-a-b/game'
        page.wait_for_function = AsyncMock()
        page.evaluate = AsyncMock(return_value={
            'status': 'Full Time', 'score': '80 - 75', 'isFinished': True,
            'title': 'Basketball', 'pageText': '',
        })
        cancelled = []

        class Session:
            async def fetch(self, url, **kwargs):
                await kwargs['page_setup'](page)
                try:
                    await asyncio.Event().wait()  # A resource never finishes loading.
                finally:
                    cancelled.append(True)

        checker = AiscoreFinishedMatchChecker(page_timeout_ms=100, retry_attempts=0)
        result = asyncio.run(asyncio.wait_for(checker._check_single(Session(), {
            'match_id': 'game', 'url': page.url,
        }), 1))
        self.assertTrue(result['is_finished'])
        self.assertEqual(result['score'], '80 - 75')
        self.assertEqual(cancelled, [True])

    def test_reused_page_cannot_archive_previous_match(self):
        page = MagicMock()
        page.url = 'https://m.aiscore.com/basketball/match-a-b/previous'
        page.wait_for_function = AsyncMock()
        page.evaluate = AsyncMock(return_value={
            'status': 'Full Time', 'score': '80 - 75', 'isFinished': True,
        })

        class Session:
            async def fetch(self, url, **kwargs):
                await kwargs['page_setup'](page)
                await asyncio.Event().wait()

        checker = AiscoreFinishedMatchChecker(page_timeout_ms=100)
        with self.assertRaises(TimeoutError):
            asyncio.run(asyncio.wait_for(checker._check_single(Session(), {
                'match_id': 'game', 'url': 'https://m.aiscore.com/basketball/match-a-b/game',
            }), 0.02))
        page.evaluate.assert_not_awaited()
