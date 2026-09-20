import unittest

from playwright.sync_api import sync_playwright

from aiscore_scoreboard import QUARTER_SCORES_JS
from match_state import normalize_quarter_scores, current_pace_projection


class QuarterScoreboardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.playwright = sync_playwright().start()
        try:
            cls.browser = cls.playwright.chromium.launch(headless=True)
        except Exception:
            cls.playwright.stop()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.browser.close()
        cls.playwright.stop()

    def setUp(self):
        self.page = self.browser.new_page()

    def tearDown(self):
        self.page.close()

    def read(self, body, score):
        self.page.set_content(body)
        return self.page.evaluate(QUARTER_SCORES_JS, score)

    def test_separate_score_cells_ignore_numbers_in_team_names(self):
        result = self.read('''<div class="scoresDetails">
            <div class="content"><span>Home U21</span><span>18</span><span>3</span><span>21</span></div>
            <div class="content"><span>Away U21</span><span>9</span><span>4</span><span>13</span></div>
        </div>''', '21 - 13')
        self.assertEqual(result['home'], [18, 3])
        self.assertEqual(result['away'], [9, 4])
        projection = current_pace_projection(
            '21 - 13', 'Q2 08:00', quarter_scores=result,
        )
        self.assertEqual([row['total'] for row in projection['periods']], [27, 7])
        self.assertEqual([row['ppm'] for row in projection['periods']], [2.7, 3.5])

    def test_total_before_quarters_and_tied_score_preserve_team_order(self):
        result = self.read('''<div class="scoresDetails">
            <div><span>Home</span><b>40</b><span>18</span><span>22</span></div>
            <div><span>Away</span><b>40</b><span>25</span><span>15</span></div>
        </div>''', '40 - 40')
        self.assertEqual(result['home'], [18, 22])
        self.assertEqual(result['away'], [25, 15])

    def test_quarter_columns_and_unplayed_placeholders(self):
        result = self.read('''<div class="scoresDetails">
            <div><span>Q1</span><span>18</span><span>9</span></div>
            <div><span>Q2</span><span>3</span><span>4</span></div>
            <div><span>Q3</span><span>-</span><span>-</span></div>
            <div><span>Q4</span><span>-</span><span>-</span></div>
            <div><span>Total</span><b>21</b><b>13</b></div>
        </div>''', '21 - 13')
        self.assertEqual(result['home'], [18, 3])
        self.assertEqual(result['away'], [9, 4])

    def test_unrelated_rows_and_mismatched_score_are_rejected(self):
        body = '''<div class="scoresDetails">
            <div><span>18</span><span>3</span><span>21</span></div>
            <div><span>9</span><span>4</span><span>13</span></div>
        </div><div>Home U21 20 3 23</div><div>Away U21 9 4 13</div>'''
        self.assertEqual(self.read(body, '23 - 13'), {})
        self.assertEqual(normalize_quarter_scores(
            {'home': [18, 3], 'away': [9, 4]}, '23 - 13',
        ), {})

    def test_single_team_cannot_fill_both_sides_of_tied_score(self):
        self.assertEqual(self.read('''<div class="scoresDetails"><div>
            <div><span>18</span><span>22</span><span>40</span></div>
        </div></div>''', '40 - 40'), {})
