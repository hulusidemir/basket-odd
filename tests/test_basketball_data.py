import json
import subprocess
import unittest

from aiscore_basketball_data import BASKETBALL_DATA_JS, BASKETBALL_READY_JS, normalize_basketball_data


def observation():
    # A slow-looking 48 points can coexist with 49 field-goal attempts.
    totals = {"points": "48", "fieldGoals": "14-49", "threePoints": "10-31",
              "freeThrows": "10-15", "offensiveRebounds": "9", "turnovers": "7",
              "personalFouls": "15"}
    return {"match_id": "sample", "home_scores": [20, 28, 0, 0, 0],
            "away_scores": [20, 28, 0, 0, 0], "boxscore": {"home": totals.copy(), "away": totals.copy()},
            "team_stats": {"1": {"home": "10", "away": "10"},
                           "2": {"home": "4", "away": "4"},
                           "3": {"home": "10", "away": "10"}}}


class BasketballDataTests(unittest.TestCase):
    def test_readiness_waits_for_all_identity_fields_and_reports_failed_field(self):
        script = '''const window = {$nuxt: {$store: {state: {basketball: {
            basketballDetailMatchData: {match: {id: 'sample', statusId: 4,
                homeScores: [20,28,0,0,0], awayScores: [20,28,0,0,0]}},
            detailMatchId: 'previous'
        }}}}};
        const location = {pathname: '/basketball/match-home-away/sample/odds'};
        const document = {querySelector: () => null};
        '''
        script += 'const ready = (' + BASKETBALL_READY_JS + ');\n'
        script += 'const read = (' + BASKETBALL_DATA_JS + ');\n'
        script += '''const before = {ready: ready('sample'), data: read('sample')};
        window.$nuxt.$store.state.basketball.detailMatchId = 'sample';
        const after = {ready: ready('sample'), data: read('sample')};
        console.log(JSON.stringify({before, after}));'''
        result = json.loads(subprocess.run(['node', '-e', script], check=True,
                                           capture_output=True, text=True).stdout)
        self.assertFalse(result['before']['ready'])
        checks = result['before']['data']['identity_checks']
        self.assertEqual(checks, {'source_match_id': True, 'detail_match_id': False, 'url_match_id': True})
        self.assertTrue(result['after']['ready'])
        self.assertNotIn('error', result['after']['data'])
        normalized = normalize_basketball_data(result['before']['data'], 'sample')
        self.assertEqual(normalized['identity_checks'], checks)

    def test_exact_attempts_and_two_pointers(self):
        result = normalize_basketball_data(observation(), "sample")
        self.assertTrue(result["full_boxscore_available"])
        home = result["teams"]["home"]["boxscore"]
        self.assertEqual((home["two_points_made"], home["two_points_attempted"]), (4, 18))
        self.assertEqual(home["field_goals_attempted"], 49)
        self.assertIsNone(home["assists"])
        self.assertFalse(result["provider_freshness_verified"])

    def test_wrong_game_and_different_signal_score(self):
        self.assertFalse(normalize_basketball_data(observation(), "another")["available"])
        self.assertEqual(normalize_basketball_data(observation(), "sample", expected_score="50-48")["error"],
                         "observation_score_mismatch")

    def test_source_errors_remain_distinct(self):
        self.assertEqual(normalize_basketball_data({"error": "statistics_fetch_timeout"}, "sample"),
                         {"available": False, "error": "statistics_fetch_timeout"})

    def test_delayed_boxscore_is_not_used(self):
        payload = observation()
        payload["home_scores"][1] += 2
        result = normalize_basketball_data(payload, "sample")
        self.assertFalse(result["full_boxscore_available"])
        self.assertIsNone(result["teams"]["home"]["boxscore"])
        self.assertIn("home:boxscore_score_mismatch", result["issues"])
        self.assertIsNone(result["teams"]["home"]["basic_shots"]["two_points_made"])

    def test_missing_data_is_not_zero(self):
        payload = observation()
        payload["boxscore"] = {}
        payload["team_stats"] = {}
        result = normalize_basketball_data(payload, "sample")
        self.assertFalse(result["full_boxscore_available"])
        self.assertIsNone(result["teams"]["home"]["boxscore"])

    def test_bad_shot_arithmetic_is_rejected(self):
        payload = observation()
        payload["boxscore"]["home"]["fieldGoals"] = "10-12"
        result = normalize_basketball_data(payload, "sample")
        self.assertIsNone(result["teams"]["home"]["boxscore"])

    def test_valid_shots_but_wrong_points_are_rejected(self):
        payload = observation()
        payload["boxscore"]["home"]["freeThrows"] = "9-15"
        result = normalize_basketball_data(payload, "sample")
        self.assertIn("home:boxscore_points_mismatch", result["issues"])

    def test_overtime_is_included_extra_non_score_slots_are_not(self):
        payload = observation()
        payload["home_scores"] = [20, 20, 0, 0, 8, 3, 4]
        self.assertTrue(normalize_basketball_data(payload, "sample")["full_boxscore_available"])


if __name__ == "__main__":
    unittest.main()
