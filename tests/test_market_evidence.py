import unittest

from market_evidence import build_market_evidence


def _team(*, points, fgm, fga, fg3m, fg3a, ftm, fta, oreb=4, dreb=10, tov=5, pf=8):
    return {
        "points": points,
        "fgm": fgm,
        "fga": fga,
        "fg3m": fg3m,
        "fg3a": fg3a,
        "ftm": ftm,
        "fta": fta,
        "oreb": oreb,
        "dreb": dreb,
        "reb": oreb + dreb,
        "tov": tov,
        "pf": pf,
    }


def _match(home, away, *, score, opening=160, live=170, status="Q2 05:00", tournament="FIBA Europe Cup"):
    return {
        "match_name": "Home - Away",
        "tournament": tournament,
        "status": status,
        "score": score,
        "opening_total": opening,
        "inplay_total": live,
        "team_stats": {
            "version": "aiscore_team_stats_v1",
            "source": "nuxt_boxscore_team_totals",
            "has_stats": True,
            "home": home,
            "away": away,
            "current_period_flow": {
                "period": 2,
                "foul_events": 2,
                "free_throw_events": 2,
                "turnover_events": 2,
            },
        },
    }


class MarketEvidenceTests(unittest.TestCase):
    def test_low_scoring_normal_pace_supports_alt_against_raised_line(self):
        home = _team(points=30, fgm=11, fga=25, fg3m=3, fg3a=9, ftm=5, fta=6)
        away = _team(points=30, fgm=11, fga=25, fg3m=3, fg3a=9, ftm=5, fta=6)

        result = build_market_evidence(_match(home, away, score="30 - 30"), "ALT")

        self.assertEqual(result["code"], "supports_signal")
        self.assertEqual(result["symbol"], "✓")
        self.assertGreaterEqual(result["metrics"]["signal_edge"], 3)

    def test_sustained_high_production_supports_market_move(self):
        home = _team(points=38, fgm=14, fga=25, fg3m=4, fg3a=9, ftm=6, fta=7)
        away = _team(points=37, fgm=13, fga=24, fg3m=5, fg3a=10, ftm=6, fta=7)

        result = build_market_evidence(_match(home, away, score="38 - 37"), "ALT")

        self.assertEqual(result["code"], "supports_market")
        self.assertEqual(result["symbol"], "⇄")
        self.assertGreaterEqual(result["metrics"]["movement_explained_pct"], 70)

    def test_stale_boxscore_is_not_used(self):
        home = _team(points=30, fgm=11, fga=25, fg3m=3, fg3a=9, ftm=5, fta=6)
        away = _team(points=30, fgm=11, fga=25, fg3m=3, fg3a=9, ftm=5, fta=6)

        result = build_market_evidence(_match(home, away, score="35 - 35"), "ALT")

        self.assertEqual(result["code"], "insufficient")
        self.assertIn("eşleşmiyor", result["primary_reason"])

    def test_non_4x10_game_is_not_evaluated(self):
        home = _team(points=30, fgm=11, fga=25, fg3m=3, fg3a=9, ftm=5, fta=6)
        away = _team(points=30, fgm=11, fga=25, fg3m=3, fg3a=9, ftm=5, fta=6)
        match = _match(
            home,
            away,
            score="30 - 30",
            status="Q2 06:00",
            tournament="NBA",
        )

        result = build_market_evidence(match, "ALT")

        self.assertEqual(result["code"], "insufficient")
        self.assertIn("4x10", result["primary_reason"])


if __name__ == "__main__":
    unittest.main()
