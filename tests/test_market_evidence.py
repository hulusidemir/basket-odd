import unittest

from market_evidence import MARKET_EVIDENCE_VERSION, _classify_edge, build_market_evidence


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

    def test_sustained_high_production_supports_opposite_of_alt_signal(self):
        home = _team(points=38, fgm=14, fga=25, fg3m=4, fg3a=9, ftm=6, fta=7)
        away = _team(points=37, fgm=13, fga=24, fg3m=5, fg3a=10, ftm=6, fta=7)

        result = build_market_evidence(_match(home, away, score="38 - 37"), "ALT")

        self.assertEqual(result["version"], MARKET_EVIDENCE_VERSION)
        self.assertEqual(result["code"], "opposes_signal")
        self.assertEqual(result["label"], "SİNYALİN TERSİ DESTEKLENİYOR")
        self.assertEqual(result["symbol"], "⇄")
        self.assertEqual(result["metrics"]["supported_direction"], "ÜST")
        self.assertLessEqual(result["metrics"]["signal_edge"], -3)
        self.assertGreater(result["metrics"]["opening_move_alignment_pct"], 200)

    def test_near_live_fair_is_neutral_instead_of_market_support(self):
        home = _team(points=30, fgm=11, fga=25, fg3m=3, fg3a=9, ftm=5, fta=6)
        away = _team(points=30, fgm=11, fga=25, fg3m=3, fg3a=9, ftm=5, fta=6)

        result = build_market_evidence(
            _match(home, away, score="30 - 30", opening=145, live=155),
            "ALT",
        )

        self.assertEqual(result["code"], "mixed")
        self.assertEqual(result["label"], "NET AYRIŞMA YOK")
        self.assertLess(abs(result["metrics"]["signal_edge"]), 3)
        self.assertIn("karar bandında", result["primary_reason"])

    def test_falling_line_uses_the_same_edge_rule_for_ust(self):
        home = _team(points=25, fgm=9, fga=22, fg3m=2, fg3a=7, ftm=5, fta=6)
        away = _team(points=25, fgm=9, fga=22, fg3m=2, fg3a=7, ftm=5, fta=6)

        result = build_market_evidence(
            _match(home, away, score="25 - 25", opening=160, live=150),
            "ÜST",
        )

        self.assertEqual(result["code"], "opposes_signal")
        self.assertEqual(result["metrics"]["supported_direction"], "ALT")
        self.assertLessEqual(result["metrics"]["signal_edge"], -3)

    def test_decision_band_is_symmetric(self):
        self.assertEqual(_classify_edge(3.0, "ALT")["code"], "supports_signal")
        self.assertEqual(_classify_edge(2.99, "ALT")["code"], "mixed")
        self.assertEqual(_classify_edge(-2.99, "ALT")["code"], "mixed")
        self.assertEqual(_classify_edge(-3.0, "ALT")["code"], "opposes_signal")

        self.assertEqual(_classify_edge(3.0, "ÜST")["supported_direction"], "ÜST")
        self.assertEqual(_classify_edge(-3.0, "ÜST")["supported_direction"], "ALT")

    def test_move_is_split_into_realised_score_and_future_revision(self):
        home = _team(points=38, fgm=14, fga=25, fg3m=4, fg3a=9, ftm=6, fta=7)
        away = _team(points=37, fgm=13, fga=24, fg3m=5, fg3a=10, ftm=6, fta=7)

        result = build_market_evidence(_match(home, away, score="38 - 37"), "ALT")
        metrics = result["metrics"]

        self.assertAlmostEqual(
            metrics["realised_scoring_surprise"] + metrics["market_future_revision"],
            metrics["live_total"] - metrics["opening_total"],
            places=1,
        )
        self.assertAlmostEqual(
            metrics["realised_scoring_surprise"] + metrics["model_future_revision"],
            metrics["stats_fair_total"] - metrics["opening_total"],
            places=1,
        )
        self.assertAlmostEqual(
            metrics["model_remaining_points"] - metrics["market_remaining_points"],
            -metrics["signal_edge"],
            places=1,
        )
        self.assertAlmostEqual(
            metrics["naive_remaining_points"]
            + metrics["pace_regression_points"]
            + metrics["efficiency_regression_points"]
            + metrics["script_adjustment"],
            metrics["model_remaining_points"],
            places=1,
        )

    def test_stale_boxscore_is_not_used(self):
        home = _team(points=30, fgm=11, fga=25, fg3m=3, fg3a=9, ftm=5, fta=6)
        away = _team(points=30, fgm=11, fga=25, fg3m=3, fg3a=9, ftm=5, fta=6)

        result = build_market_evidence(_match(home, away, score="35 - 35"), "ALT")

        self.assertEqual(result["code"], "insufficient")
        self.assertIn("eşleşmiyor", result["primary_reason"])

    def test_missing_personal_fouls_do_not_block_core_evidence(self):
        home = _team(points=30, fgm=11, fga=25, fg3m=3, fg3a=9, ftm=5, fta=6)
        away = _team(points=30, fgm=11, fga=25, fg3m=3, fg3a=9, ftm=5, fta=6)
        home["pf"] = None
        away["pf"] = None

        result = build_market_evidence(_match(home, away, score="30 - 30"), "ALT")

        self.assertEqual(result["code"], "supports_signal")

    def test_absent_upstream_boxscore_has_a_specific_reason(self):
        match = _match({}, {}, score="30 - 30")
        match["team_stats"]["has_stats"] = False

        result = build_market_evidence(match, "ALT")

        self.assertEqual(result["code"], "insufficient")
        self.assertIn("yayınlamıyor", result["primary_reason"])
        self.assertFalse(result["metrics"]["has_stats"])
        self.assertIn("points", result["metrics"]["home_missing_fields"])

    def test_unknown_boxscore_availability_does_not_claim_it_is_unpublished(self):
        match = _match({}, {}, score="30 - 30")
        match["team_stats"]["has_stats"] = None

        result = build_market_evidence(match, "ALT")

        self.assertEqual(result["code"], "insufficient")
        self.assertIn("snapshot'ta bulunamadı", result["primary_reason"])
        self.assertNotIn("yayınlamıyor", result["primary_reason"])
        self.assertIsNone(result["metrics"]["has_stats"])

    def test_unknown_competition_is_not_treated_as_verified_4x10(self):
        home = _team(points=30, fgm=11, fga=25, fg3m=3, fg3a=9, ftm=5, fta=6)
        away = _team(points=30, fgm=11, fga=25, fg3m=3, fg3a=9, ftm=5, fta=6)

        result = build_market_evidence(
            _match(home, away, score="30 - 30", tournament="Unknown"),
            "ALT",
        )

        self.assertEqual(result["code"], "insufficient")
        self.assertIn("doğrulanmış 4x10", result["primary_reason"])

    def test_live_total_below_current_score_is_rejected(self):
        home = _team(points=38, fgm=14, fga=25, fg3m=4, fg3a=9, ftm=6, fta=7)
        away = _team(points=37, fgm=13, fga=24, fg3m=5, fg3a=10, ftm=6, fta=7)

        result = build_market_evidence(
            _match(home, away, score="38 - 37", live=74),
            "ALT",
        )

        self.assertEqual(result["code"], "insufficient")
        self.assertIn("mevcut toplam skordan düşük", result["primary_reason"])

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
