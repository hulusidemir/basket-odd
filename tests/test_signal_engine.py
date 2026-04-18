import unittest

from signal_engine import decide_signal
from signal_features import build_signal_features


class DummyConfig:
    MINIMUM_CONFIDENCE_FOR_SIGNAL = 62
    MINIMUM_SUPPORTING_SOURCES = 2
    MINIMUM_PROJECTION_EDGE = 4
    CONTINUATION_CONFIDENCE_THRESHOLD = 58
    CONTRARIAN_CONFIDENCE_THRESHOLD = 68


def quality(support: int, against: int = 0, names: list[str] | None = None) -> dict:
    source_names = names or ["projeksiyon", "takım profili", "H2H geçmiş"][:support]
    supporting = [{"name": name, "vote": "", "detail": ""} for name in source_names[:support]]
    opposing = [{"name": f"karşı {i}", "vote": "", "detail": ""} for i in range(against)]
    return {
        "support_count": support,
        "against_count": against,
        "neutral_count": max(0, 4 - support - against),
        "supporting_signals": supporting,
        "opposing_signals": opposing,
    }


class SignalEngineTest(unittest.TestCase):
    def features(
        self,
        opening: float,
        live: float,
        score: str,
        status: str = "Q2 05:00",
        recent_snapshots: list[dict] | None = None,
    ):
        return build_signal_features(
            {
                "opening_total": opening,
                "prematch_total": None,
                "inplay_total": live,
                "status": status,
                "score": score,
                "match_name": "Home - Away",
                "tournament": "Test League",
            },
            reference_total=opening,
            reference_label="Açılış",
            recent_snapshots=recent_snapshots,
        )

    def test_line_down_low_pace_continuation_under(self):
        features = self.features(170.0, 155.0, "25-25")
        decision = decide_signal(
            features,
            {"ALT": quality(2, names=["projeksiyon", "takım profili"]), "ÜST": quality(0)},
            DummyConfig(),
        )
        self.assertTrue(decision.should_alert)
        self.assertEqual(decision.direction, "ALT")
        self.assertEqual(decision.action, "UNDER")

    def test_line_down_does_not_auto_create_over(self):
        features = self.features(170.0, 155.0, "25-25")
        decision = decide_signal(
            features,
            {"ALT": quality(1), "ÜST": quality(0)},
            DummyConfig(),
        )
        self.assertFalse(decision.should_alert)
        self.assertIn(decision.action, {"PASS", "LOW_CONFIDENCE_PASS"})

    def test_line_down_can_create_contrarian_over_with_real_edge(self):
        features = self.features(170.0, 155.0, "62-58")
        decision = decide_signal(
            features,
            {"ALT": quality(0), "ÜST": quality(3, names=["projeksiyon", "takım profili", "H2H geçmiş"])},
            DummyConfig(),
        )
        self.assertTrue(decision.should_alert)
        self.assertEqual(decision.direction, "ÜST")
        self.assertEqual(decision.action, "CONTRARIAN_OVER")

    def test_line_up_high_pace_continuation_over(self):
        features = self.features(155.0, 170.0, "62-58")
        decision = decide_signal(
            features,
            {"ALT": quality(0), "ÜST": quality(2, names=["projeksiyon", "takım profili"])},
            DummyConfig(),
        )
        self.assertTrue(decision.should_alert)
        self.assertEqual(decision.direction, "ÜST")
        self.assertEqual(decision.action, "OVER")

    def test_recent_cooling_can_upgrade_under(self):
        features = self.features(
            170.0,
            155.0,
            "25-25",
            recent_snapshots=[{"elapsed_minutes": 13.0, "total_score": 49, "live": 158.0}],
        )
        self.assertEqual(features.recent_pace_trend, "cooling")
        decision = decide_signal(
            features,
            {"ALT": quality(1, names=["projeksiyon"]), "ÜST": quality(0)},
            DummyConfig(),
        )
        self.assertTrue(decision.should_alert)
        self.assertEqual(decision.direction, "ALT")

    def test_recent_heating_blocks_blind_under_on_line_up(self):
        features = self.features(
            155.0,
            170.0,
            "55-45",
            recent_snapshots=[{"elapsed_minutes": 13.0, "total_score": 80, "live": 164.0}],
        )
        self.assertEqual(features.recent_pace_trend, "heating")
        decision = decide_signal(
            features,
            {"ALT": quality(2, names=["takım profili", "H2H geçmiş"]), "ÜST": quality(1)},
            DummyConfig(),
        )
        self.assertNotEqual(decision.direction, "ALT")


if __name__ == "__main__":
    unittest.main()
