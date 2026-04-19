import unittest

from signal_reliability import RELIABLE_LABEL, WATCH_LABEL, WEAK_LABEL, alert_reliability


class SignalReliabilityTest(unittest.TestCase):
    def test_a_grade_is_not_automatically_reliable(self):
        reliability = alert_reliability(
            direction="ALT",
            quality_grade="A",
            quality_score=82,
            status="Q2 05:00",
            diff=14,
            threshold=10,
            opening=160,
            live=174,
        )

        self.assertEqual(reliability["label"], WATCH_LABEL)
        self.assertFalse(reliability["is_reliable"])

    def test_extreme_contrarian_under_is_weak_even_with_a_grade(self):
        reliability = alert_reliability(
            direction="ALT",
            quality_grade="A",
            quality_score=94,
            status="Q3 05:00",
            diff=26,
            threshold=10,
            opening=170,
            live=196,
        )

        self.assertEqual(reliability["label"], WEAK_LABEL)
        self.assertFalse(reliability["is_reliable"])

    def test_clean_high_score_a_can_be_reliable(self):
        reliability = alert_reliability(
            direction="ALT",
            quality_grade="A",
            quality_score=92,
            status="Q2 05:00",
            diff=14,
            threshold=10,
            opening=160,
            live=174,
        )

        self.assertEqual(reliability["label"], RELIABLE_LABEL)
        self.assertTrue(reliability["is_reliable"])

    def test_early_a_signal_stays_watch(self):
        reliability = alert_reliability(
            direction="ÜST",
            quality_grade="A",
            quality_score=92,
            status="Q1 05:00",
            diff=13,
            threshold=10,
            opening=170,
            live=157,
        )

        self.assertEqual(reliability["label"], WATCH_LABEL)
        self.assertFalse(reliability["is_reliable"])


if __name__ == "__main__":
    unittest.main()
