import unittest

from projection import calculate_quarter_ppm


class QuarterPpmTests(unittest.TestCase):
    def test_completed_quarters_use_full_quarter_length(self):
        result = calculate_quarter_ppm(
            [41, 47], period=3, remaining_min=8, quarter_length=10
        )
        self.assertEqual(result, [4.1, 4.7])

    def test_live_quarter_uses_elapsed_minutes(self):
        result = calculate_quarter_ppm(
            [41, 14], period=2, remaining_min=6, quarter_length=10
        )
        self.assertEqual(result, [4.1, 3.5])

    def test_period_end_uses_full_quarter_length(self):
        result = calculate_quarter_ppm(
            [44], period=1, remaining_min=0, quarter_length=10
        )
        self.assertEqual(result, [4.4])


if __name__ == "__main__":
    unittest.main()
