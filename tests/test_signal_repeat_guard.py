import unittest

from signal_repeat import live_total_delta


class SameDirectionRepeatGuardTests(unittest.TestCase):
    def test_live_total_difference_is_absolute(self):
        self.assertEqual(live_total_delta(170, 160), 10)
        self.assertEqual(live_total_delta(150, 160), 10)

    def test_live_total_difference_below_ten(self):
        self.assertEqual(live_total_delta(169.5, 160), 9.5)

    def test_unreadable_live_total_has_no_delta(self):
        self.assertIsNone(live_total_delta("", 160))
        self.assertIsNone(live_total_delta(170, None))


if __name__ == "__main__":
    unittest.main()
