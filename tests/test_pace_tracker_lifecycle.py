import unittest

from pace_tracker import PaceTracker


class _Clock:
    def __init__(self):
        self.value = 1.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class PaceTrackerLifecycleTests(unittest.TestCase):
    def test_expired_matches_are_pruned(self):
        clock = _Clock()
        tracker = PaceTracker(ttl_seconds=60, clock=clock)
        tracker.update("match-1", 1, 12, 10, remaining_min=7)
        clock.advance(61)

        self.assertEqual(tracker.prune(), 1)
        self.assertEqual(tracker.tracked_match_count, 0)

    def test_repeatedly_absent_matches_are_pruned(self):
        tracker = PaceTracker(inactive_cycle_limit=2)
        tracker.update("match-1", 1, 12, 10, remaining_min=7)

        self.assertEqual(tracker.prune(active_match_ids={"other"}), 0)
        self.assertEqual(tracker.prune(active_match_ids={"other"}), 1)
        self.assertEqual(tracker.tracked_match_count, 0)

    def test_period_or_score_rewind_resets_reused_match_state(self):
        tracker = PaceTracker()
        tracker.update("match-1", 1, 20, 10, remaining_min=0)
        second_period = tracker.update("match-1", 2, 35, 10, remaining_min=9)
        self.assertEqual(second_period["quarter_pts"], {1: 20})

        restarted = tracker.update("match-1", 1, 4, 10, remaining_min=9)
        self.assertEqual(restarted["quarter_pts"], {})
        self.assertEqual(tracker.tracked_match_count, 1)

    def test_size_limit_evicts_oldest_state(self):
        clock = _Clock()
        tracker = PaceTracker(max_matches=2, clock=clock)
        tracker.update("match-1", 1, 4, 10, remaining_min=9)
        clock.advance(1)
        tracker.update("match-2", 1, 4, 10, remaining_min=9)
        clock.advance(1)
        tracker.update("match-3", 1, 4, 10, remaining_min=9)

        self.assertEqual(tracker.tracked_match_count, 2)
        self.assertNotIn("match-1", tracker.tracked_match_ids)


if __name__ == "__main__":
    unittest.main()
