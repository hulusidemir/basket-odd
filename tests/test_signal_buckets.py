import unittest

from signal_buckets import build_signal_bucket_profile, matching_signal_buckets


class SignalBucketTests(unittest.TestCase):
    def test_alt_prime_bucket_matches_strict_160_170_shape(self):
        alert = {
            "direction": "ALT",
            "live": 166.0,
            "fair_line": 156.0,
            "projected": 162.0,
        }

        buckets = matching_signal_buckets(alert)

        self.assertEqual([item["id"] for item in buckets], ["alt_160_170_fair_proj_below"])
        self.assertEqual(buckets[0]["values"]["fair_projected_gap"], -6.0)

    def test_ust_cross_pressure_bucket_requires_projection_above_live(self):
        alert = {
            "direction": "ÜST",
            "live": 154.0,
            "fair_line": 150.0,
            "projected": 159.0,
        }

        buckets = matching_signal_buckets(alert)

        self.assertEqual([item["id"] for item in buckets], ["ust_cross_pressure"])

    def test_profile_counts_deleted_results(self):
        rows = [
            {
                "id": 1,
                "result": "Başarılı",
                "direction": "ALT",
                "live": 166.0,
                "fair_line": 156.0,
                "projected": 162.0,
                "alerted_at": "2026-06-01 10:00:00",
            },
            {
                "id": 2,
                "result": "Başarısız",
                "direction": "ALT",
                "live": 166.0,
                "fair_line": 156.0,
                "projected": 162.0,
                "alerted_at": "2026-06-02 10:00:00",
            },
        ]

        profile = build_signal_bucket_profile(rows)

        self.assertEqual(profile["alt_160_170_fair_proj_below"]["resolved"], 2)
        self.assertEqual(profile["alt_160_170_fair_proj_below"]["success"], 1)
        self.assertEqual(profile["alt_160_170_fair_proj_below"]["rate"], 50.0)


if __name__ == "__main__":
    unittest.main()
