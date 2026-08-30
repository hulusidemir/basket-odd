import unittest

from aiscore_scraper import (
    AiscoreScraper,
    _normalize_market_snapshot,
    _redact_proxy_url,
    _select_market_line,
    _status_from_play_by_play_hint,
)


class AiscoreScraperTests(unittest.TestCase):
    def test_mobile_url_is_forced(self):
        self.assertEqual(
            AiscoreScraper._mobile_url("https://www.aiscore.com/basketball/match-a/id/odds"),
            "https://m.aiscore.com/basketball/match-a/id/odds",
        )

    def test_play_by_play_hint_recovers_clock(self):
        result = _status_from_play_by_play_hint({"period": 2, "clock": "04:21"})
        self.assertEqual(result["status"], "Q2 04:21")

    def test_first_paired_bookmaker_line_is_primary(self):
        snapshot = _normalize_market_snapshot({
            "opening_lines": [160, 180],
            "inplay_lines": [170, 190],
            "paired_bookmaker_count": 2,
        })
        self.assertEqual(_select_market_line({"opening": 160}, snapshot, "opening"), 160)
        self.assertEqual(_select_market_line({"inplay": 170}, snapshot, "inplay"), 170)

    def test_proxy_credentials_are_redacted(self):
        value = _redact_proxy_url("socks5://user:secret@127.0.0.1:9050")
        self.assertNotIn("user", value)
        self.assertNotIn("secret", value)
        self.assertIn("127.0.0.1:9050", value)


if __name__ == "__main__":
    unittest.main()
