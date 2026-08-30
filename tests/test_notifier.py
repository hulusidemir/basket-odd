import unittest

from notifier import _build_alert_text


class NotifierTests(unittest.TestCase):
    def test_alert_contains_only_raw_line_facts(self):
        text = _build_alert_text(
            match_name="Home - Away",
            tournament="League",
            opening=160,
            prematch=164,
            live=171,
            direction="ALT",
            diff=11,
            status="Q2 05:00",
            score="40 - 35",
            signal_count=1,
            period=2,
        )
        self.assertIn("<b>ALT</b>", text)
        self.assertIn("160.0 → 164.0 → 171.0 (+11.0)", text)
        for removed in ("Projeksiyon", "Adil", "H2H", "Kalite", "Kanıt", "Skor: 70/100"):
            self.assertNotIn(removed, text)


if __name__ == "__main__":
    unittest.main()
