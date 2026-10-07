import unittest

from notifier import _build_alert_text


class NotifierTests(unittest.TestCase):
    def test_alert_explains_prematch_reference_and_threshold(self):
        text = _build_alert_text(
            match_name="Home - Away", tournament="League", opening=160,
            prematch=200, live=180, direction="ÜST", diff=-20, status="Q2 05:00",
            score="40 - 35", signal_count=1, period=2,
            reference_used="prematch", reference_total=200, effective_threshold=14,
        )
        self.assertIn("160.0 → 200.0 → 180.0", text)
        self.assertIn("Maç önü 200.0 · fark -20.0 · eşik 14.00", text)

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

    def test_probability_and_any_company_are_explicit_without_zero_threshold(self):
        text = _build_alert_text(
            match_name="Home - Away", tournament="League", opening=145,
            prematch=140, live=135, direction="ALT", diff=-5, status="Q2 05:00",
            score="40 - 35", signal_count=1, period=2,
            reference_used="prematch", reference_total=140, effective_threshold=0,
            fair_total=127, bookmaker="Any & Company",
            win_probability={"validated": False, "probability": None},
        )
        self.assertIn("Tahmini toplam:</b> 127.0", text)
        self.assertIn("Kaynak:</b> Any &amp; Company", text)
        self.assertIn("Kazanma olasılığı:</b> —", text)
        self.assertNotIn("eşik 0.00", text)

    def test_extreme_probability_is_html_escaped_for_telegram(self):
        text = _build_alert_text(
            match_name="Home - Away", tournament="League", opening=160,
            prematch=160, live=135, direction="ALT", diff=-25, status="Q2 05:00",
            score="40 - 35", signal_count=1, period=2,
            win_probability={"probability": .00001, "validated": False},
        )
        self.assertIn("&lt;%0.1", text)
        self.assertNotIn("<%0.1", text)


if __name__ == "__main__":
    unittest.main()
