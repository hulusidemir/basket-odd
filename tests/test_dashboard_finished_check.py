import importlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class DashboardFinishedCheckRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.previous_db_path = os.environ.get("DB_PATH")
        os.environ["DB_PATH"] = str(Path(cls.temp_dir.name) / "dashboard.db")

        import config
        import dashboard

        importlib.reload(config)
        cls.dashboard = importlib.reload(dashboard)

    @classmethod
    def tearDownClass(cls):
        if cls.previous_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = cls.previous_db_path
        cls.temp_dir.cleanup()

    def setUp(self):
        self.dashboard.app.config["TESTING"] = True
        self.client = self.dashboard.app.test_client()

    def test_check_finished_route_returns_scan_summary(self):
        async def fake_scan(db, config, before_delete=None):
            if before_delete is not None:
                before_delete("match-1")
            return {
                "tracked_count": 1,
                "checked_count": 1,
                "finished_match_count": 1,
                "moved_count": 1,
                "message": "1 biten maç Silinen Maçlar'a taşındı.",
            }

        with (
            patch.object(self.dashboard, "_archive_active_match", return_value=1) as archive_match,
            patch.object(self.dashboard, "run_active_match_finished_scan", side_effect=fake_scan),
        ):
            response = self.client.post("/api/alerts/check-finished")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["moved_count"], 1)
        self.assertIn("biten maç", payload["message"])
        archive_match.assert_called_once_with("match-1")

    def test_check_finished_route_returns_json_error_when_scan_fails(self):
        async def failing_scan(db, config, before_delete=None):
            raise RuntimeError("browser unavailable")

        with (
            patch.object(self.dashboard, "run_active_match_finished_scan", side_effect=failing_scan),
            patch.object(self.dashboard.logger, "exception"),
        ):
            response = self.client.post("/api/alerts/check-finished")

        self.assertEqual(response.status_code, 500)
        payload = response.get_json()
        self.assertIn("Biten maçlar kontrol edilemedi", payload["error"])


if __name__ == "__main__":
    unittest.main()
