import csv
import unittest
from contextlib import chdir
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from fastapi.testclient import TestClient

from main import app


class SeasonAPITests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.data_dir = Path(self.temp_dir.name)
        data_patch = patch("analyse_fpl.seasons.DATA_DIR", self.data_dir)
        data_patch.start()
        self.addCleanup(data_patch.stop)
        self.client = TestClient(app)
        self.addCleanup(self.client.close)
        self.create_season("2025-2026", gameweek=8, player_id=101)
        self.create_season("2026-2027", gameweek=2, player_id=202)

    def write_csv(self, path, fieldnames, rows):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def create_season(self, season, gameweek, player_id):
        directory = self.data_dir / season
        self.write_csv(
            directory / "gameweek_summaries.csv",
            ["id", "deadline_time", "finished", "data_checked", "is_next"],
            [
                {"id": gameweek, "deadline_time": "2026-09-01T12:00:00Z",
                 "finished": True, "data_checked": True, "is_next": False},
                {"id": gameweek + 1, "deadline_time": "2026-09-08T12:00:00Z",
                 "finished": False, "data_checked": False, "is_next": True},
            ],
        )
        gameweek_dir = directory / "By Gameweek" / f"GW{gameweek}"
        self.write_csv(
            gameweek_dir / "player_gameweek_stats.csv",
            ["id", "status", "web_name", "event_points", "minutes"],
            [{"id": player_id, "status": "a", "web_name": f"Player {player_id}",
              "event_points": gameweek, "minutes": 90}],
        )
        self.write_csv(
            gameweek_dir / "players.csv",
            ["player_id", "position", "team_code"],
            [{"player_id": player_id, "position": "Defender", "team_code": 1}],
        )
        self.write_csv(
            gameweek_dir / "teams.csv",
            ["code", "name", "elo"],
            [{"code": 1, "name": f"Team {season}", "elo": 1500}],
        )

    def test_lists_only_analysis_ready_seasons_newest_first(self):
        (self.data_dir / "2024-2025").mkdir()
        for name in ["2027-2029", "invalid", "2027-28"]:
            directory = self.data_dir / name
            directory.mkdir()
            (directory / "gameweek_summaries.csv").touch()
        (self.data_dir / "2028-2029").touch()
        response = self.client.get("/api/seasons")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), ["2026-2027", "2025-2026"])

    def test_default_analyses_latest_season(self):
        response = self.client.get("/api/analyse")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["past_gameweeks"], [2])
        self.assertEqual(body["next_gameweek"], 3)
        self.assertEqual(body["player_stats"]["202-Defender"][0]["season"], "2026-2027")

    def test_explicit_selection_and_legacy_route_use_the_requested_dataset(self):
        for season, gameweek, player_id in [("2025-2026", 8, 101), ("2026-2027", 2, 202)]:
            with self.subTest(season=season):
                query_response = self.client.get("/api/analyse", params={"season": season})
                path_response = self.client.get(f"/api/analyse/{season}")
                self.assertEqual(query_response.status_code, 200)
                self.assertEqual(path_response.status_code, 200)
                self.assertEqual(query_response.json(), path_response.json())
                body = query_response.json()
                self.assertEqual(body["past_gameweeks"], [gameweek])
                self.assertEqual(body["next_gameweek"], gameweek + 1)
                stats = body["player_stats"][f"{player_id}-Defender"]
                self.assertEqual(stats[0]["season"], season)
                self.assertEqual(stats[0]["team_name"], f"Team {season}")
                self.assertEqual(stats[0]["event_points"], gameweek)

    def test_new_analysis_ready_season_becomes_default(self):
        self.create_season("2027-2028", gameweek=1, player_id=303)
        self.assertEqual(self.client.get("/api/seasons").json()[0], "2027-2028")
        body = self.client.get("/api/analyse").json()
        self.assertEqual(body["player_stats"]["303-Defender"][0]["season"], "2027-2028")

    def test_malformed_and_nonconsecutive_seasons_return_400(self):
        for season in ["bad-season", "2026-2028", "26-27", "2026-2027suffix"]:
            for url, params in [("/api/analyse", {"season": season}),
                                (f"/api/analyse/{season}", {})]:
                with self.subTest(season=season, url=url):
                    self.assertEqual(self.client.get(url, params=params).status_code, 400)
        for season in ["", "../2026-2027", "/tmp/2026-2027"]:
            with self.subTest(season=season):
                self.assertEqual(self.client.get("/api/analyse", params={"season": season}).status_code, 400)

    def test_unavailable_seasons_return_404(self):
        (self.data_dir / "2024-2025").mkdir()
        for season in ["2024-2025", "2027-2028"]:
            with self.subTest(season=season):
                self.assertEqual(self.client.get("/api/analyse", params={"season": season}).status_code, 404)
                self.assertEqual(self.client.get(f"/api/analyse/{season}").status_code, 404)

    def test_no_seasons_returns_empty_list_and_analysis_404(self):
        empty_dir = self.data_dir / "empty"
        empty_dir.mkdir()
        with patch("analyse_fpl.seasons.DATA_DIR", empty_dir):
            self.assertEqual(self.client.get("/api/seasons").json(), [])
            self.assertEqual(self.client.get("/api/analyse").status_code, 404)
            self.assertEqual(self.client.get("/api/analyse/2026-2027").status_code, 404)

    def test_missing_data_directory_returns_empty_list(self):
        with patch("analyse_fpl.seasons.DATA_DIR", self.data_dir / "missing"):
            response = self.client.get("/api/seasons")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), [])
            self.assertEqual(self.client.get("/api/analyse").status_code, 404)

    def test_discovery_and_analysis_do_not_depend_on_working_directory(self):
        with chdir(self.data_dir):
            self.assertEqual(self.client.get("/api/seasons").json(), ["2026-2027", "2025-2026"])
            self.assertEqual(self.client.get("/api/analyse").json()["past_gameweeks"], [2])

    def test_season_without_finished_gameweeks_keeps_empty_response(self):
        self.write_csv(
            self.data_dir / "2026-2027" / "gameweek_summaries.csv",
            ["id", "deadline_time", "finished", "data_checked", "is_next"],
            [{"id": 1, "deadline_time": "2026-09-01T12:00:00Z",
              "finished": False, "data_checked": False, "is_next": True}],
        )
        response = self.client.get("/api/analyse")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"past_gameweeks": [], "next_gameweek": None, "player_stats": {}})

    def test_missing_player_stats_keeps_gameweek_summary(self):
        (self.data_dir / "2026-2027" / "By Gameweek" / "GW2" / "player_gameweek_stats.csv").unlink()
        response = self.client.get("/api/analyse")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"past_gameweeks": [2], "next_gameweek": 3, "player_stats": {}})


if __name__ == "__main__":
    unittest.main()
