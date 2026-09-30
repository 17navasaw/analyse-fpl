import re
from pathlib import Path


DATA_DIR = Path(__file__).resolve().parents[2] / "FPL-Core-Insights" / "data"


def is_valid_season(season: str) -> bool:
    if not re.fullmatch(r"[0-9]{4}-[0-9]{4}", season):
        return False
    start, end = map(int, season.split("-"))
    return end == start + 1


def get_season_data_dir(season: str) -> Path:
    return DATA_DIR / season


def get_available_seasons() -> list[str]:
    if not DATA_DIR.is_dir():
        return []
    return sorted(
        (
            path.name
            for path in DATA_DIR.iterdir()
            if path.is_dir()
            and is_valid_season(path.name)
            and (path / "gameweek_summaries.csv").is_file()
        ),
        reverse=True,
    )
