import logging
from pathlib import Path
from fastapi import FastAPI, HTTPException

from analyse_fpl.run_analysis import analyse_fpl
from analyse_fpl.model import FPLAnalysisResponse
from analyse_fpl.seasons import get_available_seasons, is_valid_season

app = FastAPI()

log_dir = Path(__file__).resolve().parent / "log"
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    filename=log_dir / "info.log",
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d|%(levelname)s|%(process)d:%(thread)d|%(filename)s:%(lineno)d|%(module)s.%(funcName)s|%(message)s',
)


def resolve_season(season: str | None) -> str:
    if season is not None and not is_valid_season(season):
        raise HTTPException(
            status_code=400,
            detail="season must contain consecutive years in the format YYYY-YYYY, e.g. 2026-2027",
        )
    seasons = get_available_seasons()
    if season is None:
        if not seasons:
            raise HTTPException(status_code=404, detail="No seasons available")
        return seasons[0]
    if season not in seasons:
        raise HTTPException(status_code=404, detail=f"Season {season} is not available")
    return season


@app.get("/api/seasons")
def seasons() -> list[str]:
    return get_available_seasons()


@app.get("/api/analyse")
def analyse(season: str | None = None) -> FPLAnalysisResponse:
    return analyse_fpl(resolve_season(season))


@app.get("/api/analyse/{season}")
def analyse_season(season: str) -> FPLAnalysisResponse:
    return analyse_fpl(resolve_season(season))
