import logging
import re
from fastapi import FastAPI, HTTPException

from analyse_fpl.run_analysis import analyse_fpl
from analyse_fpl.model import FPLAnalysisResponse

app = FastAPI()

logging.basicConfig(
    filename="log/info.log",
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d|%(levelname)s|%(process)d:%(thread)d|%(filename)s:%(lineno)d|%(module)s.%(funcName)s|%(message)s',
)

@app.get("/api/analyse")
def analyse() -> FPLAnalysisResponse:
    return analyse_fpl()

@app.get("/api/analyse/{season}")
def analyse_season(season: str) -> FPLAnalysisResponse:
    if not re.fullmatch(r"\d{4}-\d{4}", season):
        raise HTTPException(status_code=400, detail="season must be in the format YYYY-YYYY, e.g. 2026-2027")
    return analyse_fpl(season)
