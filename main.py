import logging
from fastapi import FastAPI

from analyse_fpl.run_analysis import analyse_fpl
from analyse_fpl.model import FPLAnalysisResponse

app = FastAPI()

logging.basicConfig(
    filename="log/info.log",
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d|%(levelname)s|%(process)d:%(thread)d|%(filename)s:%(lineno)d|%(module)s.%(funcName)s|%(message)s',
)

@app.get("/analyse")
def analyse() -> FPLAnalysisResponse:
    return analyse_fpl()
