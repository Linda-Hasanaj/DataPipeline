# api.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional

from pipeline.orchestrator import Orchestrator
from pipeline.read.csvreader import CSVReader
from pipeline.process.missing_value import MissingValuesProcessor
from pipeline.process.conversion import ConversionProcessor
from pipeline.process.state_abbreviation import StateAbbreviationProcessor
from pipeline.process.normalization import NormalizationProcessor
from pipeline.process.percentile import PercentileProcessor
from pipeline.write.postgres_storage import PostgreSQLStorage


app = FastAPI(title="DataPipeline API")

class IngestRequest(BaseModel):
    # CSV options
    path: Optional[str] = Field(default="data/dataset.csv")
    sep: str = Field(default=",")

    # Postgres options (you can also load these from env in build_pipeline)
    dsn: Optional[str] = None
    dbschema: str = "public"
    table: str = "marketing_data"
    if_exists: str = "replace"  # "append" | "replace" | "fail"
    chunksize: int = 5000

def build_pipeline(
    csv_path: str,
    sep: str,
    dsn: str,
    schema: str,
    table: str,
    if_exists: str,
    chunksize: int,
) -> Orchestrator:
    # Reader
    reader = CSVReader(name="CSV", config={"path": csv_path, "sep": sep})

    # Processors (order matters: fix missing values before conversions/normalization)
    processors = [
        MissingValuesProcessor(name="MissingValue", config={"strategy": "mean"}),
        ConversionProcessor(name="Conversion"),
        StateAbbreviationProcessor(name="StateAbbrev"),
        NormalizationProcessor(name="Norm", config={"method": "min_max"}),
        PercentileProcessor(name="Percentile", config={"percentile": 0.85}),
    ]

    # Writer
    writer = PostgreSQLStorage(
        name="PostgresWriter",
        config={
            "dsn": dsn,  # use the request DSN
            "schema": schema,
            "table": table,
            "if_exists": if_exists,  # use the request if_exists
            "chunksize": chunksize,
            "index": False,
        },
    )
    return Orchestrator(reader=reader, processors=processors, writer=writer)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/ingest")
def ingest(req: IngestRequest):
    # default DSN
    dsn = req.dsn or "postgresql+psycopg://postgres:Attributy123!@localhost:5432/Pipeline"

    try:
        orch = build_pipeline(
            csv_path=req.path,
            sep=req.sep,
            dsn=dsn,
            schema=req.dbschema,
            table=req.table,
            if_exists=req.if_exists,
            chunksize=req.chunksize,
        )
        orch.run()
    except Exception as e:
        print("INGEST ERROR:", repr(e))  # add this line
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "status": "ok",
        "written_to": f"{req.dbschema}.{req.table}",
        "source": req.path,
        "if_exists": req.if_exists,
    }
