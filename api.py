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
"""
This module defines a FastAPI application with endpoints for managing the ingestion of data into the processing pipeline.

It includes the following routes:
- /health: A simple health check endpoint to ensure the API is running.
- /ingest: A POST request to trigger the ingestion process, starting the pipeline with specified CSV file and PostgreSQL
options.

The pipeline consists of the following stages:
1. CSVReader: reads data from a csv file into a pandas dataframe.
2. MissingValuesProcessor: fills in missing values using a specified strategy
3. ConversionProcessor: converts datatypes if needed
4. StateAbbreviationProcessor: adds state abbreviations to the dataset
5. NormalizationProcessor: normalizes data based on a chosen method
6. PercentileProcessor: adds a column indicating whether the data is within the to 15% of all sales
7. PostgreSQLStorage: writes the processed data to the PostgreSQL database

The IngestRequest model is used to receive the ingestion parameters from the client
"""

class IngestRequest(BaseModel):
    """
    Request model for the /ingest API endpoint

    This model is used to receive data via POST requests to trigger the pipeline process.
    It includes options for both CSV file reading and PostgreSQL database configuration.

    :param path: The path to the CSV file to ingest.
    :type path: Optional[str]
    :param sep: The delimiter used in the CSV file.
    :type sep: str
    :param dsn: The DSN for PostgreSQL connection.
    :type dsn: Optional[str]
    :param dbschema: The schema in PostgreSQL where data will be inserted.
    :type dbschema: str
    :param table: The name of the table where data will be inserted.
    :type table: str
    :param if_exists: What to do if the table already exists. ("replace", "append", "fail")
    :type if_exists: str
    :param chunksize: The number of rows per chunk when inserting data into PostgreSQL
    :type chunksize: int


    """
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
    """
    Builds the data pipeline from the reader, processor and writer components.
    This function assembles the pipeline, linking together the components that read, process and write the data.

    :param csv_path: The path to the CSV file to read
    :type csv_path: str
    :param sep: The delimiter used in the CSV file.
    :type sep: str
    :param dsn: The DSN for PostgreSQL connection.
    :type dsn: str
    :param schema: The schema in PostrgreSQL database.
    :type schema: str
    :param table: The name of the table where data will be inserted.
    :type table: str
    :param if_exists: What to do if the table already exists. ("replace", "append", "fail")
    :type if_exists: str
    :param chunksize: The number of rows per chunk when inserting data into PostgreSQL
    :type chunksize: int

    :return: An orchestrator instance that can run the pipeline.
    :rtype: Orchestrator
    """
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
    """
    Health check endpoint to ensure the API is running.

    This is a simple endpoint that returns a status of "ok" when the API is running.

    :return: A dictionary containing the health status.
    :rtype: dict
    """
    return {"status": "ok"}

@app.post("/ingest")
def ingest(req: IngestRequest):
    """
    Ingest data into the pipeline and writes to the PostgreSQL database.

    This endpoint tiggers the pipeline process by accepting a POST request with a payload that specifies the CSV file and
    PostgreSQL options. It invokess the entire pipeline process, from reading the CSV to writing the data to the database.

    :param req: The ingestion request containing file path and DB parameters.
    :type req: IngestRequest

    :return: A dictionary indicating the result of the ingestion.
    :rtype: dict
    :raises: HTTPException: If any error occurs during the ingestion process
    """
    dsn = req.dsn

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
        print("INGEST ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "status": "ok",
        "written_to": f"{req.dbschema}.{req.table}",
        "source": req.path,
        "if_exists": req.if_exists,
    }

from mangum import Mangum
handler = Mangum(app)