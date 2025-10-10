# main.py
import os
from dotenv import load_dotenv
from pipeline.orchestrator import Orchestrator
from pipeline.read.csvreader import CSVReader
from pipeline.process.missing_value import MissingValuesProcessor
from pipeline.process.conversion import ConversionProcessor
from pipeline.process.state_abbreviation import StateAbbreviationProcessor
from pipeline.process.normalization import NormalizationProcessor
from pipeline.process.percentile import PercentileProcessor
from pipeline.write.postgres_storage import PostgreSQLStorage

def build_and_run():
    load_dotenv()

    dsn = os.getenv("DB_DSN", "postgresql+psycopg://postgres:Attributy123!@localhost:5432/Pipeline")
    schema = os.getenv("DB_SCHEMA", "public")
    table = os.getenv("DB_TABLE", "marketing_data")
    csv_path = os.getenv("CSV_PATH", "data/dataset.csv")
    sep = os.getenv("CSV_SEP", ",")

    reader = CSVReader("CSV", {"path": csv_path, "sep": sep})
    processors = [
        MissingValuesProcessor("MissingValue", {"strategy": "mean"}),
        ConversionProcessor("Conversion"),
        StateAbbreviationProcessor("StateAbbrev"),
        NormalizationProcessor("Norm", {"method": "min_max"}),
        PercentileProcessor("Percentile", {"percentile": 0.85}),
        AnalysisProcessor("Analysis"),
    ]
    writer = PostgreSQLStorage("PostgresWriter", {
        "dsn": dsn, "schema": schema, "table": table,
        "if_exists": "replace", "chunksize": 5000, "index": False,
    })
    orch = Orchestrator(reader, processors, writer)
    rows = orch.run()
    print(f"Wrote {rows} rows to {schema}.{table}")

if __name__ == "__main__":
    build_and_run()
