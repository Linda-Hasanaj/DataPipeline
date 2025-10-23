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

"""
main.py
=======

This script sets up and runs the data pipeline, which reads, processes, and writes
data to a PostgreSQL database.

The pipeline consists of the following stages:
1. **CSVReader**: Loads raw data from a CSV file into a Pandas DataFrame.
2. **MissingValuesProcessor**: Fills missing values in the data (e.g., using the mean).
3. **ConversionProcessor**: Adds a `converted` column indicating whether a sale has been made.
4. **StateAbbreviationProcessor**: Maps full state names to their two-letter abbreviations.
5. **NormalizationProcessor**: Normalizes the `purchase` column using either `z_score` or `min_max` methods.
6. **PercentileProcessor**: Flags purchases that fall within the 85th percentile of each state and nationally.
7. **PostgreSQLStorage**: Writes the processed data to a PostgreSQL table.

Configuration values (such as the database connection details and file paths) are loaded from environment variables using the `dotenv` library.

The pipeline is executed by creating an instance of the :class:`Orchestrator`, which coordinates the reader, processors, and writer.
"""
def build_and_run():
    """Sets up and executes the data pipeline.

      This function loads the environment variables, initializes the pipeline stages,
      and orchestrates the data processing from reading the CSV file to writing the processed
      data to PostgreSQL. It uses the `Orchestrator` class to manage the pipeline flow.

      Configuration for the pipeline is loaded from environment variables:
      - `DB_DSN`: The Data Source Name (DSN) for PostgreSQL connection.
      - `DB_SCHEMA`: The PostgreSQL schema to write data to (default: `public`).
      - `DB_TABLE`: The target table for the processed data (default: `marketing_data`).
      - `CSV_PATH`: The path to the CSV file to be read (default: `data/dataset.csv`).
      - `CSV_SEP`: The separator for the CSV file (default: `,`).
      """
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
        #AnalysisProcessor("Analysis"),
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
