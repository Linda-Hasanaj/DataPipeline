from __future__ import annotations
from typing import Any, Dict, Optional
import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import VARCHAR, INTEGER, BIGINT, NUMERIC, BOOLEAN
from sqlalchemy.exc import SQLAlchemyError

from pipeline.write.writer import Writer

"""
postgresql_storage.py
=====================

This module defines the :class:`PostgreSQLStorage` class, which is responsible for
writing data to a PostgreSQL database.

The processor is configured with a Data Source Name (DSN), the target table, and
other optional parameters like chunk size and behavior for existing data. It uses
SQLAlchemy to manage database connections and data insertion.

Key Features:
- Supports large datasets with chunked insertions.
- Automatically infers PostgreSQL column types based on Pandas DataFrame types.
- Logs connection details (with password masking) and handles schema creation if needed.
- Handles database write failures with proper error handling and logging.
"""

class PostgreSQLStorage(Writer):
    """Writes data from a Pandas DataFrame to a PostgreSQL database.

        This processor uses SQLAlchemy to handle database connections and insert data
        into the specified PostgreSQL table. It supports chunked inserts, type inference,
        and error handling.

        :param name: The name assigned to this writer instance.
        :type name: str
        :param config: Configuration dictionary containing the following parameters:
            - ``dsn`` (*str*): The Data Source Name (DSN) for connecting to PostgreSQL.
            - ``table`` (*str*): The target table in PostgreSQL where data will be written.
            - ``schema`` (*str*, optional): The schema in PostgreSQL (default is `public`).
            - ``if_exists`` (*str*, optional): What to do if the table already exists ("append", "replace", or "fail").
            - ``chunksize`` (*int*, optional): The number of rows to write per chunk (default is `10_000`).
            - ``index`` (*bool*, optional): Whether to include the DataFrame index as a column in the table.
        :type config: dict | None"""
    def __init__(self, name: str, config: Dict[str, Any] | None = None) -> None:
        super().__init__(name=name, config=config or {})
        self._engine: Optional[Engine] = None


    def write(self, df: pd.DataFrame) -> int:
        """Writes the provided DataFrame to the PostgreSQL database.

               This method attempts to insert the data in chunks to avoid overwhelming
               the database. It uses the configuration parameters for the DSN, table name,
               and other optional parameters to perform the write operation.

               :param df: The Pandas DataFrame containing the data to be written.
               :type df: pandas.DataFrame
               :return: The number of rows written to the database.
               :rtype: int
               :raises SQLAlchemyError: If an error occurs during the database write operation.
               """
        dsn = self._require("dsn")
        table = self._require("table")
        schema = self.config.get("schema", "public")
        if_exists = self.config.get("if_exists", "append")
        chunksize = int(self.config.get("chunksize", 10_000))
        include_index = bool(self.config.get("index", False))

        self._ensure_engine(dsn)
        self._ensure_schema(schema)

        dtype_cfg = self.config.get("dtype")
        dtype = dtype_cfg if isinstance(dtype_cfg, dict) else self._infer_types(df)

        self.log(
            f"Writing {len(df)} rows x {len(df.columns)} cols to {schema}.{table} "
            f"(if_exists={if_exists}, chunksize={chunksize})"
        )

        try:
            assert self._engine is not None
            with self._engine.begin() as conn:
                df.to_sql(
                    name=table,
                    con=conn,
                    schema=schema,
                    if_exists=if_exists,
                    index=include_index,
                    chunksize=chunksize,
                    method="multi",
                    dtype=dtype,
                )
        except SQLAlchemyError as e:
            self.log(f"Error: database write failed: {e}")
            raise
        finally:
            self._dispose()
        self.log(f"Done  writing to {schema}.{table}")
        return int(len(df))

    def _ensure_engine(self, dsn: str) -> None:
        if self._engine is None:
            masked = dsn
            try:
                before, after = dsn.split("://", 1)
                creds, rest = after.split("@", 1)
                if ":" in creds:
                    user, pwd = creds.split(":", 1)
                    masked = f"{before}://{user}:***@{rest}"
            except Exception:
                pass
            self.log(f"Connecting via DSN: {masked}")
            self._engine = create_engine(dsn, pool_pre_ping=True)

    def _dispose(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            self.log("Disconnected")

    def _ensure_schema(self, schema: str) -> None:
        if not schema:
            return
        assert self._engine is not None
        self.log(f"Ensuring schema exists: {schema}")
        with self._engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

    def _infer_types(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Basic pandas dtype -> PostgreSQL type mapping.
        Extend as needed (timestamps, JSON, categorical, etc.).
        """
        mapping: Dict[str, Any] = {}
        for col, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                mapping[col] = BIGINT()  # safer than INTEGER for large ids
            elif pd.api.types.is_float_dtype(dtype):
                # wide NUMERIC to avoid precision loss; tune if you know the scale
                mapping[col] = NUMERIC(precision=38, scale=10)
            elif pd.api.types.is_bool_dtype(dtype):
                mapping[col] = BOOLEAN()
            else:
                mapping[col] = VARCHAR()
        return mapping

    def _require(self, key: str) -> Any:
        val = self.config.get(key)
        if val in (None, ""):
            raise ValueError(f"Missing required config key: {key}")
        return val