from __future__ import annotations
from typing import Any, Dict, Optional
import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import VARCHAR, INTEGER, BIGINT, NUMERIC, BOOLEAN
from sqlalchemy.exc import SQLAlchemyError

from pipeline.write.writer import Writer

class PostgreSQLStorage(Writer):

    def __init__(self, name: str, config: Dict[str, Any] | None = None) -> None:
        super().__init__(name=name, config=config or {})
        self._engine: Optional[Engine] = None


    def write(self, df: pd.DataFrame) -> int:
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
            # Mask password in logs
            masked = dsn
            try:
                # very basic masking for common DSN shapes
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
                # default to text; if you know max lengths, set VARCHAR(n)
                mapping[col] = VARCHAR()
        return mapping

    def _require(self, key: str) -> Any:
        val = self.config.get(key)
        if val in (None, ""):
            raise ValueError(f"Missing required config key: {key}")
        return val