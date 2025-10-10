from __future__ import annotations
import os
import pandas as pd
from pipeline.read.base import Reader

class CSVReader(Reader):
    """
    config:
      - path: str (required)
      - sep: str (default ',')
    """

    def __init__(self, name: str, config: dict | None = None) -> None:
        super().__init__(name=name, config=config or {})

    def read(self) -> pd.DataFrame:
        path = self.config.get("path")
        sep  = self.config.get("sep", ",")
        self.log(f"Reading CSV from {path} (sep='{sep}')")

        if not path or not os.path.exists(path):
            self.log(f"File not found: {path}. Returning empty DataFrame so pipeline can continue.")
            return pd.DataFrame()

        df = pd.read_csv(path, sep=sep)
        self.log(f"Read {len(df)} rows x {len(df.columns)} cols")
        return df
