from __future__ import annotations
import pandas as pd
from pipeline.process.processor import Processor

class ConversionProcessor(Processor):
    """Add the converted column which indicates if a sale has been made or not"""

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log("Creating converted column from purchase")
        df["converted"] = df["purchase"].notnull().astype(int)
        return df