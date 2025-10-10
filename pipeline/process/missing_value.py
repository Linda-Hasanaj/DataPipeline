from __future__ import annotations
import pandas as pd
from pipeline.process.processor import Processor

class MissingValuesProcessor(Processor):
    """Fill missing values in the column timeSpentSeconds using mean or median"""

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """fill missing values in the column timeSpentSeconds using mean or median"""
        strategy = self.config.get("strategy", "mean").lower()
        if "time_spent_seconds" not in df.columns:
            self.log("Column time_spent_seconds not found, skipping this step")
            return df

        self.log(f"Filling missing time_spent_seconds using {strategy} strategy")
        if strategy == "mean":
            value = df["time_spent_seconds"].mean()
        elif strategy == "median":
            value = df["time_spent_seconds"].median()
        else:
            raise ValueError(f"Unknown strategy '{strategy}'")

        df["time_spent_seconds"] = df["time_spent_seconds"].fillna(value)
        self.log(f"Filled missing values with {value:.2f}")

        return df