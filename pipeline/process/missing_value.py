from __future__ import annotations
import pandas as pd
from pipeline.process.processor import Processor

"""
This module defines :class:MissingValuesProcessor, a concrete implementation of :class:pipeline.process.processor.Processor.

It's purpose is to handle missing values in the time_spent_seconds column by filling them using a configurable statistical 
strategy - either the mean or median of the existing values.

This step ensures that the dataset remains complete and consistent before further transformations such as normalization or
percentile calculations.
"""

class MissingValuesProcessor(Processor):
    """
    Processor that fills missing values in time_spent_seconds column.

    The MissingValuesProcessor replaces NaN values in the time_spent_seconds column using a selected strategy, which
    can be either mean or median,

    If the column is not present in the input dataframe, the processor logs a warning and skips the operation.

    :param name: The name assigned to this processor instance.
    :type name: str
    :param config: Configuration dictionary that may include:
        -strategy (str): The method used to fill missing values.
        Accepts mean or median. Defaults to mean.
    :type config: dict | None
    """

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fills missing values in the time_spent_seconds column.

        The processor calculates the specified statistic (mean or median)
        and replaces all ``NaN`` values in the time_spent_seconds column
        with that value. If the column does not exist, the step is skipped.

        :param df: Input Pandas DataFrame containing the column time_spent_seconds.
        :type df: pandas.DataFrame
        :return: The DataFrame with missing values filled.
        :rtype: pandas.DataFrame
        :raises ValueError: If the provided ``strategy`` is not recognized.
        """
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