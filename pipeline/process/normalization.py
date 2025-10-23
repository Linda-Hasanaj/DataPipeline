from __future__ import annotations
import pandas as pd
from pipeline.process.processor import Processor

"""
This module defines :class: NormalizationProcessor, a concrete implementation of :class:pipeline.process.processor.Processor.

It's purpose is to normalize numerical purchase values in the dataset to ensure they are comparable and standardized. This transformation helps in downstream
analyses such as percentile computation and model training.

Two normalization methods are supported:
- z_score - Standard score normalization:
    (x - mean) /std
- min_max - Min-max normalization:
    (x - mean) /std
    
If the purchase column is missing or contains only non-numeric data, the processor logs a warning and adds a normalized_purchases column filled with NA values
"""
class NormalizationProcessor(Processor):
    """
    Processor that normalizes the purchase column.

    This processor scales or standardizes the values in the purchase column according to a chosen normalization method, either z-score or min-max. The result is
    stored in a new column called normalized_purchases.

    :param name: The name assigned to this processor instance.
    :type name: str
    :param config: Configuration dictionary supporting:
        - method (str): Normalization strategy. Accepts:
            - z-score: Uses standard score normalization (default)
            - min_max: Scales values into the range [0, 1]
    :type config: dict | None
    """

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalizes the purchase column based on the configured method.

        The processor converts the purchase column to numeric form and applies the chosen normalization algorithm. Handles missing or invalid numeric data gracefully.
        :param df: Input pandas dataframe containing the purchase column
        :type df: pd.DataFrame
        :return: The dataframe with a new normalized_purchase column
        :rtype: pd.DataFrame
        """
        self.log("Normalizing purchase column")
        if "purchase" not in df.columns:
            self.log("WARN: 'purchase column is missing, skipping normalization")
            df["normalization_purchases"] = pd.NA
            return df

        method = self.config.get("method", "z_score").lower().strip()
        self.log(f"Normalization 'purchase' column using method: {method}")

        df['purchase'] = pd.to_numeric(df['purchase'], errors='coerce')
        if df['purchase'].dropna().empty:
            self.log("No valid numeric purchase found")
            df["normalized_purchases"] = pd.NA
            return df

        if method == "z_score":
            mean = df["purchase"].mean()
            std = df["purchase"].std()
            if std == 0 or pd.isna(std):
                self.log("WARN: Standard deviation is zero — all purchases identical.")
                df["normalized_purchases"] = 0
            else:
                df["normalized_purchases"] = (df["purchase"] - mean) / std

        elif method == "min_max":
            min_val = df["purchase"].min()
            max_val = df["purchase"].max()
            if min_val == max_val:
                self.log("WARN: Min and max are equal — all purchases identical.")
                df["normalized_purchases"] = 0

            else:
                df["normalized_purchases"] = (df["purchase"] - min_val) / (max_val - min_val)

        else:
            self.log(f"ERROR: Unknown normalization method '{method}'. Using z_score by default.")
            mean = df["purchase"].mean()
            std = df["purchase"].std()
            df["normalized_purchases"] = (df["purchase"] - mean) / std


        return df