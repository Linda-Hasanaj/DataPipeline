from __future__ import annotations
import pandas as pd
from pipeline.process.processor import Processor

class NormalizationProcessor(Processor):
    """Create normalized_purchase column from the normalization of purchase column either by using z score or min max method
    Supported methods:
    - "z_score": standard score normalization x - mean  /std
    -"min_max": scales values to [0, 1] range

    Config options:
    - method: str, either "z_score" or "min_max (default: "z_score")
    """

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
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