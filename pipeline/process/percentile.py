from __future__ import annotations
import pandas as pd
import numpy as np
from pipeline.process.processor import Processor

class PercentileProcessor(Processor):
    """Add the columns 85th_percentile_state - purchase in top 15% within its state
    85th_percentile_national: purchase in top 15% within its national"""

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log("Percentile columns")
        thresh = self.config.get("percentile", 0.85)
        out_dtype  =str(self.config.get("output_dtype", "int")).lower()

        if "purchase" not in df.columns:
            self.log("WARN: 'purchase' column missing; creating both percentile flags as 0")
            df["85th_percentile_state"] = 0
            df["85th_percentile_national"] = 0
            return df

        purchase = df["purchase"]
        valid = purchase.notna()

        if valid.any():
            national_cut = purchase.quantile(thresh, interpolation="linear")
        else:
            national_cut = np.nan

        if "state" in df.columns:
            state_cuts = (
              df.loc[valid]
              .groupby("state")["purchase"]
              .quantile(thresh, interpolation="linear")
            )
            per_row_state_cut = df["state"].map(state_cuts)
        else:
            # No state column; treat all as no state threshold
            self.log("WARN: 'state' column missing; '85th_percentile_state' will be 0 for all rows.")
            per_row_state_cut = pd.Series(np.nan, index=df.index)

        state_flag = valid & (purchase >= per_row_state_cut)
        national_flag = valid & (purchase >= national_cut)

        # Output dtype
        if out_dtype == "bool":
            df["85th_percentile_state"] = state_flag
            df["85th_percentile_national"] = national_flag
        else:
            # default int 0/1 for easier downstream SQL and aggregation
            df["85th_percentile_state"] = state_flag.astype("int8")
            df["85th_percentile_national"] = national_flag.astype("int8")

        self.log(
            f"Computed cuts: national={national_cut!r}; "
            f"states with cuts={state_cuts.index.tolist() if 'state_cuts' in locals() else 'N/A'}"
        )
        return df