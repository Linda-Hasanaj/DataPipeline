from __future__ import annotations
import pandas as pd
import numpy as np
from pipeline.process.processor import Processor

"""
This module defines the :class: PercentileProcessor, a concrete implementation
of :class:`pipeline.process.processor.Processor`.

The primary function of this processor is to add two new columns to the dataset:
- **85th_percentile_state**: A binary flag (0 or 1) indicating whether the purchase
  value is within the top 15% of purchases within the same state.
- **85th_percentile_national**: A binary flag (0 or 1) indicating whether the purchase
  value is within the top 15% of all purchases nationally.

This transformation allows downstream analyses to focus on top-tier purchases at both
the state and national levels, such as identifying high-value customers or analyzing
market trends.
"""
class PercentileProcessor(Processor):
    """Processor that flags purchases in the top 15% within their state and nationally.

       This processor computes two new binary columns:
       - ``85th_percentile_state``: 1 if the purchase is greater than or equal to the 85th
         percentile purchase value within the same state, else 0.
       - ``85th_percentile_national``: 1 if the purchase is greater than or equal to the 85th
         percentile purchase value nationally, else 0.

       These flags are useful for identifying top-tier purchases based on state and national
       benchmarks.

       :param name: The name assigned to this processor instance.
       :type name: str
       :param config: Configuration dictionary supporting:
           - ``percentile`` (*float*): The percentile value to use for comparison. Defaults to `0.85` (85th percentile).
           - ``output_dtype`` (*str*): Output data type for the flags (`"int"` or `"bool"`). Defaults to `"int"`.
       :type config: dict | None

       **Example usage:**

       .. code-block:: python

           import pandas as pd
           from pipeline.process.percentile import PercentileProcessor

           df = pd.DataFrame({
               "purchase": [120.5, 80.0, 200.0, 150.0],
               "state": ["NY", "CA", "NY", "CA"]
           })

           processor = PercentileProcessor(name="Percentile", config={"percentile": 0.85})
           df = processor.run(df)
           print(df)
"""

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds the ``85th_percentile_state`` and ``85th_percentile_national`` columns to the DataFrame.

               This method calculates the 85th percentile for both state-specific and national purchase values.
               It flags the rows where the purchase value exceeds the threshold for the state and national levels.

               :param df: Input Pandas DataFrame containing the ``purchase`` column (and optionally ``state`` column).
               :type df: pandas.DataFrame
               :return: The DataFrame with two new binary columns:
                   - ``85th_percentile_state``: 1 if the purchase is above the state's 85th percentile, else 0.
                   - ``85th_percentile_national``: 1 if the purchase is above the national 85th percentile, else 0.
               :rtype: pandas.DataFrame
               """
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