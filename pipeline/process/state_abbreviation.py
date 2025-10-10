from __future__ import annotations
import pandas as pd
import us
from pipeline.process.processor import Processor

class StateAbbreviationProcessor(Processor):
    """Add the column state_abbreviation which contain the abbreviated US state names"""

    def __init__(self, name: str, config: dict | None = None) -> None:
        super().__init__(name, config or {})
        # Build lowercase mapping to avoid title-casing pitfalls ("of", "and", etc.)
        self._name_to_abbr = {
            s.name.strip().casefold(): s.abbr
            for s in us.states.STATES_AND_TERRITORIES
        }
        self._abbr_set = {s.abbr for s in us.states.STATES_AND_TERRITORIES}

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log("Mapping state via 'us' library")
        if "state" not in df.columns:
            self.log("WARN: 'state' column missing; creating 'state_abbreviation' as NaN")
            df["state_abbreviation"] = pd.NA
            return df

        # Normalize input once
        state_raw = df["state"].astype("string")
        state_trim = state_raw.str.strip()

        # 1) Name-based mapping (lowercased keys)
        state_lower = state_trim.str.casefold()
        mapped_from_name = state_lower.map(self._name_to_abbr)

        # 2) Already an abbreviation? Keep it.
        state_upper = state_trim.str.upper()
        already_abbr = state_upper.where(state_upper.isin(self._abbr_set), other=pd.NA)

        # Prefer name mapping, else keep existing abbr
        df["state_abbreviation"] = mapped_from_name.fillna(already_abbr)

        # Optional: log unmapped values to help debugging
        unmapped = df.loc[state_trim.notna() & df["state_abbreviation"].isna(), "state"].unique()
        if len(unmapped) > 0:
            self.log(f"Unmapped states ({len(unmapped)}): {sorted(map(str, unmapped))}")

        return df
