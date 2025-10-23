from __future__ import annotations
import pandas as pd
import us
from pipeline.process.processor import Processor

"""
This module defines the :class:`StateAbbreviationProcessor`, a concrete implementation
of :class:`pipeline.process.processor.Processor`.

The purpose of this processor is to map full state names to their official U.S.
abbreviations. It uses the **`us`** library to perform the mapping, ensuring that
state names are converted to the appropriate two-letter abbreviation.

If the state is already in abbreviation form, the processor leaves it unchanged.
If the state name is unrecognized, it logs the unmapped values and assigns a missing value (`NaN`)
to the ``state_abbreviation`` column.

This processor is commonly used to standardize state names before storing data in databases or performing analytics.

"""
class StateAbbreviationProcessor(Processor):
    """Processor that adds a column ``state_abbreviation`` containing the US state abbreviations.

        This processor uses the `us` library to map full state names (e.g., "California") to their
        corresponding two-letter abbreviations (e.g., "CA"). It handles both full state names and
        existing abbreviations in the ``state`` column, ensuring consistency across the dataset.

        If the state column is missing or contains unrecognized values, the processor will log a warning
        and populate the ``state_abbreviation`` column with missing values.

        :param name: The name assigned to this processor instance.
        :type name: str
        :param config: Optional configuration dictionary (unused in this processor).
        :type config: dict | None
        """

    def __init__(self, name: str, config: dict | None = None) -> None:
        super().__init__(name, config or {})
        # Build lowercase mapping to avoid title-casing pitfalls ("of", "and", etc.)
        self._name_to_abbr = {
            s.name.strip().casefold(): s.abbr
            for s in us.states.STATES_AND_TERRITORIES
        }
        self._abbr_set = {s.abbr for s in us.states.STATES_AND_TERRITORIES}

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Maps state names to their abbreviations and fills the ``state_abbreviation`` column.

                This method uses the ``us`` library to map full state names to their corresponding
                abbreviations. It also handles cases where the input column already contains abbreviations,
                or when unrecognized state names are encountered.

                :param df: Input Pandas DataFrame containing the ``state`` column.
                :type df: pandas.DataFrame
                :return: The DataFrame with a new ``state_abbreviation`` column.
                :rtype: pandas.DataFrame
                """
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
