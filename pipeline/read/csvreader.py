from __future__ import annotations
import os
import pandas as pd
from pipeline.read.base import Reader

"""
This module the :class: "CSVReader", a concrete subclass of :class: "pipeline.read.base.Reader". It is responsible for reading
a CSV file from a specified path into a pandas DataFrame.

This reader is typically the first stage of the data pipeline, providing raw data that will later be processed by other pipeline
components.

Configuration
The reader accepts the following configuration options through the config dictionary:
- path (str): Path to the CSV file. Required.
- sep (str, optional): Field separator used in the CSV. Defaults to ','.

Behavior
If the specified CSV file does not exist, the reader logs a warning and returns an empty DataFrame instead of raising an error.
This design ensures that the pipeline continues gracefully.

"""

class CSVReader(Reader):
    """
    Concrete implementation of :class: "CSVReader" for reading CSV files.

    This class uses a Pandas :func: "pandas.read_csv" function to load tabular data from a CSV file into a pandas DataFrame.
    It also handles basic validation and logging, ensuring that the pipeline remains stable even when the file is missing.

    :param name: The name assigned to this reader instance (for logging or debugging).
    :type name: str
    :param config: A dictionary containing reader configuration parameters.
                    Must include "path" and optionally "sep".
    :type config: dict | None

    Example usage:
    code-block:: python
    reader = CSVReader(name="CSVInput", config={"path": "data/dataset.csv", "sep": ","})
        df = reader.run()
        print(df.head())

    """

    def __init__(self, name: str, config: dict | None = None) -> None:
        super().__init__(name=name, config=config or {})

    def read(self) -> pd.DataFrame:
        """Reads data from a CSV file into a Pandas DataFrame.

            The method uses the configuration dictionary to locate and load the file.
            If the file path is invalid or does not exist, an empty DataFrame is returned.

           :return: The contents of the CSV file as a Pandas DataFrame.
           :rtype: pandas.DataFrame
           :raises FileNotFoundError: (internally handled) If the file path is not valid.
            """
        path = self.config.get("path")
        sep  = self.config.get("sep", ",")
        self.log(f"Reading CSV from {path} (sep='{sep}')")

        if not path or not os.path.exists(path):
            self.log(f"File not found: {path}. Returning empty DataFrame so pipeline can continue.")
            return pd.DataFrame()

        df = pd.read_csv(path, sep=sep)
        self.log(f"Read {len(df)} rows x {len(df.columns)} cols")
        return df