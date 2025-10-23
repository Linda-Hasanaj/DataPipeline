from __future__ import annotations
import pandas as pd
from pipeline.process.processor import Processor

"""
This module defines the :class: "ConversionProcessor", a concrete subclass of :class: "pipeline.process.processor.Processor"
It's main function is to derive the converted column from the existing purchase field in the dataset, indicating whether a 
purchase (conversion) occurred.

This processor is typically one of the early steps in the pipeline as it enriches the dataset with a binary conversion indicator
used for downstream analysis such as conversion rate computation and percentile evaluation. 
"""
class ConversionProcessor(Processor):
    """
    Processor that adds a conversion indicator column to the dataset.

    The ConversionProcessor inspects the purchase column and adds a new boolean-like integer column name converted
    where:
    -1. indicates a successful purchase (non-null value in purchase)
    -0. indicates no purchase (null value in purchase)

    This helps in computing conversion metrics such as overall conversion rate or state-level conversion performance.

    :param name: The name assigned to this processor instance.
    :type name: str
    :param config: Optional configuration dictionary.
    :type config: dict | None
    """

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds the converted column to the dataframe. The method checks the purchase column for non-null values and converts
        this boolean mask into an integer column where 1 indicates a sale and 0 indicates no sale.

        :param df: The input pandas dataframe containing a purchase column.
        :type df: pandas.DataFrame
        :return: The dataframe with an additional converted column.
        :rtype: pandas.DataFrame
        """
        self.log("Creating converted column from purchase")
        df["converted"] = df["purchase"].notnull().astype(int)
        return df