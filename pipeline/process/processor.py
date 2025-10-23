from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict
import pandas as pd
from pipeline.task import Task

"""
This module defines the :class:"Processor" abstract base class, which serves as the common interface dor all data 
transformation components within the pipeline.

Each subclass must implement the :meth:"process" method, defining a specific data transformation or computation logic.
Examples of concrete processors might include handling missing values, normalizing numerical data, or computing derived 
columns.

The :meth: "run" method is a unified xecution entry point used by the :class: "pipeline.orchestrator.Orchestrator",
ensuring consistency across all processors.

This class inherits from:
- :class: "pipeline.task.Task" - provides basic configuration and logging capabilities.
- :class: "abc.ABC" - enforces the implementation of abstract methods.
"""

class Processor(Task, ABC):
    """
    Abstract base class for all data processors in the pipeline.
    The "Processor" defines a standard interface for transforming data between the reading and writing stages
    of the pipeline. All processors must accept a pandas dataframe as input and return a transformed dataframe as output.

    Subclasses should focus solely on implementing the :meth:"process" method, which defines the transformation logic
    specific to that processor.

    :param name: The name of the processor instance.
    :type name: str
    :param config: Optional configuration dictionary containing processor parameters.
    :type config: dict[str, Any] | None

    Example usage:
    code-block:: python
        class DropNullsProcessor(Processor):
            def process(self, df: pd.DataFrame) -> pd.DataFrame:
                return df.dropna()

        processor = DropNullsProcessor(name="DropNulls")
        df = processor.run(df)
    """
    def __init__(self, name: str, config: Dict[str, Any] | None=None) -> None:
        super().__init__(name = name, config = config or {})

    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Abstract method that performs the core data transformation.

        This method must be implemented by all subclasses. It should take a pandas dataframe as input, perform the desired
        transformation, and return the transformed dataframe.

        :param df: Input dataframe to be processed
        :type df: pandas.DataFrame
        :return: The transformed dataframe
        :rtype: pandas.DataFrame
        :raises NotImplementedError: If the subclass does not implement this method.
        """
        raise NotImplementedError()

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Executes the processor by delegating to the :meth: "process" method.

        This method is called by the Orchestrator to ensure all processors follow a consistent interface. It also logs the
        execution for traceability.

        :param df: Input dataframe to be processed
        :type df: pandas.DataFrame
        :return: The processed dataframe
        :rtype: pandas.DataFrame
        """
        self.log("Processor.run() -> delegating to process()")
        return self.process(df)



