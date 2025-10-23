from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict
import pandas as pd
from pipeline.task import Task

"""
This module defines the abstract base class Reader that serves as the interface for all reader components in the data pipeline.
A reader is responsible for loading input data from a source (CSV, API. database) into a Pandas dataframe, which will later be 
processed and written by other components in the pipeline.

Each subclass must implement the :meth: read method, defining how data is retrieved. The :meth: sun method provides a unified 
entry point for the pipeline orchestrator, delegating the execution to :meth: read.

This class inherits from: 
    :class: pipeline.task.Task - which provides common attributes like "name" and "config"
    :class: abc.ABC - enforcing the implementation of abstract methods.
"""
class Reader(Task, ABC):
    """
    Abstract base class for all data readers in the pipeline.

    The Reader defines a consistent interface for reading data from different sources. It ensures that every reader component
    implements the :meth: read method, which returns a pandas dataframe. This allows the Orchestrator to seamlessly integrate any
    reader implementation.

    :param name: The name of the reader component.
    :type name: str
    :param config: The configuration of the reader component (file paths, connection settings)
    :type config: dict[str, Any] | None
    """
    def __init__(self, name: str, config: Dict[str, Any] | None = None) -> None:
        super().__init__(name=name, config=config or {})

    @abstractmethod
    def read(self) -> pd.DataFrame:
        """
        Reads data from the source and returns it as a pandas dataframe.

        This abstract method must be implemented by subclasses to define the data retrieval logic (for example, reading from a CSV
        file or a database).

        :return: The dataset loaded into a pandas dataframe.
        :rtype: pd.DataFrame
        :raises: NotImplementedError: If a subclass does not implement this method.
        """
        raise NotImplementedError

    def run(self) -> pd.DataFrame:
        """
        Executes the reader component,
        This method is called by the Orchestrator to run the reader.
        It delegates the actual work to :meth: "read", while providing consistent logging behavior through
        :meth:"Task.log".

        :return: The dataset loaded into a pandas dataframe.
        :rtype: pd.DataFrame
        """
        self.log("Reader.run() -> delegating to read()")
        return self.read()