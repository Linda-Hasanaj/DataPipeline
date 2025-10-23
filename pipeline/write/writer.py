from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict
import pandas as pd
from pipeline.task import Task

"""
writer.py
=========

This module defines the :class:`Writer` abstract base class, which serves as the interface
for all writer components in the pipeline.

A Writer is responsible for **persisting data** from a Pandas DataFrame to a specified destination,
such as a database or file. The :class:`Writer` class defines the basic structure for how
data should be written, leaving the implementation details to subclasses.

The :class:`Writer` class inherits from :class:`pipeline.task.Task`, allowing writers to have
a name, configuration, and logging capabilities.

Subclasses of :class:`Writer` must implement the :meth:`write` method, which is responsible
for the actual data persistence. For example, the :class:`PostgreSQLStorage` class implements
this interface to write data to a PostgreSQL database.
"""

class Writer(Task, ABC):
    """Abstract base class for all data writers in the pipeline.

    This class defines a common interface for any component that persists data,
    such as storing it in a database. It ensures that each writer subclass will implement
    the :meth:`write` method to perform the actual persistence logic.

    :param name: The name of the writer instance.
    :type name: str
    :param config: Configuration dictionary containing writer-specific settings.
    :type config: dict[str, Any] | None
    """
    def __init__(self, name: str, config: Dict[str, Any] | None=None) -> None:
        super().__init__(name=name, config=config or {})

    @abstractmethod
    def write(self, data: pd.DataFrame) -> None:
        """Writes data to a specified destination (e.g., a database or file).

               This method is abstract and must be implemented by subclasses to define
               how data is persisted. The method should accept a Pandas DataFrame and return
               nothing. Instead, the implementation should write the data to the destination.

               :param data: The Pandas DataFrame to be persisted.
               :type data: pandas.DataFrame
               :raises NotImplementedError: If the subclass does not implement this method.
               """
        raise NotImplementedError()

    def run(self, df: pd.DataFrame) -> int:
        """Executes the write operation.

               This method is an adapter for the Orchestrator class. It delegates the actual
               writing to the :meth:`write` method and logs the progress. It returns the
               number of rows written.

               :param df: The DataFrame to be written.
               :type df: pandas.DataFrame
               :return: The number of rows written by the writer.
               :rtype: int
               """
        self.log("Writer.run() -> delegating to write()")
        rows = self.write(df)
        self.log("Writer.run() -> done")
        return rows
