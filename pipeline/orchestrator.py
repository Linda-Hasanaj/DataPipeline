from __future__ import annotations
from typing import Any, Dict, Protocol

import data

"""
This module defines the **Orchestrator** class, which acts as the central controller for executing
the data pipeline. It coordinates the three key stages:
1. Reader - loads the raw data from the csv (or any other format)
2. Processor - performs data transformation or enrichment
3. Writer - stores the final, processed data

Each of these components implements a run() method, following a lightweight interface defined via python Protocol classes. The
Orchestrator is designed to remain generic - any class conforming to these interfaces can be plugged in.
"""
class Reader(Protocol):
    """
    Interface for the Reader component in the data pipeline.
    A reader is responsible for loading the raw input data from a source such as a CSV file, API or database.

    :return: The raw data to be processed (a pandas dataframe)
    :rtype: Any
    """
    def run(self) -> Any: ...

class Processor(Protocol):
    """
    Interface for the Processor component in the data pipeline.
    A Processor transforms, cleans, or enriches the data.
    Multiple processors can be chained together.

    :param data: Input data from the previous stage.
    :type data: Any
    :return: Transformed data.
    :rtype: Any
    """
    def run(self) -> Any: ...

class Writer(Protocol):
    """
    Interface for the Writer component in the data pipeline.

    A Writer is responsible for persisting the final data - for example, inserting into PostgreSQL table.
    :param data: The processed data to be stored.
    :type data: Any
    :return: The number of rows written to the storage
    :rtype: int
    """
    def run(self) -> Any: ...

class Orchestrator:
    """
    Coordinates the execution of the three-stage data pipeline.
    The Orchestrator sequentially executes the reader,one or more processors, and the writer.
    It logs progress through printed messages.

    :param reader: The component that reads or loads raw data.
    :type reader: Reader
    :param processors: A list of processor components to apply transformations.
    :type processors: list[Processor]
    :param writer: The component writes the processed data to the destination
    :type writer: Writer
    """
    def __init__(self, reader: Reader, processors: list[Processor], writer: Writer):
        self.reader = reader
        self.processors = processors
        self.writer = writer

    def run(self) -> int:
        """
        Executes the entire data pipeline.

        This method orchestrates the flow:
        1. Calls reader.run() to load the initial data
        2. Passes the result through each processor sequentially
        3. Sends the final output to writer.run()

        :return: The number of rows written by the writer.
        :rtype: int
        """
        print("[Orchestrator] Start")
        data = self.reader.run()
        for i, p in enumerate(self.processors, start=1):
            print(f"[Orchestrator] Processor {i}: {p.__class__.__name__}")
            data = p.run(data)
        rows = self.writer.run(data)  # return rows
        print("[Orchestrator] Done")
        return rows