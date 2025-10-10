from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict
import pandas as pd
from pipeline.task import Task

class Processor(Task, ABC):
    """This is the base processor, each subclass must implement this class which takes a pandas dataframe
    and returns a pandas dataframe"""
    def __init__(self, name: str, config: Dict[str, Any] | None=None) -> None:
        super().__init__(name = name, config = config or {})

    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Run the transformation on dataframe and return the result"""
        raise NotImplementedError()

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log("Processor.run() -> delegating to process()")
        return self.process(df)



