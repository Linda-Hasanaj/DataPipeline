from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict
import pandas as pd
from pipeline.task import Task

class Writer(Task, ABC):
    """Base writer interface that persists a dataframe somewhere, in my case into db."""
    def __init__(self, name: str, config: Dict[str, Any] | None=None) -> None:
        super().__init__(name=name, config=config or {})

    @abstractmethod
    def write(self, data: pd.DataFrame) -> None:
        raise NotImplementedError()

    # and adapter for the orchestrator
    def run(self, df: pd.DataFrame) -> int:
        self.log("Writer.run() -> delegating to write()")
        rows = self.write(df)
        self.log("Writer.run() -> done")
        return rows
