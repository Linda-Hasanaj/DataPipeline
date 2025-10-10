from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict
import pandas as pd
from pipeline.task import Task

"""This class defines a common interface for all kinds of readers, every reader must implement the method
read and the method run"""
class Reader(Task, ABC):
    def __init__(self, name: str, config: Dict[str, Any] | None = None) -> None:
        super().__init__(name=name, config=config or {})

    @abstractmethod
    def read(self) -> pd.DataFrame:
        raise NotImplementedError

    # so Orchestrator can call reader.run()
    def run(self) -> pd.DataFrame:
        self.log("Reader.run() -> delegating to read()")
        return self.read()
