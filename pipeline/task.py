# DataPipeline/pipeline/task.py

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

@dataclass
class Task:
    """The task class is the base class for all components in the data pipeline (Readers, Processors, Writers).
    It defines two common attributes:
    name: the identifier of the task
    config: a dictionary for configuration options"""
    name: str
    config: Dict[str, Any] = field(default_factory=dict)

    def log(self, msg: str) -> None:
        print(f"[{self.name}] {msg}")

