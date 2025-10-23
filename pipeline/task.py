# DataPipeline/pipeline/task.py

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

@dataclass
class Task:
    """
    Base class for all components in the data pipeline.

    This class serves as the foundation for the **Reader**, **Processor**, and **Writer**
    components of the data pipeline. It defines common attributes and a basic logging method shared
    across all pipeline tasks.

    Attributes
    name: str - identifier of the task. Used for logging and tracking purposes.
    config: doct[str, Any] - configuration dictionary containing task
    """
    name: str
    config: Dict[str, Any] = field(default_factory=dict)

    def log(self, msg: str) -> None:
        print(f"[{self.name}] {msg}")