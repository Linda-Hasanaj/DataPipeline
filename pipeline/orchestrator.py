from __future__ import annotations
from typing import Any, Dict, Protocol

"""The class orchestrator is like the controller of the pipeline, it wires the three stages of the pipeline together,
the Reader, Processor, Writer are in fact interfaces, and basically anything that has the method rin can be treated as
reader, processor or writer"""
class Reader(Protocol):
    def run(self) -> Any: ...

class Processor(Protocol):
    def run(self) -> Any: ...

class Writer(Protocol):
    def run(self) -> Any: ...

class Orchestrator:
    """Runs the 3 step pipeline"""
    def __init__(self, reader: Reader, processors: list[Processor], writer: Writer):
        self.reader = reader
        self.processors = processors
        self.writer = writer

    def run(self) -> int:
        print("[Orchestrator] Start")
        data = self.reader.run()
        for i, p in enumerate(self.processors, start=1):
            print(f"[Orchestrator] Processor {i}: {p.__class__.__name__}")
            data = p.run(data)
        rows = self.writer.run(data)  # return rows
        print("[Orchestrator] Done")
        return rows




