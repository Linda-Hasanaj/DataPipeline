# tests/test_orchestrator.py
from typing import Any, List
import pytest

from pipeline.orchestrator import Orchestrator  # e.g., pipeline/orchestrator.py


class FakeReader:
    def __init__(self, data: List[int]) -> None:
        self._data = list(data)
        self.calls = 0

    def run(self) -> List[int]:
        self.calls += 1
        return list(self._data)


class AddProcessor:
    def __init__(self, inc: int) -> None:
        self.inc = inc
        self.inputs: list[list[int]] = []

    def run(self, data: List[int]) -> List[int]:
        self.inputs.append(list(data))
        return [x + self.inc for x in data]


class MulProcessor:
    def __init__(self, factor: int) -> None:
        self.factor = factor
        self.inputs: list[list[int]] = []

    def run(self, data: List[int]) -> List[int]:
        self.inputs.append(list(data))
        return [x * self.factor for x in data]


class FakeWriter:
    def __init__(self) -> None:
        self.received: list[int] | None = None
        self.calls = 0

    def run(self, data: List[int]) -> int:
        self.calls += 1
        self.received = list(data)
        return len(data)


class BoomProcessor:
    def run(self, data: Any) -> Any:
        raise RuntimeError("boom")



def test_happy_path_runs_in_order_and_returns_rows(capsys):
    reader = FakeReader([1, 2, 3])
    p1 = AddProcessor(inc=1)      # -> [2,3,4]
    p2 = MulProcessor(factor=2)   # -> [4,6,8]
    writer = FakeWriter()

    orch = Orchestrator(reader=reader, processors=[p1, p2], writer=writer)
    rows = orch.run()

    assert rows == 3

    assert reader.calls == 1

    assert p1.inputs == [[1, 2, 3]]
    assert p2.inputs == [[2, 3, 4]]

    assert writer.calls == 1
    assert writer.received == [4, 6, 8]

    out = capsys.readouterr().out.splitlines()
    assert "[Orchestrator] Start" in out[0]
    assert any("Processor 1: AddProcessor" in line for line in out)
    assert any("Processor 2: MulProcessor" in line for line in out)
    assert "[Orchestrator] Done" in out[-1]


def test_no_processors_passes_reader_output_directly_to_writer():
    reader = FakeReader([10, 20])
    writer = FakeWriter()

    orch = Orchestrator(reader=reader, processors=[], writer=writer)
    rows = orch.run()

    assert rows == 2
    assert writer.received == [10, 20]


def test_exception_in_processor_bubbles_and_writer_not_called():
    reader = FakeReader([1, 2, 3])
    bad = BoomProcessor()
    writer = FakeWriter()

    orch = Orchestrator(reader=reader, processors=[bad], writer=writer)

    with pytest.raises(RuntimeError, match="boom"):
        orch.run()

    assert writer.calls == 0
    assert writer.received is None
