import pandas as pd
import pytest

# Adjust this import to match your project structure if needed.
# e.g., from pipeline.reader import Reader  OR  from pipeline.readers.reader import Reader
from pipeline.read.base import Reader


def test_reader_is_abstract():
    """Reader is abstract and cannot be instantiated without implementing read()."""
    with pytest.raises(TypeError):
        Reader(name="base")  # abstractmethod not implemented


class DummyReader(Reader):
    """Concrete test double for Reader that records calls and logs into a list."""
    def __init__(self, name="dummy", config=None, df=None):
        super().__init__(name=name, config=config or {})
        self._df = df if df is not None else pd.DataFrame({"x": [1, 2]})
        self.read_called = 0
        self.logs = []

    # Override log so we don't rely on Task/logging.
    def log(self, msg: str) -> None:
        self.logs.append(str(msg))

    def read(self) -> pd.DataFrame:
        self.read_called += 1
        return self._df


def test_run_delegates_to_read_and_returns_dataframe():
    """run() must call read() exactly once and return its DataFrame."""
    expected = pd.DataFrame({"x": [10, 20, 30]})
    r = DummyReader(df=expected)

    result = r.run()

    # Delegation
    assert r.read_called == 1, "run() should call read() exactly once"
    # Return value
    pd.testing.assert_frame_equal(result, expected)


def test_run_logs_message_before_read():
    """run() should log the delegation message."""
    r = DummyReader()
    _ = r.run()

    assert any("Reader.run() -> delegating to read()" in m for m in r.logs), \
        "Expected delegation log message not found"


def test_init_sets_name_and_config():
    """Reader __init__ should pass name/config to base; we verify attributes exist and match."""
    cfg = {"a": 1, "b": "x"}
    r = DummyReader(name="reader-1", config=cfg)

    # These attributes are expected to be provided via Task.__init__ that Reader calls.
    # If your Task uses different attribute names, adjust assertions accordingly.
    assert getattr(r, "name", None) == "reader-1"
    assert getattr(r, "config", None) == cfg
