import pandas as pd
import pytest

# 🔧 IMPORTANT: fix this import to match your project layout.
# e.g. from pipeline.processors.base import Processor
from pipeline.process.processor import Processor


class _ProbeProcessor(Processor):
    """
    Minimal concrete implementation used only for testing the base class.
    Captures call order, the df passed to process(), and returns a modified df.
    """
    def __init__(self, name="probe", config=None):
        super().__init__(name=name, config=config)
        self._calls = []         # list of ("log"|"process", payload)
        self.seen_df = None      # the exact df object received by process()

    # Override log so we can inspect calls without touching Task internals.
    def log(self, msg):
        # Keep behavior as string; base class uses .log("...") with a string.
        self._calls.append(("log", str(msg)))

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        self._calls.append(("process", len(df)))
        self.seen_df = df
        # Return a new df to make assertion about return value straightforward.
        return df.assign(_processed=True)


def test_processor_is_abstract():
    # You should not be able to instantiate the abstract base class directly.
    with pytest.raises(TypeError):
        Processor(name="nope")  # type: ignore[abstract]


def test_run_delegates_to_process_and_returns_its_result():
    p = _ProbeProcessor()
    df_in = pd.DataFrame({"a": [1, 2]})

    df_out = p.run(df_in)

    # 1) run() must return exactly whatever process() returns
    assert isinstance(df_out, pd.DataFrame)
    assert "_processed" in df_out.columns, "run() should return the DataFrame produced by process()"

    # 2) process() must have been called exactly once
    process_calls = [c for c in p._calls if c[0] == "process"]
    assert len(process_calls) == 1, f"Expected 1 process() call, got {len(process_calls)}"

    # 3) run() must pass the SAME df object to process()
    assert p.seen_df is df_in, "run() must delegate the SAME DataFrame object to process()"


def test_run_logs_before_processing():
    p = _ProbeProcessor()
    df_in = pd.DataFrame({"a": [42]})

    _ = p.run(df_in)

    kinds = [k for (k, _) in p._calls]
    assert kinds[:2] == ["log", "process"], "run() should log before calling process()"

    # Optional: check the actual log message text
    log_msgs = [m for (k, m) in p._calls if k == "log"]
    assert any("Processor.run() -> delegating to process()" in m for m in log_msgs), \
        "Expected the run() log message to mention delegating to process()"
