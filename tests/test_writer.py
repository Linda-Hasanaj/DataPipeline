import pandas as pd
import pytest


from pipeline.write.writer import Writer


def _dummy_df():
    return pd.DataFrame({"a": [1, 2, 3]})


def test_writer_is_abstract():
    # You should NOT be able to instantiate an abstract class.
    with pytest.raises(TypeError):
        Writer(name="abstract")  # type: ignore[abstract]


def test_run_delegates_to_write_and_returns_rows_and_logs():
    """run() should:
       - log 'delegating' and 'done'
       - call write() exactly once with the SAME df object
       - return whatever integer write() returns
    """
    class DummyWriter(Writer):
        def __init__(self):
            super().__init__(name="dummy")
            self._logs = []
            self.log = lambda msg: self._logs.append(str(msg))
            self.calls = []
        def write(self, data: pd.DataFrame) -> int:
            self.calls.append(data)
            # Return number of rows to make it deterministic
            return len(data)

    df = _dummy_df()
    w = DummyWriter()

    out = w.run(df)

    # returned value is whatever write() returned
    assert out == len(df), "run() should return the value from write()"

    # write called exactly once and with the same df instance
    assert len(w.calls) == 1, "write() should be called exactly once"
    assert w.calls[0] is df, "run() must pass the same DataFrame instance to write()"

    # logs contain both messages
    joined = "\n".join(w._logs)
    assert "delegating to write()" in joined
    assert "done" in joined


def test_run_works_with_empty_dataframe_and_zero_rows_returned():
    class ZeroWriter(Writer):
        def __init__(self):
            super().__init__(name="zero")
            self._logs = []
            self.log = lambda msg: self._logs.append(str(msg))
        def write(self, data: pd.DataFrame) -> int:
            # Pretend nothing was written
            return 0

    df = pd.DataFrame()
    w = ZeroWriter()

    out = w.run(df)

    assert out == 0, "run() should return 0 when write() returns 0"
    assert any("delegating" in m for m in w._logs)
    assert any("done" in m for m in w._logs)
