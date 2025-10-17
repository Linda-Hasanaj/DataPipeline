import os
import pandas as pd
import pytest

# Adjust this import to match your project structure if needed.
# e.g. from pipeline.read.csv_reader import CSVReader
from pipeline.read.csvreader import CSVReader


def _reader_with_log_capture(config):
    """Helper: create a CSVReader and capture its log messages in r._logs."""
    r = CSVReader(name="csv", config=config)
    r._logs = []
    # Monkeypatch the instance's log method to capture messages without touching Task internals
    r.log = lambda msg: r._logs.append(str(msg))
    return r


def test_read_returns_empty_df_and_logs_when_path_missing(tmp_path):
    r = _reader_with_log_capture(config={})  # no 'path' key at all
    df = r.read()

    assert isinstance(df, pd.DataFrame)
    assert df.empty, "Expected empty DataFrame when path is missing"
    assert any("File not found" in m for m in r._logs), "Missing 'File not found' log"


def test_read_returns_empty_df_and_logs_when_path_does_not_exist(tmp_path):
    non_existent = tmp_path / "nope.csv"
    r = _reader_with_log_capture(config={"path": str(non_existent)})

    df = r.read()

    assert df.empty
    assert any("File not found" in m for m in r._logs)
    assert any("Reading CSV from" in m for m in r._logs), "Should log the attempted path"


def test_read_valid_csv_with_default_separator(tmp_path):
    p = tmp_path / "data.csv"
    # default sep is comma
    p.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")

    r = _reader_with_log_capture(config={"path": str(p)})
    df = r.read()

    pd.testing.assert_frame_equal(df, pd.DataFrame({"a": [1, 3], "b": [2, 4]}))
    assert any("Read 2 rows x 2 cols" in m for m in r._logs)


def test_read_valid_csv_with_custom_separator(tmp_path):
    p = tmp_path / "data.tsv"
    p.write_text("a\tb\n10\t20\n30\t40\n", encoding="utf-8")

    r = _reader_with_log_capture(config={"path": str(p), "sep": "\t"})
    df = r.read()

    pd.testing.assert_frame_equal(df, pd.DataFrame({"a": [10, 30], "b": [20, 40]}))
    # also verify the log shows the custom sep
    assert any("sep='\t'" in m for m in r._logs)


def test_run_delegates_to_read_and_logs(tmp_path, monkeypatch):
    p = tmp_path / "data.csv"
    p.write_text("x,y\n5,6\n", encoding="utf-8")

    r = _reader_with_log_capture(config={"path": str(p)})

    # Spy on read() calls
    called = {"n": 0}
    original = r.read

    def spy_read():
        called["n"] += 1
        return original()

    monkeypatch.setattr(r, "read", spy_read)

    out = r.run()

    assert called["n"] == 1, "run() must call read() exactly once"
    pd.testing.assert_frame_equal(out, pd.DataFrame({"x": [5], "y": [6]}))

    # Inherited Reader.run() should log the delegation message
    assert any("Reader.run() -> delegating to read()" in m for m in r._logs)


def test_config_is_not_mutated_by_reader(tmp_path):
    p = tmp_path / "data.csv"
    p.write_text("a,b\n1,2\n", encoding="utf-8")

    cfg = {"path": str(p)}  # no 'sep' provided; default is used internally
    r = _reader_with_log_capture(config=cfg)

    _ = r.read()

    # Reader should not inject defaults back into the user-provided config
    assert "sep" not in cfg, "Reader must not mutate the provided config dict"
