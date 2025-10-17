import pandas as pd
import pytest

# 🔧 Adjust this import to your layout.
# e.g. from pipeline.processors.missing_values import MissingValuesProcessor
from pipeline.process.missing_value import MissingValuesProcessor


def _proc(strategy=None):
    cfg = {} if strategy is None else {"strategy": strategy}
    p = MissingValuesProcessor(name="mv", config=cfg)
    p._logs = []
    p.log = lambda msg: p._logs.append(str(msg))
    return p


def test_process_fills_mean_by_default_and_logs():
    df = pd.DataFrame({"time_spent_seconds": [10.0, None, 100.0, 100.0], "x": [1, 2, 3, 4]})
    expected_value = df["time_spent_seconds"].mean()  # (10 + 100 + 100)/3 = 70.0
    p = _proc()  # default = mean

    out = p.process(df)

    # In-place and same object returned
    assert out is df

    # All former NaNs replaced with mean
    assert df["time_spent_seconds"].isna().sum() == 0
    # Check that at least one formerly-NaN spot equals expected_value
    assert expected_value in df["time_spent_seconds"].values

    # Other columns untouched
    pd.testing.assert_series_equal(df["x"], pd.Series([1, 2, 3, 4], name="x"))

    # Logs: first "Filling...", then "Filled..."
    assert any("Filling missing time_spent_seconds using mean strategy" in m for m in p._logs)
    assert any("Filled missing values with" in m for m in p._logs)
    # formatted value with two decimals in the final log
    assert f"{expected_value:.2f}" in p._logs[-1]
    assert len(p._logs) == 2, "Expected exactly two log messages when filling"


def test_process_fills_median_when_configured_and_logs():
    df = pd.DataFrame({"time_spent_seconds": [10.0, None, 100.0, 100.0]})
    expected_value = df["time_spent_seconds"].median()  # 100.0
    p = _proc("median")

    out = p.process(df)

    assert out is df
    assert df["time_spent_seconds"].isna().sum() == 0
    assert expected_value in df["time_spent_seconds"].values

    assert any("Filling missing time_spent_seconds using median strategy" in m for m in p._logs)
    assert any("Filled missing values with" in m for m in p._logs)
    assert f"{expected_value:.2f}" in p._logs[-1]
    assert len(p._logs) == 2


def test_process_noop_when_column_missing_logs_and_returns_same_df():
    df = pd.DataFrame({"other": [1, 2, 3]})
    p = _proc("mean")

    out = p.process(df)

    assert out is df
    assert any("Column time_spent_seconds not found, skipping this step" in m for m in p._logs)
    # Only the "not found" log; no filling logs
    assert len(p._logs) == 1


def test_process_raises_on_unknown_strategy_and_logs_first():
    df = pd.DataFrame({"time_spent_seconds": [1.0, None, 3.0]})
    p = _proc("mode")  # invalid

    with pytest.raises(ValueError) as ei:
        p.process(df)

    assert "Unknown strategy 'mode'" in str(ei.value)
    # It should have logged the "Filling..." message before raising
    assert any("Filling missing time_spent_seconds using mode strategy" in m for m in p._logs)
    # No "Filled..." log should be present
    assert not any(m.startswith("Filled missing values with") for m in p._logs)
