import numpy as np
import pandas as pd
import pytest

# 🔧 Adjust this import to your layout.
# e.g. from pipeline.processors.percentile import PercentileProcessor
from pipeline.processors.percentile import PercentileProcessor


def _proc(config=None):
    p = PercentileProcessor(name="pct", config=config or {})
    p._logs = []
    p.log = lambda msg: p._logs.append(str(msg))
    return p


def test_returns_same_df_instance():
    df = pd.DataFrame({"purchase": [1, 2, 3], "state": ["A", "A", "B"]})
    p = _proc()
    out = p.process(df)
    assert out is df


def test_missing_purchase_adds_both_flags_zero_and_logs():
    df = pd.DataFrame({"state": ["A", "B", "A"]})
    p = _proc()

    out = p.process(df)

    assert out is df
    assert "85th_percentile_state" in df.columns
    assert "85th_percentile_national" in df.columns
    assert (df["85th_percentile_state"] == 0).all()
    assert (df["85th_percentile_national"] == 0).all()
    assert any("purchase' column missing" in m for m in p._logs)


def test_no_state_column_national_only_computed_state_all_zero_and_logs_warn():
    df = pd.DataFrame({"purchase": [10, 20, 30, 40, 50]})
    p = _proc({"percentile": 0.85})  # default is 0.85 but being explicit

    _ = p.process(df)

    # Expected: national 0/0/0/0/1 based on linear interpolation cut ~44.0
    expected_national = pd.Series([0, 0, 0, 0, 1], name="85th_percentile_national", dtype="int8")
    pd.testing.assert_series_equal(df["85th_percentile_national"], expected_national, check_names=False)

    # State flag should be all zeros because 'state' col missing path fills with NaNs and then cast to int8
    assert (df["85th_percentile_state"] == 0).all()
    assert any("state' column missing" in m for m in p._logs)


def test_with_state_column_per_state_and_national_cuts_default_int8_dtype():
    # A: [10, 20, 50]  -> 85th% ~ 41.0  -> state flag True only for 50 in A
    # B: [5, 30, 40]   -> 85th% ~ 37.0  -> state flag True only for 40 in B
    # All: [10, 20, 50, 5, 30, 40] -> 85th% ~ 42.5 -> national True only for 50
    df = pd.DataFrame({
        "purchase": [10, 20, 50, 5, 30, 40],
        "state":    ["A", "A", "A", "B", "B", "B"],
    })
    p = _proc()  # default percentile=0.85, output int

    _ = p.process(df)

    # State expectations: [0,0,1,0,0,1]
    expected_state = pd.Series([0, 0, 1, 0, 0, 1], name="85th_percentile_state", dtype="int8")
    pd.testing.assert_series_equal(df["85th_percentile_state"], expected_state, check_names=False)

    # National expectations: [0,0,1,0,0,0]
    expected_national = pd.Series([0, 0, 1, 0, 0, 0], name="85th_percentile_national", dtype="int8")
    pd.testing.assert_series_equal(df["85th_percentile_national"], expected_national, check_names=False)

    # Dtypes default to small ints for downstream DB work
    assert str(df["85th_percentile_state"].dtype) == "int8"
    assert str(df["85th_percentile_national"].dtype) == "int8"

    # Logged computed cuts summary
    assert any("Computed cuts:" in m for m in p._logs)


def test_bool_output_dtype_when_requested():
    df = pd.DataFrame({
        "purchase": [1.0, 2.0, 100.0, 3.0],
        "state":    ["X",   "X",  "Y",    "Y"],
    })
    p = _proc({"output_dtype": "bool"})  # any casing/str handled by impl

    _ = p.process(df)

    assert df["85th_percentile_state"].dtype == bool
    assert df["85th_percentile_national"].dtype == bool
    # Sanity: at least one True somewhere
    assert df["85th_percentile_national"].any() or df["85th_percentile_state"].any()


def test_all_nan_purchases_results_in_all_zero_flags():
    df = pd.DataFrame({"purchase": [np.nan, None, pd.NA], "state": ["A", "A", "B"]})
    p = _proc()

    _ = p.process(df)

    # valid.any() is False -> national_cut is NaN, comparisons are False -> 0 after int cast
    assert (df["85th_percentile_state"] == 0).all()
    assert (df["85th_percentile_national"] == 0).all()
    # Should have logged something about computation summary
    assert any("Computed cuts:" in m for m in p._logs)


def test_mixed_nan_and_values_handled_correctly():
    # Include NaNs and ensure only valid rows are considered for cuts
    df = pd.DataFrame({
        "purchase": [np.nan, 10.0, 50.0, np.nan, 40.0],
        "state":    ["A",    "A",   "A",  "B",   "B"],
    })
    p = _proc({"percentile": 0.85})

    _ = p.process(df)

    # A purchases considered: [10, 50] -> 85th% ~ 41.0 -> only 50 flagged in state
    # B purchases considered: [40]     -> 85th% is 40 -> 40 flagged in state
    # National considered: [10,50,40]  -> 85th% between 40 and 50 (~46.5) -> only 50 flagged
    expected_state = pd.Series([0, 0, 1, 0, 1], name="85th_percentile_state", dtype="int8")
    expected_national = pd.Series([0, 0, 1, 0, 0], name="85th_percentile_national", dtype="int8")
    pd.testing.assert_series_equal(df["85th_percentile_state"], expected_state, check_names=False)
    pd.testing.assert_series_equal(df["85th_percentile_national"], expected_national, check_names=False)
