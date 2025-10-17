import math
import numpy as np
import pandas as pd
import pytest

# 🔧 Adjust this import to your layout.
# e.g. from pipeline.processors.normalization import NormalizationProcessor
from pipeline.process.normalization import NormalizationProcessor


def _proc(config=None):
    p = NormalizationProcessor(name="norm", config=config or {})
    p._logs = []
    p.log = lambda msg: p._logs.append(str(msg))
    return p


def test_returns_same_df_instance():
    df = pd.DataFrame({"purchase": [1, 2, 3]})
    p = _proc()
    out = p.process(df)
    assert out is df


def test_missing_purchase_creates_normalization_purchases_and_logs_skip():
    df = pd.DataFrame({"x": [1, 2]})
    p = _proc()

    out = p.process(df)

    assert out is df
    # NOTE: implementation writes "normalization_purchases" (without the 'd')
    assert "normalization_purchases" in df.columns, \
        "Expected 'normalization_purchases' column when 'purchase' is missing"
    assert any("purchase column is missing, skipping normalization" in m.lower() for m in p._logs)


def test_all_non_numeric_sets_normalized_purchases_to_na_and_logs():
    # After to_numeric(..., errors='coerce'), everything becomes NaN
    df = pd.DataFrame({"purchase": ["a", None, "b"]})
    p = _proc()

    out = p.process(df)

    assert out is df
    assert "normalized_purchases" in df.columns
    assert df["normalized_purchases"].isna().all()
    assert any("No valid numeric purchase found" in m for m in p._logs)


def test_z_score_normalization_happy_path_logs_and_values():
    df = pd.DataFrame({"purchase": [10.0, 20.0, 30.0, None]})
    p = _proc({"method": "z_score"})

    _ = p.process(df)

    # Only numeric rows are used (None coerces to NaN)
    mean = df["purchase"].mean()
    std = df["purchase"].std()
    expected = (df["purchase"] - mean) / std
    # pandas: NaNs stay NaN in normalized column too
    pd.testing.assert_series_equal(df["normalized_purchases"], expected, check_names=False)

    assert any("Normalizing purchase column" in m for m in p._logs)
    assert any("Normalization 'purchase' column using method: z_score" in m for m in p._logs)


def test_z_score_zero_std_sets_zero_and_warns():
    df = pd.DataFrame({"purchase": [5.0, 5.0, 5.0, None]})
    p = _proc({"method": "z_score"})

    _ = p.process(df)

    # When std == 0, implementation fills 0 for entire column (including NaN rows)
    # NaN rows will also get 0 since they assign a scalar 0
    assert (df["normalized_purchases"] == 0).all()
    assert any("Standard deviation is zero" in m for m in p._logs)


def test_min_max_normalization_happy_path_logs_and_values():
    df = pd.DataFrame({"purchase": [10.0, 20.0, 30.0, None]})
    p = _proc({"method": "min_max"})

    _ = p.process(df)

    min_val = df["purchase"].min()
    max_val = df["purchase"].max()
    expected = (df["purchase"] - min_val) / (max_val - min_val)
    pd.testing.assert_series_equal(df["normalized_purchases"], expected, check_names=False)

    assert any("Normalization 'purchase' column using method: min_max" in m for m in p._logs)


def test_min_max_equal_min_max_sets_zero_and_warns():
    df = pd.DataFrame({"purchase": [7.0, 7.0, 7.0, None]})
    p = _proc({"method": "min_max"})

    _ = p.process(df)

    assert (df["normalized_purchases"] == 0).all()
    assert any("Min and max are equal" in m for m in p._logs)


def test_unknown_method_falls_back_to_z_score_behavior_and_logs_error():
    df = pd.DataFrame({"purchase": [1.0, 2.0, 4.0]})
    p = _proc({"method": "weird"})

    _ = p.process(df)

    # Implementation logs error and computes z-score anyway (without special-case zero-std)
    mean = df["purchase"].mean()
    std = df["purchase"].std()
    expected = (df["purchase"] - mean) / std
    pd.testing.assert_series_equal(df["normalized_purchases"], expected, check_names=False)

    assert any("ERROR: Unknown normalization method 'weird'" in m for m in p._logs)
