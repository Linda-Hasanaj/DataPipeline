import pandas as pd
import pytest

# 🔧 Adjust this import to your layout.
# e.g. from pipeline.processors.conversion import ConversionProcessor
from pipeline.process.conversion import ConversionProcessor


def _processor_with_log_capture():
    p = ConversionProcessor(name="conv")
    p._logs = []
    p.log = lambda msg: p._logs.append(str(msg))  # capture logs
    return p


def test_process_adds_converted_column_and_values_match_notnull():
    df = pd.DataFrame({
        "purchase": [None, pd.NA, 123, "", 0],
        "other": [1, 2, 3, 4, 5],
    })
    p = _processor_with_log_capture()

    out = p.process(df)

    # Returns same object (in-place)
    assert out is df, "process() should return the same DataFrame instance"

    # Column added
    assert "converted" in df.columns, "Expected 'converted' column to be created"

    # Exactly matches .notnull().astype(int)
    expected = df["purchase"].notnull().astype(int)
    pd.testing.assert_series_equal(df["converted"], expected, check_names=False)

    # Integer dtype
    assert pd.api.types.is_integer_dtype(df["converted"]), "'converted' should be integer dtype"

    # Other columns unchanged
    pd.testing.assert_series_equal(df["other"], pd.Series([1, 2, 3, 4, 5], name="other"))

    # Logged once with expected message
    assert any("Creating converted column from purchase" in m for m in p._logs), \
        "Expected a log about creating the converted column"
    # exactly one log call from this method
    assert len(p._logs) == 1, "process() should log exactly once"


def test_process_raises_keyerror_when_purchase_missing_but_logs_first():
    df = pd.DataFrame({"not_purchase": [1, 2, 3]})
    p = _processor_with_log_capture()

    with pytest.raises(KeyError):
        p.process(df)

    # It should have logged before failing on missing column
    assert any("Creating converted column from purchase" in m for m in p._logs), \
        "Expected a log even when 'purchase' column is missing"
