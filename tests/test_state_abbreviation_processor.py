import pandas as pd
import pytest
import us

# Adjust the import to your project layout if needed
from pipeline.process.state_abbreviation import StateAbbreviationProcessor


def _proc(config=None):
    """Create a processor and capture its logs on the instance."""
    p = StateAbbreviationProcessor(name="state-abbr", config=config or {})
    p._logs = []
    p.log = lambda msg: p._logs.append(str(msg))
    return p


# --- Capability discovery tied to the processor's data source ---
# Your processor builds its mapping ONLY from us.states.STATES_AND_TERRITORIES.
_ST = list(getattr(us.states, "STATES_AND_TERRITORIES", []))
_ABBRS = {s.abbr for s in _ST}
_NAMES_CF = {s.name.strip().casefold() for s in _ST}

_HAS_DC_ABBR = "DC" in _ABBRS
_HAS_PR_ABBR = "PR" in _ABBRS
_HAS_DC_NAME = "district of columbia" in _NAMES_CF  # may differ per us version


def _param_cases():
    """Parametrize only with cases guaranteed to be supported by THIS env."""
    base = [
        ("New York", "NY"),    # full name
        ("  texas ", "TX"),    # trim + casefold
        ("ca", "CA"),          # already an abbreviation (lower to upper)
    ]
    if _HAS_DC_ABBR:
        base.append(("DC", "DC"))
    if _HAS_DC_NAME:
        base.append(("district of columbia", "DC"))
    if _HAS_PR_ABBR:
        base.append(("pr", "PR"))
    return base


def test_returns_same_df_instance():
    df = pd.DataFrame({"state": ["New York", "CA"]})
    p = _proc()

    out = p.process(df)

    assert out is df, "process() should return the same DataFrame instance"


def test_missing_state_column_creates_na_and_logs():
    df = pd.DataFrame({"other": [1, 2]})
    p = _proc()

    _ = p.process(df)

    assert "state_abbreviation" in df.columns, "Expected 'state_abbreviation' to be created"
    assert df["state_abbreviation"].isna().all(), "Expected NaNs when 'state' column missing"
    assert any("column missing" in m.lower() for m in p._logs), "Expected missing-column warning log"


@pytest.mark.parametrize("raw, expected", _param_cases())
def test_maps_names_and_preserves_valid_abbreviations(raw, expected):
    df = pd.DataFrame({"state": [raw]})
    p = _proc()

    p.process(df)

    assert df.loc[0, "state_abbreviation"] == expected


def test_unmapped_values_become_na_and_are_logged():
    df = pd.DataFrame({"state": ["Narnia", "CA", "Atlantis"]})
    p = _proc()

    p.process(df)

    # Values
    assert pd.isna(df.loc[0, "state_abbreviation"]), "Unrecognized name should become NA"
    assert df.loc[1, "state_abbreviation"] == "CA", "Known abbreviation should be kept"
    assert pd.isna(df.loc[2, "state_abbreviation"]), "Unrecognized name should become NA"

    # Log contains an unmapped summary with count and the values
    joined_logs = "\n".join(p._logs)
    assert "Unmapped states (" in joined_logs
    assert "Narnia" in joined_logs and "Atlantis" in joined_logs


def test_blank_and_null_inputs_become_na():
    df = pd.DataFrame({"state": ["", "   ", None, pd.NA]})
    p = _proc()

    p.process(df)

    assert df["state_abbreviation"].isna().all(), "Blank/NA inputs should map to NA"


def test_does_not_clobber_other_columns():
    df = pd.DataFrame({
        "state": ["New York", "CA"],
        "keep_me": [1, 2],
    })
    p = _proc()

    p.process(df)

    assert "keep_me" in df.columns and list(df["keep_me"]) == [1, 2], "Other columns must be preserved"
    assert "state_abbreviation" in df.columns, "Expected added abbreviation column"


def test_idempotent_results_when_run_twice():
    df = pd.DataFrame({"state": ["Nevada", "XX", "FL"]})
    p = _proc()

    p.process(df)
    first = df["state_abbreviation"].copy()

    # Run again
    p.process(df)
    second = df["state_abbreviation"].copy()

    pd.testing.assert_series_equal(first, second, check_names=False)
