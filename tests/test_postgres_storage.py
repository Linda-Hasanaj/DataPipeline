import pandas as pd
import pytest
from sqlalchemy.exc import SQLAlchemyError

# Adjust this import to your project layout if needed
from pipeline.write.postgres_storage import PostgreSQLStorage
from sqlalchemy.dialects.postgresql import BIGINT, NUMERIC, BOOLEAN, VARCHAR


# ------------------- Test doubles for SQLAlchemy engine/connection -------------------

class _FakeConn:
    def __init__(self, engine):
        self._engine = engine

    def execute(self, clause):
        # record executed SQL text
        self._engine.executes.append(str(clause))


class _BeginCtx:
    def __init__(self, engine):
        self._engine = engine

    def __enter__(self):
        self._engine.begin_calls += 1
        return _FakeConn(self._engine)

    def __exit__(self, exc_type, exc, tb):
        return False  # don't swallow exceptions


class _FakeEngine:
    def __init__(self):
        self.begin_calls = 0
        self.executes = []
        self.disposed = False

    def begin(self):
        return _BeginCtx(self)

    def dispose(self):
        self.disposed = True


@pytest.fixture
def fake_engine(monkeypatch):
    """
    Patch create_engine inside the module under test to return our fake engine.
    Returns (engine_holder, created_engine) so tests can inspect it.
    """
    created = {"engine": None, "dsn": None, "pool_pre_ping": None}

    def _fake_create_engine(dsn, pool_pre_ping=True):
        eng = _FakeEngine()
        created["engine"] = eng
        created["dsn"] = dsn
        created["pool_pre_ping"] = pool_pre_ping
        return eng

    # IMPORTANT: patch the symbol where it's used, not in sqlalchemy
    import pipeline.write.postgres_storage as mod
    monkeypatch.setattr(mod, "create_engine", _fake_create_engine)

    return created


@pytest.fixture
def capture_to_sql(monkeypatch):
    """
    Monkeypatch DataFrame.to_sql to capture call args and optionally raise.
    """
    calls = {"calls": [], "raise": None}

    def _fake_to_sql(self, name, con, schema, if_exists, index, chunksize, method, dtype):
        calls["calls"].append(
            dict(name=name, schema=schema, if_exists=if_exists, index=index,
                 chunksize=chunksize, method=method, dtype=dtype)
        )
        if calls["raise"]:
            raise calls["raise"]

    monkeypatch.setattr(pd.DataFrame, "to_sql", _fake_to_sql, raising=True)
    return calls


def _storage(config=None):
    s = PostgreSQLStorage(name="pg", config=config or {})
    s._logs = []
    s.log = lambda msg: s._logs.append(str(msg))
    return s


# ------------------------------------- Tests -------------------------------------


def test_require_raises_when_mandatory_keys_missing():
    df = pd.DataFrame({"a": [1]})
    s = _storage(config={"table": "t"})  # missing dsn
    with pytest.raises(ValueError) as e:
        s.write(df)
    assert "Missing required config key: dsn" in str(e.value)


def test_engine_created_once_with_masked_dsn_and_schema_created(fake_engine, capture_to_sql):
    df = pd.DataFrame({"a": [1]})
    cfg = {"dsn": "postgresql://user:secret@localhost/db", "table": "events", "schema": "myschema"}
    s = _storage(cfg)

    n = s.write(df)

    # row count returned
    assert n == 1

    # engine created with pool_pre_ping=True
    assert fake_engine["engine"] is not None
    assert fake_engine["dsn"] == cfg["dsn"]
    assert fake_engine["pool_pre_ping"] is True

    # schema create executed
    eng = fake_engine["engine"]
    assert any('CREATE SCHEMA IF NOT EXISTS "myschema"' in sql for sql in eng.executes)

    # DSN masked in logs, password not present
    logs = "\n".join(s._logs)
    assert "Connecting via DSN: postgresql://user:***@localhost/db" in logs
    assert "secret" not in logs

    # Disposed & logged
    assert eng.disposed is True
    assert any("Disconnected" in m for m in s._logs)

    # Final done log present
    assert any("Done  writing to myschema.events" in m for m in s._logs)


def test_to_sql_parameters_and_config_passthrough(fake_engine, capture_to_sql):
    df = pd.DataFrame({"a": [1, 2, 3]})
    cfg = {
        "dsn": "postgresql://u:p@h/db",
        "table": "table_x",
        "schema": "s",
        "if_exists": "replace",
        "chunksize": 500,
        "index": True,
    }
    s = _storage(cfg)

    _ = s.write(df)

    call = capture_to_sql["calls"][0]
    assert call["name"] == "table_x"
    assert call["schema"] == "s"
    assert call["if_exists"] == "replace"
    assert call["index"] is True
    assert call["chunksize"] == 500
    assert call["method"] == "multi"
    # dtype inferred (dict)
    assert isinstance(call["dtype"], dict)


def test_dtype_override_from_config(fake_engine, capture_to_sql):
    df = pd.DataFrame({"a": [1]})
    from sqlalchemy.dialects.postgresql import VARCHAR
    custom = {"a": VARCHAR(42)}
    s = _storage({"dsn": "postgresql://u:p@h/db", "table": "t", "schema": "s", "dtype": custom})

    _ = s.write(df)

    call = capture_to_sql["calls"][0]
    assert call["dtype"] is custom  # passed as-is


def test_infer_types_mapping():
    df = pd.DataFrame({
        "i": pd.Series([1, 2], dtype="int64"),
        "f": pd.Series([1.2, 3.4], dtype="float64"),
        "b": pd.Series([True, False], dtype="bool"),
        "s": pd.Series(["x", "y"], dtype="object"),
    })
    s = _storage({})
    mapping = s._infer_types(df)

    assert isinstance(mapping["i"], BIGINT)
    assert isinstance(mapping["f"], NUMERIC)
    # numeric precision/scale configured
    assert mapping["f"].precision == 38 and mapping["f"].scale == 10
    assert isinstance(mapping["b"], BOOLEAN)
    assert isinstance(mapping["s"], VARCHAR)


def test_error_path_logs_and_raises(fake_engine, capture_to_sql):
    df = pd.DataFrame({"a": [1]})
    s = _storage({"dsn": "postgresql://u:p@h/db", "table": "t", "schema": "s"})
    capture_to_sql["raise"] = SQLAlchemyError("boom")

    with pytest.raises(SQLAlchemyError):
        s.write(df)

    # error logged
    assert any("Error: database write failed:" in m for m in s._logs)
    # engine disposed even on error
    assert fake_engine["engine"].disposed is True


def test_no_schema_when_empty(fake_engine, capture_to_sql):
    """If schema evaluates falsey, _ensure_schema should be a no-op."""
    df = pd.DataFrame({"a": [1]})
    s = _storage({"dsn": "postgresql://u:p@h/db", "table": "t", "schema": ""})

    _ = s.write(df)

    # began twice: write() + ensure_schema? When schema empty, only write() begin should happen.
    assert fake_engine["engine"].begin_calls == 1
    assert fake_engine["engine"].executes == []  # no CREATE SCHEMA executed
