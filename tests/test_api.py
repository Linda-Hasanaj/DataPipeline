# tests/test_api.py
import pytest
from fastapi.testclient import TestClient

import api


@pytest.fixture
def client():
    return TestClient(api.app)


def test_health_ok(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_ingest_happy_path_uses_defaults_and_runs(client, monkeypatch):
    captured = {}

    class FakeOrchestrator:
        def __init__(self, reader=None, processors=None, writer=None):
            pass

        def run(self):
            captured["ran"] = True
            return 123

    def spy_build_pipeline(**kwargs):
        captured.update(kwargs)
        return FakeOrchestrator()

    monkeypatch.setattr(api, "build_pipeline", spy_build_pipeline, raising=True)

    r = client.post("/ingest", json={})
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
    assert r.json()["written_to"] == "public.marketing_data"
    assert r.json()["source"] == "data/dataset.csv"
    assert r.json()["if_exists"] == "replace"

    assert captured["csv_path"] == "data/dataset.csv"
    assert captured["sep"] == ","
    assert captured["dsn"].startswith("postgresql+psycopg://")
    assert captured["schema"] == "public"
    assert captured["table"] == "marketing_data"
    assert captured["if_exists"] == "replace"
    assert captured["chunksize"] == 5000
    assert captured.get("ran") is True
    assert captured.get("ran") is True

def test_ingest_happy_path_custom_args(client, monkeypatch):
    calls = {}

    class FakeOrchestrator:
        def run(self):
            calls["run"] = True
            return 77

    def spy_build_pipeline(**kwargs):
        calls["kwargs"] = kwargs
        return FakeOrchestrator()

    monkeypatch.setattr(api, "build_pipeline", spy_build_pipeline, raising=True)

    payload = {
        "path": "data/custom.csv",
        "sep": "|",
        "dsn": "postgresql+psycopg://u:p@h:5432/db",
        "dbschema": "myschema",
        "table": "mytable",
        "if_exists": "append",
        "chunksize": 999,
    }
    r = client.post("/ingest", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert data["written_to"] == "myschema.mytable"
    assert data["source"] == "data/custom.csv"
    assert data["if_exists"] == "append"

    kwargs = calls["kwargs"]
    assert kwargs["csv_path"] == "data/custom.csv"
    assert kwargs["sep"] == "|"
    assert kwargs["dsn"] == "postgresql+psycopg://u:p@h:5432/db"
    assert kwargs["schema"] == "myschema"
    assert kwargs["table"] == "mytable"
    assert kwargs["if_exists"] == "append"
    assert kwargs["chunksize"] == 999
    assert calls["run"] is True


def test_ingest_error_bubbles_as_500(client, monkeypatch):
    class FakeOrchestrator:
        def run(self):
            raise RuntimeError("boom")

    def spy_build_pipeline(**kwargs):
        return FakeOrchestrator()

    monkeypatch.setattr(api, "build_pipeline", spy_build_pipeline, raising=True)

    r = client.post("/ingest", json={})
    assert r.status_code == 500
    body = r.json()
    assert body["detail"] == "boom"
