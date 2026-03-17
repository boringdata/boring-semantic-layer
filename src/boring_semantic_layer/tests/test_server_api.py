"""Tests for the FastAPI HTTP transport."""

from __future__ import annotations

import ibis
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.server import create_app
from boring_semantic_layer.server.loader import load_models


@pytest.fixture(scope="module")
def sample_models():
    """Create sample semantic models for HTTP API tests."""
    con = ibis.duckdb.connect(":memory:")

    flights_df = pd.DataFrame(
        {
            "origin": ["JFK", "LAX", "ORD"] * 10,
            "destination": ["LAX", "JFK", "DEN"] * 10,
            "carrier": ["AA", "UA", "DL"] * 10,
            "flight_date": pd.date_range("2024-01-01", periods=30, freq="D"),
            "dep_delay": [5.2, 8.1, 3.5] * 10,
        },
    )
    carriers_df = pd.DataFrame(
        {
            "code": ["AA", "UA", "DL"],
            "name": ["American", "United", "Delta"],
        },
    )

    flights_tbl = con.create_table("flights_http", flights_df, overwrite=True)
    carriers_tbl = con.create_table("carriers_http", carriers_df, overwrite=True)

    flights_model = (
        to_semantic_table(
            flights_tbl,
            name="flights",
            description="Flight operations and delays",
        )
        .with_dimensions(
            origin=lambda t: t.origin,
            destination=lambda t: t.destination,
            carrier=lambda t: t.carrier,
            flight_date={
                "expr": lambda t: t.flight_date,
                "description": "Flight departure date",
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )
        .with_measures(
            flight_count={
                "expr": lambda t: t.count(),
                "description": "Total number of flights",
            },
            avg_delay={
                "expr": lambda t: t.dep_delay.mean(),
                "description": "Average departure delay",
            },
        )
    )

    carriers_model = (
        to_semantic_table(
            carriers_tbl,
            name="carriers",
            description="Carrier lookup table",
        )
        .with_dimensions(code=lambda t: t.code, name=lambda t: t.name)
        .with_measures(carrier_count=lambda t: t.count())
    )

    return {"flights": flights_model, "carriers": carriers_model}


@pytest.fixture
def client(sample_models):
    """Create a FastAPI test client with in-memory models."""
    with TestClient(create_app(models=sample_models)) as test_client:
        yield test_client


def test_load_models_from_config_file(tmp_path):
    """The loader should import MODELS from an external semantic_config.py."""
    config_path = tmp_path / "semantic_config.py"
    config_path.write_text(
        """
import ibis
import pandas as pd

from boring_semantic_layer import to_semantic_table

tbl = ibis.memtable(pd.DataFrame({"carrier": ["AA", "UA"]}))
demo = (
    to_semantic_table(tbl, name="demo")
    .with_dimensions(carrier=lambda t: t.carrier)
    .with_measures(row_count=lambda t: t.count())
)

MODELS = {"demo": demo}
""".strip()
        + "\n"
    )

    models = load_models(config_path)

    assert list(models) == ["demo"]
    assert models["demo"].name == "demo"


def test_health(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_list_models(client):
    response = client.get("/models")

    assert response.status_code == 200
    assert response.json() == {
        "flights": "Flight operations and delays",
        "carriers": "Carrier lookup table",
    }


def test_get_model_schema(client):
    response = client.get("/models/flights/schema")

    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "flights"
    assert data["description"] == "Flight operations and delays"
    assert data["dimensions"]["flight_date"]["is_time_dimension"] is True
    assert data["measures"]["flight_count"]["description"] == "Total number of flights"


def test_get_time_range(client):
    response = client.get("/models/flights/time-range")

    assert response.status_code == 200
    data = response.json()
    assert data["start"].startswith("2024-01-01")
    assert data["end"].startswith("2024-01-30")


def test_search_dimension_values(client):
    response = client.get("/models/flights/dimensions/carrier/values", params={"limit": 2})

    assert response.status_code == 200
    data = response.json()
    assert data["total_distinct"] == 3
    assert len(data["values"]) == 2
    assert all("count" in item for item in data["values"])


def test_query_uses_core_bsl_interface(client):
    response = client.post(
        "/query",
        json={
            "model_name": "flights",
            "dimensions": ["flights.carrier"],
            "measures": ["flights.flight_count"],
            "filters": [{"field": "flights.carrier", "operator": "=", "value": "AA"}],
            "get_chart": False,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["columns"] == ["carrier", "flight_count"]
    assert data["records"] == [{"carrier": "AA", "flight_count": 10}]
    assert "chart" not in data


def test_query_supports_time_grain_and_time_range(client):
    response = client.post(
        "/query",
        json={
            "model_name": "flights",
            "dimensions": ["flight_date"],
            "measures": ["flight_count"],
            "time_grain": "TIME_GRAIN_MONTH",
            "time_range": {"start": "2024-01-01", "end": "2024-01-31"},
            "get_chart": False,
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["total_rows"] == 1
    assert data["records"][0]["flight_count"] == 30


def test_missing_model_returns_404(client):
    response = client.get("/models/missing/schema")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()
