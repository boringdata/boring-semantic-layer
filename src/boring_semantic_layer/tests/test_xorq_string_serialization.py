from __future__ import annotations

import json

import ibis
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture
def flights_data():
    con = ibis.duckdb.connect(":memory:")
    data = {
        "origin": ["JFK", "LAX", "SFO"],
        "destination": ["LAX", "JFK", "NYC"],
        "distance": [100, 200, 300],
    }
    return con.create_table("flights", data)


def test_dimension_serialization(flights_data):
    from boring_semantic_layer.xorq_convert import serialize_dimensions

    flights = to_semantic_table(flights_data, name="flights").with_dimensions(
        origin=lambda t: t.origin,
        destination=lambda t: t.destination,
    )

    op = flights.op()
    dims = op.get_dimensions()

    result = serialize_dimensions(dims)
    assert result
    dim_metadata = json.loads(result.unwrap())

    assert "origin" in dim_metadata
    assert dim_metadata["origin"]["expr"] == "_.origin"

    assert "destination" in dim_metadata
    assert dim_metadata["destination"]["expr"] == "_.destination"


def test_measure_serialization(flights_data):
    from boring_semantic_layer.xorq_convert import serialize_measures

    flights = to_semantic_table(flights_data, name="flights").with_measures(
        avg_distance=lambda t: t.distance.mean(),
        total_distance=lambda t: t.distance.sum(),
    )

    op = flights.op()
    measures = op.get_measures()

    result = serialize_measures(measures)
    assert result
    meas_metadata = json.loads(result.unwrap())

    assert "avg_distance" in meas_metadata
    assert meas_metadata["avg_distance"]["expr"] == "_.distance.mean()"

    assert "total_distance" in meas_metadata
    assert meas_metadata["total_distance"]["expr"] == "_.distance.sum()"


xorq = pytest.importorskip("xorq", reason="xorq not installed")


@pytest.mark.skipif(not xorq, reason="xorq not available")
def test_to_xorq_with_string_metadata(flights_data):
    from boring_semantic_layer.xorq_convert import to_xorq

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(
            origin=lambda t: t.origin,
            destination=lambda t: t.destination,
        )
        .with_measures(
            avg_distance=lambda t: t.distance.mean(),
            total_distance=lambda t: t.distance.sum(),
        )
    )

    xorq_expr = to_xorq(flights)

    op = xorq_expr.op()
    metadata = dict(op.metadata)

    dims = json.loads(metadata["dimensions"])
    assert dims["origin"]["expr"] == "_.origin"
    assert dims["destination"]["expr"] == "_.destination"

    meas = json.loads(metadata["measures"])
    assert meas["avg_distance"]["expr"] == "_.distance.mean()"
    assert meas["total_distance"]["expr"] == "_.distance.sum()"


@pytest.mark.skipif(not xorq, reason="xorq not available")
def test_from_xorq_deserialization(flights_data):
    from boring_semantic_layer.xorq_convert import from_xorq, to_xorq

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(avg_distance=lambda t: t.distance.mean())
    )

    xorq_expr = to_xorq(flights)
    reconstructed = from_xorq(xorq_expr)

    result = (
        reconstructed.group_by("origin")
        .aggregate("avg_distance")
        .execute()
    )

    assert len(result) > 0
    assert "origin" in result.columns
    assert "avg_distance" in result.columns
