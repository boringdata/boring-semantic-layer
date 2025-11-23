from __future__ import annotations

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
    dim_metadata = result.unwrap()

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
    meas_metadata = result.unwrap()

    assert "avg_distance" in meas_metadata
    assert meas_metadata["avg_distance"]["expr"] == "_.distance.mean()"

    assert "total_distance" in meas_metadata
    assert meas_metadata["total_distance"]["expr"] == "_.distance.sum()"


xorq = pytest.importorskip("xorq", reason="xorq not installed")


@pytest.mark.skipif(not xorq, reason="xorq not available")
def test_to_tagged_with_string_metadata(flights_data):
    from boring_semantic_layer.xorq_convert import to_tagged

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

    tagged_expr = to_tagged(flights)

    op = tagged_expr.op()
    metadata = dict(op.metadata)

    # metadata is stored as nested tuples, convert to dict
    dims = dict(metadata["dimensions"])
    # each dimension value is also a tuple of key-value pairs
    origin_dim = dict(dims["origin"])
    assert origin_dim["expr"] == "_.origin"

    destination_dim = dict(dims["destination"])
    assert destination_dim["expr"] == "_.destination"

    # measures are also stored as nested tuples
    meas = dict(metadata["measures"])
    avg_distance_meas = dict(meas["avg_distance"])
    assert avg_distance_meas["expr"] == "_.distance.mean()"

    total_distance_meas = dict(meas["total_distance"])
    assert total_distance_meas["expr"] == "_.distance.sum()"


@pytest.mark.skipif(not xorq, reason="xorq not available")
def test_from_tagged_deserialization(flights_data):
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(avg_distance=lambda t: t.distance.mean())
    )

    tagged_expr = to_tagged(flights)
    reconstructed = from_tagged(tagged_expr)

    result = reconstructed.group_by("origin").aggregate("avg_distance").execute()

    assert len(result) > 0
    assert "origin" in result.columns
    assert "avg_distance" in result.columns


def test_serialize_entity_dimensions(flights_data):
    from boring_semantic_layer import entity_dimension
    from boring_semantic_layer.xorq_convert import serialize_dimensions

    flights = to_semantic_table(flights_data, name="flights").with_dimensions(
        origin=entity_dimension(lambda t: t.origin, "Origin airport"),
        destination=lambda t: t.destination,
    )

    op = flights.op()
    dims = op.get_dimensions()

    result = serialize_dimensions(dims)
    assert result
    dim_metadata = result.unwrap()

    # Entity dimension should have is_entity flag
    assert "origin" in dim_metadata
    assert dim_metadata["origin"]["is_entity"] is True
    assert dim_metadata["origin"]["description"] == "Origin airport"
    assert dim_metadata["origin"]["expr"] == "_.origin"

    # Regular dimension should not have is_entity flag
    assert "destination" in dim_metadata
    assert dim_metadata["destination"]["is_entity"] is False


def test_serialize_event_timestamp_dimensions(flights_data):
    from boring_semantic_layer import time_dimension
    from boring_semantic_layer.xorq_convert import serialize_dimensions

    con = ibis.duckdb.connect(":memory:")
    data = {
        "origin": ["JFK", "LAX"],
        "arr_time": ["2024-01-01", "2024-01-02"],
        "distance": [100, 200],
    }
    tbl = con.create_table("flights", data)

    flights = to_semantic_table(tbl, name="flights").with_dimensions(
        arr_time=time_dimension(
            lambda t: t.arr_time,
            "Arrival time",
            smallest_time_grain="TIME_GRAIN_DAY"
        ),
        origin=lambda t: t.origin,
    )

    op = flights.op()
    dims = op.get_dimensions()

    result = serialize_dimensions(dims)
    assert result
    dim_metadata = result.unwrap()

    # Event timestamp dimension should have flags
    assert "arr_time" in dim_metadata
    assert dim_metadata["arr_time"]["is_event_timestamp"] is True
    assert dim_metadata["arr_time"]["is_time_dimension"] is True
    assert dim_metadata["arr_time"]["smallest_time_grain"] == "TIME_GRAIN_DAY"
    assert dim_metadata["arr_time"]["description"] == "Arrival time"

    # Regular dimension should not have flags
    assert "origin" in dim_metadata
    assert dim_metadata["origin"]["is_event_timestamp"] is False
    assert dim_metadata["origin"]["is_time_dimension"] is False


@pytest.mark.skipif(not xorq, reason="xorq not available")
def test_entity_dimension_roundtrip(flights_data):
    from boring_semantic_layer import entity_dimension
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(
            origin=entity_dimension(lambda t: t.origin, "Origin airport"),
            destination=lambda t: t.destination,
        )
        .with_measures(avg_distance=lambda t: t.distance.mean())
    )

    # Serialize and deserialize
    tagged_expr = to_tagged(flights)
    reconstructed = from_tagged(tagged_expr)

    # Verify entity dimension metadata is preserved
    dims = reconstructed.get_dimensions()
    assert "origin" in dims
    assert dims["origin"].is_entity is True
    assert dims["origin"].description == "Origin airport"

    assert "destination" in dims
    assert dims["destination"].is_entity is False

    # Verify it still works
    result = reconstructed.group_by("origin").aggregate("avg_distance").execute()
    assert len(result) > 0


@pytest.mark.skipif(not xorq, reason="xorq not available")
def test_event_timestamp_roundtrip(flights_data):
    from boring_semantic_layer import time_dimension
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    data = {
        "origin": ["JFK", "LAX", "SFO"],
        "arr_time": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "distance": [100, 200, 300],
    }
    tbl = con.create_table("flights", data)

    flights = (
        to_semantic_table(tbl, name="flights")
        .with_dimensions(
            arr_time=time_dimension(
                lambda t: t.arr_time,
                "Arrival time",
                smallest_time_grain="TIME_GRAIN_DAY"
            ),
            origin=lambda t: t.origin,
        )
        .with_measures(total_distance=lambda t: t.distance.sum())
    )

    # Serialize and deserialize
    tagged_expr = to_tagged(flights)
    reconstructed = from_tagged(tagged_expr)

    # Verify event timestamp metadata is preserved
    dims = reconstructed.get_dimensions()
    assert "arr_time" in dims
    assert dims["arr_time"].is_event_timestamp is True
    assert dims["arr_time"].is_time_dimension is True
    assert dims["arr_time"].smallest_time_grain == "TIME_GRAIN_DAY"
    assert dims["arr_time"].description == "Arrival time"

    assert "origin" in dims
    assert dims["origin"].is_event_timestamp is False

    # Verify it still works
    result = reconstructed.group_by("arr_time").aggregate("total_distance").execute()
    assert len(result) > 0


@pytest.mark.skipif(not xorq, reason="xorq not available")
def test_entity_and_event_timestamp_roundtrip(flights_data):
    from boring_semantic_layer import entity_dimension, time_dimension
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    data = {
        "business_id": [1, 2, 3],
        "statement_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "balance": [1000, 2000, 3000],
    }
    tbl = con.create_table("balance", data)

    balance = (
        to_semantic_table(tbl, name="balance_features")
        .with_dimensions(
            business_id=entity_dimension(lambda t: t.business_id, "Business identifier"),
            statement_date=time_dimension(
                lambda t: t.statement_date,
                "Statement date",
                smallest_time_grain="TIME_GRAIN_DAY"
            ),
        )
        .with_measures(total_balance=lambda t: t.balance.sum())
    )

    # Serialize and deserialize
    tagged_expr = to_tagged(balance)
    reconstructed = from_tagged(tagged_expr)

    # Verify entity dimension
    dims = reconstructed.get_dimensions()
    assert dims["business_id"].is_entity is True
    assert dims["business_id"].description == "Business identifier"

    # Verify event timestamp
    assert dims["statement_date"].is_event_timestamp is True
    assert dims["statement_date"].is_time_dimension is True
    assert dims["statement_date"].smallest_time_grain == "TIME_GRAIN_DAY"

    # Verify json_definition contains both
    json_def = reconstructed.json_definition
    assert "entity_dimensions" in json_def
    assert "business_id" in json_def["entity_dimensions"]
    assert "event_timestamp" in json_def
    assert "statement_date" in json_def["event_timestamp"]

    # Verify it still works
    result = reconstructed.group_by("business_id", "statement_date").aggregate("total_balance").execute()
    assert len(result) > 0
