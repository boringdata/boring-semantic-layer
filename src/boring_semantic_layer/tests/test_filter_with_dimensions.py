"""Tests for issue #98: with_dimensions() after filter()."""
import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture
def con():
    """Create an in-memory DuckDB connection."""
    return ibis.duckdb.connect(":memory:")


@pytest.fixture
def flights(con):
    """Create a sample flights table for testing."""
    df = pd.DataFrame(
        {
            "carrier": ["AA", "AA", "DL", "DL", "UA"],
            "origin": ["JFK", "LAX", "JFK", "ATL", "ORD"],
            "destination": ["LAX", "JFK", "ATL", "JFK", "SFO"],
            "arr_time": pd.to_datetime(
                [
                    "2023-01-15 10:30:00",
                    "2023-01-16 14:20:00",
                    "2023-02-10 09:15:00",
                    "2023-02-11 18:45:00",
                    "2023-03-05 12:00:00",
                ]
            ),
            "distance": [2475, 2475, 748, 748, 1846],
        }
    )
    return con.create_table("flights", df)


def test_filter_then_with_dimensions(flights):
    """Test that with_dimensions() works after filter()."""
    # Create semantic model
    semantic_flights = (
        to_semantic_table(flights, "flights")
        .with_dimensions(carrier=lambda t: t.carrier, origin=lambda t: t.origin)
        .with_measures(flight_count=lambda t: t.count(), avg_distance=lambda t: t.distance.mean())
    )

    # Apply filter then with_dimensions - this should not raise AttributeError
    result = (
        semantic_flights.filter(lambda t: t.carrier == "AA")
        .with_dimensions(arr_date=lambda t: t.arr_time.date())
        .group_by("arr_date")
        .aggregate("flight_count")
    )

    # Execute and verify
    df = result.execute()
    assert len(df) == 2  # Two different dates for AA flights
    assert "arr_date" in df.columns
    assert "flight_count" in df.columns


def test_filter_then_with_measures(flights):
    """Test that with_measures() also works after filter()."""
    semantic_flights = (
        to_semantic_table(flights, "flights")
        .with_dimensions(carrier=lambda t: t.carrier)
        .with_measures(flight_count=lambda t: t.count())
    )

    # Apply filter then with_measures
    result = (
        semantic_flights.filter(lambda t: t.carrier == "AA")
        .with_measures(total_distance=lambda t: t.distance.sum())
        .group_by("carrier")
        .aggregate("flight_count", "total_distance")
    )

    # Execute and verify
    df = result.execute()
    assert len(df) == 1  # Only AA carrier
    assert df["carrier"].iloc[0] == "AA"
    assert "total_distance" in df.columns


def test_filter_with_dimensions_preserves_existing(flights):
    """Test that with_dimensions() preserves existing dimensions after filter()."""
    semantic_flights = (
        to_semantic_table(flights, "flights")
        .with_dimensions(carrier=lambda t: t.carrier, origin=lambda t: t.origin)
        .with_measures(flight_count=lambda t: t.count())
    )

    # Filter then add a new dimension
    filtered = semantic_flights.filter(lambda t: t.carrier == "AA")
    with_new_dim = filtered.with_dimensions(arr_date=lambda t: t.arr_time.date())

    # Check that both old and new dimensions are available
    dims = with_new_dim.get_dimensions()
    assert "carrier" in dims
    assert "origin" in dims
    assert "arr_date" in dims


def test_multiple_filters_with_dimensions(flights):
    """Test chaining multiple operations including filter and with_dimensions."""
    semantic_flights = (
        to_semantic_table(flights, "flights")
        .with_dimensions(carrier=lambda t: t.carrier)
        .with_measures(flight_count=lambda t: t.count(), avg_distance=lambda t: t.distance.mean())
    )

    # Complex chaining
    result = (
        semantic_flights.filter(lambda t: t.distance > 1000)
        .with_dimensions(month=lambda t: t.arr_time.month())
        .filter(lambda t: t.carrier.isin(["AA", "UA"]))
        .group_by("carrier", "month")
        .aggregate("flight_count", "avg_distance")
    )

    # Execute and verify
    df = result.execute()
    assert len(df) > 0
    assert "carrier" in df.columns
    assert "month" in df.columns
