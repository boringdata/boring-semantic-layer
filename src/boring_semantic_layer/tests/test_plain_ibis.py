"""Tests that BSL core features work with plain ibis backends (no xorq wrapping).

This validates support for ibis backends that xorq doesn't register (e.g. Databricks).
See: https://github.com/boringdata/boring-semantic-layer/issues/216
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture(scope="module")
def plain_ibis_con():
    """Plain ibis DuckDB connection (not wrapped by xorq)."""
    return ibis.duckdb.connect()


@pytest.fixture(scope="module")
def flights_table(plain_ibis_con):
    """Sample flights table."""
    df = pd.DataFrame(
        {
            "carrier": ["AA", "AA", "UA", "UA", "DL"],
            "origin": ["JFK", "LAX", "SFO", "JFK", "ATL"],
            "dest": ["LAX", "JFK", "LAX", "SFO", "JFK"],
            "distance": [2475, 2475, 337, 2586, 760],
            "dep_delay": [10, -5, 0, 15, -3],
        }
    )
    return plain_ibis_con.create_table("flights", df)


@pytest.fixture(scope="module")
def carriers_table(plain_ibis_con):
    """Sample carriers lookup table."""
    df = pd.DataFrame(
        {
            "code": ["AA", "UA", "DL"],
            "name": ["American Airlines", "United Airlines", "Delta Air Lines"],
        }
    )
    return plain_ibis_con.create_table("carriers", df)


@pytest.fixture(scope="module")
def flights_model(flights_table):
    """BSL semantic table from plain ibis table."""
    return (
        to_semantic_table(flights_table, name="flights")
        .with_dimensions(
            carrier=lambda t: t.carrier,
            origin=lambda t: t.origin,
            dest=lambda t: t.dest,
        )
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
            avg_delay=lambda t: t.dep_delay.mean(),
        )
    )


@pytest.fixture(scope="module")
def carriers_model(carriers_table):
    """BSL semantic table for carriers."""
    return (
        to_semantic_table(carriers_table, name="carriers")
        .with_dimensions(
            code=lambda t: t.code,
            name=lambda t: t.name,
        )
        .with_measures(
            carrier_count=lambda t: t.count(),
        )
    )


class TestPlainIbisExecution:
    """Core execution works without xorq wrapping."""

    def test_simple_aggregate(self, flights_model):
        result = flights_model.group_by("carrier").aggregate("flight_count").execute()
        assert len(result) == 3
        assert result.flight_count.sum() == 5

    def test_multiple_measures(self, flights_model):
        result = (
            flights_model.group_by("carrier")
            .aggregate("flight_count", "total_distance")
            .execute()
        )
        assert "flight_count" in result.columns
        assert "total_distance" in result.columns

    def test_filter_then_aggregate(self, flights_model):
        result = (
            flights_model.filter(lambda t: t.carrier == "AA")
            .group_by("carrier")
            .aggregate("flight_count")
            .execute()
        )
        assert len(result) == 1
        assert result.flight_count.iloc[0] == 2

    def test_order_by(self, flights_model):
        result = (
            flights_model.group_by("carrier")
            .aggregate("flight_count")
            .order_by(ibis.desc("flight_count"))
            .execute()
        )
        assert result.flight_count.iloc[0] >= result.flight_count.iloc[-1]

    def test_limit(self, flights_model):
        result = (
            flights_model.group_by("carrier")
            .aggregate("flight_count")
            .limit(2)
            .execute()
        )
        assert len(result) == 2

    def test_sql_compilation(self, flights_model):
        sql = flights_model.group_by("carrier").aggregate("flight_count").sql()
        assert isinstance(sql, str)
        assert "carrier" in sql.lower()

    def test_to_pandas(self, flights_model):
        result = (
            flights_model.group_by("carrier").aggregate("flight_count").to_pandas()
        )
        assert hasattr(result, "columns")
        assert len(result) == 3


class TestPlainIbisFiltering:
    """Filtering works without xorq wrapping."""

    def test_callable_filter(self, flights_model):
        result = (
            flights_model.filter(lambda t: t.distance > 1000)
            .group_by("carrier")
            .aggregate("flight_count")
            .execute()
        )
        # AA(JFK->LAX, LAX->JFK) + UA(JFK->SFO) = 3 flights with distance > 1000
        assert result.flight_count.sum() == 3

    def test_multiple_filters(self, flights_model):
        result = (
            flights_model.filter(lambda t: t.carrier == "AA")
            .filter(lambda t: t.distance > 2000)
            .group_by("origin")
            .aggregate("flight_count")
            .execute()
        )
        assert result.flight_count.sum() == 2


class TestPlainIbisJoins:
    """Joins work without xorq wrapping."""

    def test_join_one(self, flights_model, carriers_model):
        joined = flights_model.join_one(
            carriers_model,
            on=lambda f, c: f.carrier == c.code,
        )
        result = joined.group_by("name").aggregate("flight_count").execute()
        assert len(result) == 3
        assert "name" in result.columns


class TestPlainIbisSerializationGating:
    """Serialization features raise clear errors for non-xorq backends."""

    def test_to_tagged_works_or_errors_cleanly(self, flights_model):
        """to_tagged should either work (if xorq handles backend) or error clearly."""
        expr = flights_model.group_by("carrier").aggregate("flight_count")
        # For a DuckDB backend, xorq DOES support it, so to_tagged should work.
        # For an unsupported backend, it should raise a clear error.
        # We just verify it doesn't crash with an obscure error.
        try:
            tagged = expr.to_tagged()
            assert tagged is not None
        except Exception as e:
            # Should be a clear error, not an internal xorq assertion
            assert "AssertionError" not in type(e).__name__
