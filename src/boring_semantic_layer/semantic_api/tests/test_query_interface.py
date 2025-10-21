"""
Tests for the query() interface on SemanticTable.

Tests the parameter-based query interface as an alternative to method chaining.
"""

import pandas as pd
import ibis
import pytest

from boring_semantic_layer.semantic_api import to_semantic_table


@pytest.fixture(scope="module")
def con():
    """DuckDB connection for all tests."""
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def flights_table(con):
    """Create a simple flights semantic table for testing."""
    flights_df = pd.DataFrame(
        {
            "origin": ["JFK", "JFK", "LAX", "LAX", "ORD", "ORD", "SFO", "SFO"],
            "destination": ["LAX", "SFO", "JFK", "ORD", "LAX", "SFO", "JFK", "ORD"],
            "distance": [2475, 2586, 2475, 1744, 1744, 1846, 2586, 1846],
            "dep_delay": [5, 10, -3, 0, 15, 20, 8, 12],
            "carrier": ["AA", "UA", "AA", "DL", "UA", "AA", "DL", "UA"],
        }
    )

    flights_tbl = con.create_table("flights", flights_df)

    return (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            origin=lambda t: t.origin,
            destination=lambda t: t.destination,
            carrier=lambda t: t.carrier,
        )
        .with_measures(
            flight_count=lambda t: t.count(),
            avg_distance=lambda t: t.distance.mean(),
            avg_delay=lambda t: t.dep_delay.mean(),
            total_delay=lambda t: t.dep_delay.sum(),
        )
    )


class TestQueryInterface:
    """Test the query() method on SemanticTable."""

    def test_basic_query(self, flights_table):
        """Test basic query with dimensions and measures."""
        result = flights_table.query(
            dimensions=["origin"], measures=["flight_count", "avg_distance"]
        )

        df = result.execute()

        assert set(df.columns) == {"origin", "flight_count", "avg_distance"}
        assert len(df) == 4
        assert all(df["flight_count"] == 2)

    def test_query_with_json_filter_simple(self, flights_table):
        """Test query with simple JSON filter."""
        result = flights_table.query(
            dimensions=["carrier"],
            measures=["flight_count"],
            filters=[{"field": "distance", "operator": ">", "value": 2000}],
        )

        df = result.execute()

        # Should only include long-distance flights (4 flights with distance > 2000)
        assert df["flight_count"].sum() == 4

    def test_query_with_json_filter_in_operator(self, flights_table):
        """Test query with JSON filter using 'in' operator."""
        result = flights_table.query(
            dimensions=["carrier"],
            measures=["flight_count"],
            filters=[{"field": "carrier", "operator": "in", "values": ["AA", "UA"]}],
        )

        df = result.execute()

        # Should only include AA and UA carriers
        assert set(df["carrier"]) == {"AA", "UA"}
        assert df["flight_count"].sum() == 6  # 3 AA + 3 UA flights

    def test_query_with_json_filter_compound_and(self, flights_table):
        """Test query with compound AND filter."""
        result = flights_table.query(
            dimensions=["carrier"],
            measures=["flight_count"],
            filters=[
                {
                    "operator": "AND",
                    "conditions": [
                        {"field": "distance", "operator": ">", "value": 2000},
                        {"field": "carrier", "operator": "=", "value": "UA"},
                    ],
                }
            ],
        )

        df = result.execute()

        # Should only include UA flights with distance > 2000
        assert len(df) == 1
        assert df["carrier"].iloc[0] == "UA"

    def test_query_with_json_filter_equals_operators(self, flights_table):
        """Test query with different equality operators."""
        # Test 'eq' operator
        result1 = flights_table.query(
            measures=["flight_count"],
            filters=[{"field": "carrier", "operator": "eq", "value": "AA"}],
        )
        assert result1.execute()["flight_count"].iloc[0] == 3

        # Test '=' operator
        result2 = flights_table.query(
            measures=["flight_count"],
            filters=[{"field": "carrier", "operator": "=", "value": "AA"}],
        )
        assert result2.execute()["flight_count"].iloc[0] == 3

        # Test 'equals' operator
        result3 = flights_table.query(
            measures=["flight_count"],
            filters=[{"field": "carrier", "operator": "equals", "value": "AA"}],
        )
        assert result3.execute()["flight_count"].iloc[0] == 3

    def test_query_with_callable_filter(self, flights_table):
        """Test query with callable filter (lambda function)."""
        result = flights_table.query(
            dimensions=["carrier"],
            measures=["flight_count"],
            filters=[lambda t: t.distance > 2000],
        )

        df = result.execute()

        # Should only include long-distance flights (4 flights with distance > 2000)
        assert df["flight_count"].sum() == 4

    def test_query_with_mixed_filter_types(self, flights_table):
        """Test query with both JSON and callable filters."""
        result = flights_table.query(
            dimensions=["carrier"],
            measures=["flight_count"],
            filters=[
                {"field": "carrier", "operator": "in", "values": ["AA", "UA"]},
                lambda t: t.distance > 2000,
            ],
        )

        df = result.execute()

        # Should only include AA and UA flights with distance > 2000
        # AA has 2 long flights, UA has 1 long flight = 3 total
        assert df["flight_count"].sum() == 3
        assert set(df["carrier"]) == {"AA", "UA"}

    def test_query_with_ordering_and_limit(self, flights_table):
        """Test query with ordering and limit."""
        result = flights_table.query(
            dimensions=["origin"],
            measures=["avg_distance"],
            order_by=[("avg_distance", "desc")],
            limit=2,
        )

        df = result.execute()

        assert len(df) == 2
        assert df["avg_distance"].iloc[0] >= df["avg_distance"].iloc[1]

    def test_query_with_filters(self, flights_table):
        """Test query with filters."""
        result = flights_table.query(
            dimensions=["carrier"],
            measures=["flight_count"],
            filters=[lambda t: t.distance > 2000],
        )

        df = result.execute()

        # Should only include long-distance flights (4 flights with distance > 2000)
        assert df["flight_count"].sum() == 4

    def test_query_measures_only(self, flights_table):
        """Test query with only measures (no dimensions)."""
        result = flights_table.query(measures=["flight_count", "avg_distance"])

        df = result.execute()

        assert len(df) == 1
        assert df["flight_count"].iloc[0] == 8

    def test_query_comparison_with_method_chaining(self, flights_table):
        """Test that query() produces same results as method chaining."""
        # Using query()
        df_query = (
            flights_table.query(
                dimensions=["origin"],
                measures=["flight_count", "avg_distance"],
                order_by=[("origin", "asc")],
            )
            .execute()
            .sort_values("origin")
            .reset_index(drop=True)
        )

        # Using method chaining
        df_chain = (
            flights_table.group_by("origin")
            .aggregate("flight_count", "avg_distance")
            .order_by("origin")
            .execute()
            .sort_values("origin")
            .reset_index(drop=True)
        )

        pd.testing.assert_frame_equal(df_query, df_chain)

    def test_query_returns_semantic_table(self, flights_table):
        """Test that query() returns a SemanticTable for further chaining."""
        from boring_semantic_layer.semantic_api.table import SemanticTable

        result = flights_table.query(dimensions=["origin"], measures=["flight_count"])

        assert isinstance(result, SemanticTable)

        # Should be able to chain further operations
        df = result.order_by(ibis.desc("flight_count")).limit(2).execute()
        assert len(df) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
