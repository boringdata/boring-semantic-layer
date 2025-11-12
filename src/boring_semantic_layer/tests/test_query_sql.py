"""Tests for the .query(...) method on semantic tables."""

import ibis
import pytest

from boring_semantic_layer import to_semantic_table


class TestQueryMethod:
    """Test the .query() method for executing arbitrary SQL."""

    @pytest.fixture
    def duckdb_backend(self):
        """Create a DuckDB backend for testing."""
        return ibis.duckdb.connect()

    @pytest.fixture
    def flights_table(self, duckdb_backend):
        """Create a flights table in DuckDB backend."""
        data = {
            "flight_id": [1, 2, 3, 4, 5],
            "carrier": ["AA", "UA", "AA", "DL", "UA"],
            "origin": ["JFK", "LAX", "ORD", "JFK", "SFO"],
            "dest": ["LAX", "JFK", "LAX", "ORD", "JFK"],
            "distance": [2475, 2475, 1745, 740, 2565],
            "delay": [10, -5, 20, 0, 15],
        }
        # Create table in backend
        duckdb_backend.create_table("flights", data, overwrite=True)
        return duckdb_backend.table("flights")

    @pytest.fixture
    def flights_semantic(self, flights_table):
        """Create a semantic table from flights."""
        return (
            to_semantic_table(flights_table, name="flights")
            .with_dimensions(
                carrier=lambda t: t.carrier,
                origin=lambda t: t.origin,
                dest=lambda t: t.dest,
            )
            .with_measures(
                total_distance=lambda t: t.distance.sum(),
                avg_delay=lambda t: t.delay.mean(),
                flight_count=lambda t: t.count(),
            )
        )

    def test_simple_select(self, flights_semantic):
        """Test simple SELECT query."""
        result = flights_semantic.query(sql="SELECT * FROM flights WHERE delay > 0")
        df = result.execute()

        assert len(df) == 3
        assert all(df["delay"] > 0)

    def test_aggregation(self, flights_semantic):
        """Test GROUP BY aggregation."""
        result = flights_semantic.query(
            "SELECT carrier, AVG(delay) as avg_delay, COUNT(*) as count FROM flights GROUP BY carrier"
        )
        df = result.execute()

        assert len(df) == 3
        assert "avg_delay" in df.columns
        assert "count" in df.columns

    def test_where_clause(self, flights_semantic):
        """Test WHERE clause filtering."""
        result = flights_semantic.query(sql="SELECT * FROM flights WHERE distance > 2000")
        df = result.execute()

        assert all(df["distance"] > 2000)

    def test_order_by(self, flights_semantic):
        """Test ORDER BY clause."""
        result = flights_semantic.query(sql="SELECT * FROM flights ORDER BY delay DESC")
        df = result.execute()

        # Check that delays are in descending order
        delays = df["delay"].tolist()
        assert delays == sorted(delays, reverse=True)

    def test_window_function(self, flights_semantic):
        """Test window functions."""
        result = flights_semantic.query(
            sql="""
            SELECT
                carrier,
                delay,
                AVG(delay) OVER (PARTITION BY carrier) as carrier_avg_delay
            FROM flights
            """
        )
        df = result.execute()

        assert "carrier_avg_delay" in df.columns
        assert len(df) == 5

    def test_subquery(self, flights_semantic):
        """Test subqueries."""
        result = flights_semantic.query(
            sql="""
            SELECT * FROM flights
            WHERE distance > (SELECT AVG(distance) FROM flights)
            """
        )
        df = result.execute()

        assert len(df) > 0

    def test_cte(self, flights_semantic):
        """Test Common Table Expressions (CTEs)."""
        result = flights_semantic.query(
            sql="""
            WITH long_flights AS (
                SELECT * FROM flights WHERE distance > 2000
            )
            SELECT carrier, COUNT(*) as count
            FROM long_flights
            GROUP BY carrier
            """
        )
        df = result.execute()

        assert len(df) > 0

    def test_chain_with_ibis(self, flights_semantic):
        """Test chaining SQL result with Ibis operations."""
        result = flights_semantic.query(sql="SELECT * FROM flights WHERE distance > 1000")

        # Chain with Ibis operations
        filtered = result.filter(result.delay > 0)
        ordered = filtered.order_by(filtered.delay.desc())

        df = ordered.execute()
        assert len(df) > 0
        assert all(df["delay"] > 0)

    def test_custom_table_name(self, flights_semantic):
        """Test using custom table name in query."""
        # Note: table_name parameter is not supported in .query(sql=...)
        # You must reference tables by their actual database names
        result = flights_semantic.query(sql="SELECT * FROM flights WHERE delay > 5")
        df = result.execute()

        assert len(df) > 0

    def test_in_memory_table_raises_error(self):
        """Test that in-memory tables raise ValueError."""
        # Create an in-memory table (not connected to backend)
        in_memory_table = ibis.memtable({"id": [1, 2, 3], "value": [10, 20, 30]})

        semantic = to_semantic_table(in_memory_table, name="mem_table")

        with pytest.raises(ValueError, match="Cannot execute SQL on in-memory tables"):
            semantic.query(sql="SELECT * FROM mem_table")

    def test_complex_join_via_backend(self, duckdb_backend, flights_semantic):
        """Test complex multi-table queries using backend.sql()."""
        # Create another table
        carriers_data = {
            "carrier_code": ["AA", "UA", "DL"],
            "carrier_name": ["American Airlines", "United Airlines", "Delta Air Lines"],
        }
        duckdb_backend.create_table("carriers", carriers_data, overwrite=True)

        # Use backend.sql() for multi-table queries
        result = duckdb_backend.sql("""
            SELECT f.*, c.carrier_name
            FROM flights f
            LEFT JOIN carriers c ON f.carrier = c.carrier_code
            WHERE f.delay > 0
        """)

        df = result.execute()
        assert len(df) > 0
        assert "carrier_name" in df.columns

    def test_sql_injection_safety(self, flights_semantic):
        """Test that SQL is executed as-is (user must handle injection safety)."""
        # This test documents that the method doesn't provide injection protection
        # Users must use parameterized queries or sanitize inputs themselves

        # This would be unsafe in production, but should work syntactically
        result = flights_semantic.query(sql="SELECT * FROM flights WHERE carrier = 'AA'")
        df = result.execute()

        assert all(df["carrier"] == "AA")

    def test_empty_result(self, flights_semantic):
        """Test query that returns no results."""
        result = flights_semantic.query(sql="SELECT * FROM flights WHERE delay > 1000")
        df = result.execute()

        assert len(df) == 0

    def test_multiple_queries_on_same_table(self, flights_semantic):
        """Test executing multiple queries on the same semantic table."""
        result1 = flights_semantic.query(sql="SELECT COUNT(*) as total FROM flights")
        result2 = flights_semantic.query(sql="SELECT MAX(delay) as max_delay FROM flights")

        df1 = result1.execute()
        df2 = result2.execute()

        assert df1["total"].iloc[0] == 5
        assert df2["max_delay"].iloc[0] == 20

    def test_query_on_original_after_filter(self, flights_semantic):
        """Test that you can still query the original semantic table after filtering."""
        # Apply semantic operations first
        filtered = flights_semantic.filter(lambda t: t.delay > 0)

        # filtered is a SemanticFilter, which doesn't have .query() method
        # But you can still query the original semantic table
        result = flights_semantic.query(sql="SELECT * FROM flights WHERE delay > 10")
        df = result.execute()

        # The SQL queries the original table directly
        assert len(df) > 0


class TestQueryMethodEdgeCases:
    """Test edge cases and error handling."""

    def test_invalid_sql_syntax(self):
        """Test that invalid SQL raises appropriate error."""
        con = ibis.duckdb.connect()
        con.create_table("test", {"id": [1, 2, 3]}, overwrite=True)
        table = con.table("test")
        semantic = to_semantic_table(table, name="test")

        with pytest.raises(Exception):  # Will raise backend-specific SQL error
            semantic.query(sql="SELECT * FORM test")  # Typo: FORM instead of FROM

    def test_missing_table_reference(self):
        """Test query that references non-existent table."""
        con = ibis.duckdb.connect()
        con.create_table("test", {"id": [1, 2, 3]}, overwrite=True)
        table = con.table("test")
        semantic = to_semantic_table(table, name="test")

        with pytest.raises(Exception):  # Will raise backend-specific error
            semantic.query(sql="SELECT * FROM nonexistent_table")

    def test_query_preserves_backend_connection(self):
        """Test that multiple queries use the same backend connection."""
        con = ibis.duckdb.connect()
        con.create_table("test", {"id": [1, 2, 3]}, overwrite=True)
        table = con.table("test")
        semantic = to_semantic_table(table, name="test")

        result1 = semantic.query(sql="SELECT * FROM test")
        result2 = semantic.query(sql="SELECT COUNT(*) as cnt FROM test")

        # Both should work without issues
        assert len(result1.execute()) == 3
        assert result2.execute()["cnt"].iloc[0] == 3
