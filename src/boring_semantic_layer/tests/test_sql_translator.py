"""Tests for SQL translator."""

import pytest

pytest.importorskip("sqlglot", reason="sqlglot not installed")

import ibis

from boring_semantic_layer import to_semantic_table


class TestSemanticSQLBasic:
    """Basic SQL translation tests."""

    @pytest.fixture
    def flights_model(self):
        """Create flights semantic model."""
        con = ibis.duckdb.connect()
        con.create_table(
            "flights",
            {
                "flight_id": [1, 2, 3, 4, 5, 6],
                "carrier": ["AA", "UA", "AA", "DL", "UA", "AA"],
                "distance": [2475, 2475, 1745, 740, 2565, 3000],
                "delay": [10, -5, 20, 0, 15, 25],
            },
            overwrite=True,
        )

        return (
            to_semantic_table(con.table("flights"), name="flights")
            .with_dimensions(
                airline=lambda t: t.carrier,
                flight_distance=lambda t: t.distance,
            )
            .with_measures(
                total_distance=lambda t: t.distance.sum(),
                avg_delay=lambda t: t.delay.mean(),
                flight_count=lambda t: t.count(),
            )
        )

    def test_simple_group_by_aggregate(self, flights_model):
        """Test simple GROUP BY with aggregate."""
        result = flights_model.query(
            sql="""
            SELECT airline, total_distance
            FROM flights
            GROUP BY airline
        """
        )

        df = result.execute()
        assert len(df) == 3
        assert "airline" in df.columns
        assert "total_distance" in df.columns

    def test_multiple_measures(self, flights_model):
        """Test multiple measures in SELECT."""
        result = flights_model.query(
            sql="""
            SELECT airline, total_distance, avg_delay, flight_count
            FROM flights
            GROUP BY airline
        """
        )

        df = result.execute()
        assert len(df) == 3
        assert set(df.columns) == {"airline", "total_distance", "avg_delay", "flight_count"}

    def test_where_filter(self, flights_model):
        """Test WHERE clause."""
        result = flights_model.query(
            sql="""
            SELECT airline, total_distance
            FROM flights
            WHERE flight_distance > 2000
            GROUP BY airline
        """
        )

        df = result.execute()
        assert len(df) == 2  # Only AA and UA have flights > 2000 miles

    def test_order_by_asc(self, flights_model):
        """Test ORDER BY ascending."""
        result = flights_model.query(
            sql="""
            SELECT airline, total_distance
            FROM flights
            GROUP BY airline
            ORDER BY total_distance ASC
        """
        )

        df = result.execute()
        distances = df["total_distance"].tolist()
        assert distances == sorted(distances)

    def test_order_by_desc(self, flights_model):
        """Test ORDER BY descending."""
        result = flights_model.query(
            sql="""
            SELECT airline, total_distance
            FROM flights
            GROUP BY airline
            ORDER BY total_distance DESC
        """
        )

        df = result.execute()
        distances = df["total_distance"].tolist()
        assert distances == sorted(distances, reverse=True)

    def test_limit(self, flights_model):
        """Test LIMIT clause."""
        result = flights_model.query(
            sql="""
            SELECT airline, flight_count
            FROM flights
            GROUP BY airline
            ORDER BY flight_count DESC
            LIMIT 2
        """
        )

        df = result.execute()
        assert len(df) == 2

    def test_combined_features(self, flights_model):
        """Test WHERE + GROUP BY + ORDER BY + LIMIT."""
        result = flights_model.query(
            sql="""
            SELECT airline, total_distance, flight_count
            FROM flights
            WHERE flight_distance > 1000
            GROUP BY airline
            ORDER BY total_distance DESC
            LIMIT 2
        """
        )

        df = result.execute()
        assert len(df) == 2
        assert df["total_distance"].iloc[0] > df["total_distance"].iloc[1]


class TestSemanticSQLFilters:
    """Test WHERE clause operators."""

    @pytest.fixture
    def flights_model(self):
        """Create flights semantic model."""
        con = ibis.duckdb.connect()
        con.create_table(
            "flights",
            {
                "flight_id": [1, 2, 3, 4, 5],
                "carrier": ["AA", "UA", "AA", "DL", "UA"],
                "distance": [2475, 2475, 1745, 740, 2565],
                "delay": [10, -5, 20, 0, 15],
            },
            overwrite=True,
        )

        return (
            to_semantic_table(con.table("flights"), name="flights")
            .with_dimensions(
                airline=lambda t: t.carrier,
                flight_distance=lambda t: t.distance,
            )
            .with_measures(
                flight_count=lambda t: t.count(),
            )
        )

    def test_where_greater_than(self, flights_model):
        """Test WHERE column > value."""
        result = flights_model.query(
            sql="""
            SELECT airline, flight_count
            FROM flights
            WHERE flight_distance > 2000
            GROUP BY airline
        """
        )

        df = result.execute()
        # Only flights with distance > 2000
        assert len(df) > 0

    def test_where_less_than(self, flights_model):
        """Test WHERE column < value."""
        result = flights_model.query(
            sql="""
            SELECT airline, flight_count
            FROM flights
            WHERE flight_distance < 1000
            GROUP BY airline
        """
        )

        df = result.execute()
        assert len(df) == 1  # Only DL (740 miles)

    def test_where_equals(self, flights_model):
        """Test WHERE column = value."""
        result = flights_model.query(
            sql="""
            SELECT airline, flight_count
            FROM flights
            WHERE flight_distance = 2475
            GROUP BY airline
        """
        )

        df = result.execute()
        assert len(df) == 2  # AA and UA both have 2475 mile flights

    def test_where_not_equals(self, flights_model):
        """Test WHERE column != value."""
        result = flights_model.query(
            sql="""
            SELECT airline, flight_count
            FROM flights
            WHERE flight_distance != 2475
            GROUP BY airline
        """
        )

        df = result.execute()
        assert len(df) == 3  # All airlines have flights != 2475


class TestSemanticSQLErrors:
    """Test error handling."""

    @pytest.fixture
    def flights_model(self):
        """Create flights semantic model."""
        con = ibis.duckdb.connect()
        con.create_table(
            "flights",
            {
                "carrier": ["AA", "UA"],
                "distance": [2475, 2475],
            },
            overwrite=True,
        )

        return (
            to_semantic_table(con.table("flights"), name="flights")
            .with_dimensions(
                airline=lambda t: t.carrier,
            )
            .with_measures(
                total_distance=lambda t: t.distance.sum(),
            )
        )

    def test_unknown_column(self, flights_model):
        """Test error on unknown column."""
        with pytest.raises(ValueError, match="Unknown"):
            flights_model.query(
                sql="""
                SELECT unknown_column, total_distance
                FROM flights
                GROUP BY unknown_column
            """
            )


class TestSemanticSQLCanTranslate:
    """Test can_translate() method."""

    @pytest.fixture
    def flights_model(self):
        """Create flights semantic model."""
        con = ibis.duckdb.connect()
        con.create_table(
            "flights",
            {
                "carrier": ["AA"],
                "distance": [2475],
            },
            overwrite=True,
        )

        return (
            to_semantic_table(con.table("flights"), name="flights")
            .with_dimensions(
                airline=lambda t: t.carrier,
            )
            .with_measures(
                total_distance=lambda t: t.distance.sum(),
            )
        )

    def test_can_translate_simple_query(self, flights_model):
        """Test can translate simple semantic query."""
        from boring_semantic_layer.sql_translator import SemanticSQLTranslator

        translator = SemanticSQLTranslator(flights_model)

        assert (
            translator.can_translate("""
            SELECT airline, total_distance
            FROM flights
            GROUP BY airline
        """)
            is True
        )

    def test_cannot_translate_unknown_column(self, flights_model):
        """Test cannot translate with unknown columns."""
        from boring_semantic_layer.sql_translator import SemanticSQLTranslator

        translator = SemanticSQLTranslator(flights_model)

        assert (
            translator.can_translate("""
            SELECT carrier, distance
            FROM flights
        """)
            is False
        )

    def test_cannot_translate_join(self, flights_model):
        """Test cannot translate queries with JOINs."""
        from boring_semantic_layer.sql_translator import SemanticSQLTranslator

        translator = SemanticSQLTranslator(flights_model)

        assert (
            translator.can_translate("""
            SELECT a.airline
            FROM flights a
            JOIN other_table b ON a.id = b.id
        """)
            is False
        )

    def test_cannot_translate_subquery(self, flights_model):
        """Test cannot translate queries with subqueries."""
        from boring_semantic_layer.sql_translator import SemanticSQLTranslator

        translator = SemanticSQLTranslator(flights_model)

        assert (
            translator.can_translate("""
            SELECT airline
            FROM (SELECT * FROM flights)
        """)
            is False
        )


class TestSemanticSQLIntegration:
    """Integration tests with real queries."""

    def test_end_to_end_workflow(self):
        """Test complete workflow from SQL to results."""
        con = ibis.duckdb.connect()
        con.create_table(
            "flights",
            {
                "flight_id": list(range(1, 11)),
                "carrier": ["AA", "UA", "AA", "DL", "UA", "AA", "DL", "UA", "AA", "DL"],
                "distance": [2475, 2475, 1745, 740, 2565, 3000, 800, 2400, 1800, 750],
                "delay": [10, -5, 20, 0, 15, 25, -3, 5, 12, -1],
            },
            overwrite=True,
        )

        flights = (
            to_semantic_table(con.table("flights"), name="flights")
            .with_dimensions(
                airline=lambda t: t.carrier,
                flight_distance=lambda t: t.distance,
            )
            .with_measures(
                total_distance=lambda t: t.distance.sum(),
                avg_delay=lambda t: t.delay.mean(),
                flight_count=lambda t: t.count(),
            )
        )

        # Execute complex query
        result = flights.query(
            sql="""
            SELECT
                airline,
                total_distance,
                avg_delay,
                flight_count
            FROM flights
            WHERE flight_distance > 1000
            GROUP BY airline
            ORDER BY total_distance DESC
            LIMIT 2
        """
        )

        df = result.execute()

        # Verify results
        assert len(df) == 2
        assert df["airline"].iloc[0] == "AA"  # AA has most distance
        assert df["total_distance"].iloc[0] > df["total_distance"].iloc[1]
        assert all(
            col in df.columns for col in ["airline", "total_distance", "avg_delay", "flight_count"]
        )
