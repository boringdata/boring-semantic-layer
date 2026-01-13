"""
Tests for date/timestamp filter value conversion (TYPE_MISMATCH fix).

This test suite validates that string date/timestamp values in filters are
properly converted to typed literals, preventing TYPE_MISMATCH errors on
strict backends like Athena.
"""

import ibis
import pytest

from boring_semantic_layer.api import to_semantic_table
from boring_semantic_layer.query import Filter, query


class TestDateFilterConversion:
    """Test date/timestamp string conversion in filters."""

    @pytest.fixture
    def orders_table(self):
        """Create orders table with date column."""
        con = ibis.duckdb.connect(":memory:")
        return con.create_table(
            "orders",
            {
                "order_date": ["2024-01-01", "2024-06-15", "2023-12-31", "2024-03-20"],
                "country": ["USA", "UK", "USA", "DE"],
                "amount": [100, 200, 150, 300],
            },
            schema={"order_date": "date", "country": "string", "amount": "int64"},
        )

    @pytest.fixture
    def events_table(self):
        """Create events table with timestamp column."""
        con = ibis.duckdb.connect(":memory:")
        return con.create_table(
            "events",
            {
                "event_time": [
                    "2024-01-01 10:00:00",
                    "2024-06-15 14:30:00",
                    "2023-12-31 23:59:59",
                ],
                "event_type": ["click", "purchase", "view"],
                "value": [10, 20, 5],
            },
            schema={"event_time": "timestamp", "event_type": "string", "value": "int64"},
        )

    def test_date_filter_single_comparison(self, orders_table):
        """Test single date comparison filter (>=)."""
        st = (
            to_semantic_table(orders_table, "orders")
            .with_dimensions(
                order_date=lambda t: t.order_date,
                country=lambda t: t.country,
            )
            .with_measures(order_count=lambda t: t.amount.count())
        )

        result = query(
            st,
            dimensions=["country"],
            measures=["order_count"],
            filters=[{"field": "order_date", "operator": ">=", "value": "2024-01-01"}],
        ).execute()

        # Should include 3 orders from 2024 (Jan, Mar, Jun)
        assert result["order_count"].sum() == 3

    def test_date_filter_range(self, orders_table):
        """Test date range filtering with two filters."""
        st = (
            to_semantic_table(orders_table, "orders")
            .with_dimensions(order_date=lambda t: t.order_date)
            .with_measures(order_count=lambda t: t.amount.count())
        )

        result = query(
            st,
            dimensions=["order_date"],
            measures=["order_count"],
            filters=[
                {"field": "order_date", "operator": ">=", "value": "2024-01-01"},
                {"field": "order_date", "operator": "<=", "value": "2024-06-30"},
            ],
        ).execute()

        # Should include all 3 orders from first half of 2024
        assert len(result) == 3

    def test_date_filter_in_operator(self, orders_table):
        """Test IN operator with multiple date values."""
        st = (
            to_semantic_table(orders_table, "orders")
            .with_dimensions(order_date=lambda t: t.order_date)
            .with_measures(order_count=lambda t: t.amount.count())
        )

        result = query(
            st,
            dimensions=["order_date"],
            measures=["order_count"],
            filters=[
                {
                    "field": "order_date",
                    "operator": "in",
                    "values": ["2024-01-01", "2024-06-15"],
                }
            ],
        ).execute()

        # Should match exactly 2 orders
        assert result["order_count"].sum() == 2

    def test_timestamp_filter(self, events_table):
        """Test timestamp filtering with ISO 8601 format."""
        st = (
            to_semantic_table(events_table, "events")
            .with_dimensions(event_type=lambda t: t.event_type)
            .with_measures(total_value=lambda t: t.value.sum())
        )

        result = query(
            st,
            dimensions=["event_type"],
            measures=["total_value"],
            filters=[{"field": "event_time", "operator": ">=", "value": "2024-01-01T00:00:00"}],
        ).execute()

        # Should include 2 events from 2024
        assert len(result) == 2

    def test_timestamp_with_timezone(self, events_table):
        """Test timestamp with timezone marker."""
        st = (
            to_semantic_table(events_table, "events")
            .with_dimensions(event_type=lambda t: t.event_type)
            .with_measures(total_value=lambda t: t.value.sum())
        )

        result = query(
            st,
            dimensions=["event_type"],
            measures=["total_value"],
            filters=[
                {
                    "field": "event_time",
                    "operator": ">=",
                    "value": "2024-01-01T00:00:00Z",
                }
            ],
        ).execute()

        # Should work and include 2 events
        assert len(result) == 2

    def test_combined_date_and_string_filters(self, orders_table):
        """Test combining date filter with string filter."""
        st = (
            to_semantic_table(orders_table, "orders")
            .with_dimensions(
                order_date=lambda t: t.order_date,
                country=lambda t: t.country,
            )
            .with_measures(order_count=lambda t: t.amount.count())
        )

        result = query(
            st,
            dimensions=["country"],
            measures=["order_count"],
            filters=[
                {"field": "order_date", "operator": ">=", "value": "2024-01-01"},
                {"field": "country", "operator": "=", "value": "USA"},
            ],
        ).execute()

        # Should find 1 USA order from 2024
        assert result["order_count"].sum() == 1

    def test_non_date_string_unchanged(self, orders_table):
        """Test that non-date strings pass through unchanged."""
        st = (
            to_semantic_table(orders_table, "orders")
            .with_dimensions(country=lambda t: t.country)
            .with_measures(order_count=lambda t: t.amount.count())
        )

        result = query(
            st,
            dimensions=["country"],
            measures=["order_count"],
            filters=[{"field": "country", "operator": "=", "value": "USA"}],
        ).execute()

        # Should work normally
        assert result["order_count"].iloc[0] == 2

    def test_numeric_values_unchanged(self, orders_table):
        """Test that numeric values are not affected by conversion."""
        st = (
            to_semantic_table(orders_table, "orders")
            .with_dimensions(country=lambda t: t.country)
            .with_measures(order_count=lambda t: t.amount.count())
        )

        result = query(
            st,
            dimensions=["country"],
            measures=["order_count"],
            filters=[{"field": "amount", "operator": ">=", "value": 200}],
        ).execute()

        # Should find 2 orders with amount >= 200
        assert result["order_count"].sum() == 2


class TestSQLGeneration:
    """Test SQL generation for different backends."""

    def test_trino_sql_generation(self):
        """Test that Trino/Athena SQL uses proper date functions."""
        con = ibis.duckdb.connect(":memory:")
        t = con.create_table(
            "test",
            {"date_col": ["2024-01-01", "2024-06-15"]},
            schema={"date_col": "date"},
        )

        filter_obj = Filter(filter={"field": "date_col", "operator": ">=", "value": "2024-01-01"})
        filtered = filter_obj.to_callable()(t)

        sql = ibis.to_sql(filtered, dialect="trino")

        # Should contain Trino date function
        assert "FROM_ISO8601_TIMESTAMP" in sql

    def test_duckdb_sql_generation(self):
        """Test that DuckDB SQL uses proper date functions."""
        con = ibis.duckdb.connect(":memory:")
        t = con.create_table(
            "test",
            {"date_col": ["2024-01-01", "2024-06-15"]},
            schema={"date_col": "date"},
        )

        filter_obj = Filter(filter={"field": "date_col", "operator": ">=", "value": "2024-01-01"})
        filtered = filter_obj.to_callable()(t)

        sql = ibis.to_sql(filtered, dialect="duckdb")

        # Should contain DuckDB date function
        assert "MAKE_TIMESTAMP" in sql or "MAKE_DATE" in sql


class TestFilterValueConversion:
    """Test the _convert_filter_value method directly."""

    def test_convert_date_string(self):
        """Test conversion of date string."""
        f = Filter(filter={"field": "x", "operator": "=", "value": "2024-01-01"})
        result = f._convert_filter_value("2024-01-01")
        assert isinstance(result, ibis.expr.types.temporal.TimestampScalar)

    def test_convert_timestamp_string(self):
        """Test conversion of timestamp string."""
        f = Filter(filter={"field": "x", "operator": "=", "value": "2024-01-01"})
        result = f._convert_filter_value("2024-01-01T12:00:00")
        assert isinstance(result, ibis.expr.types.temporal.TimestampScalar)

    def test_non_date_string_passthrough(self):
        """Test that non-date strings pass through unchanged."""
        f = Filter(filter={"field": "x", "operator": "=", "value": "USA"})
        result = f._convert_filter_value("USA")
        assert result == "USA"
        assert isinstance(result, str)

    def test_numeric_passthrough(self):
        """Test that numeric values pass through unchanged."""
        f = Filter(filter={"field": "x", "operator": "=", "value": 123})
        result = f._convert_filter_value(123)
        assert result == 123
        assert isinstance(result, int)

    def test_none_passthrough(self):
        """Test that None passes through unchanged."""
        f = Filter(filter={"field": "x", "operator": "=", "value": None})
        result = f._convert_filter_value(None)
        assert result is None
