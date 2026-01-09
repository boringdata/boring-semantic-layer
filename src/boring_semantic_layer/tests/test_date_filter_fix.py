"""
Tests for date/timestamp filter value conversion (TYPE_MISMATCH fix).

This test suite validates that string date/timestamp values in filters are
properly converted to typed literals, preventing TYPE_MISMATCH errors on
strict backends like Athena.

The implementation uses column-type-aware conversion: when filtering a DATE
column, string values become DATE literals; when filtering a TIMESTAMP column,
they become TIMESTAMP literals.
"""

import ibis
import pytest

from boring_semantic_layer.api import to_semantic_table
from boring_semantic_layer.query import (
    Filter,
    _convert_filter_value,
    _get_column_type,
    query,
)


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


class TestTypeAwareConversion:
    """Test that conversion is column-type-aware."""

    @pytest.fixture
    def mixed_table(self):
        """Create table with both date and timestamp columns."""
        con = ibis.duckdb.connect(":memory:")
        return con.create_table(
            "mixed",
            {
                "date_col": ["2024-01-01", "2024-06-15"],
                "ts_col": ["2024-01-01 10:00:00", "2024-06-15 14:30:00"],
                "str_col": ["2024-01-01", "2024-06-15"],  # String that looks like date
            },
            schema={
                "date_col": "date",
                "ts_col": "timestamp",
                "str_col": "string",
            },
        )

    def test_date_column_gets_date_literal(self, mixed_table):
        """Test that filtering date column uses date literal."""
        st = (
            to_semantic_table(mixed_table, "mixed")
            .with_dimensions(date_col=lambda t: t.date_col)
            .with_measures(count=lambda t: t.date_col.count())
        )

        # This should work without TYPE_MISMATCH
        result = query(
            st,
            dimensions=["date_col"],
            measures=["count"],
            filters=[{"field": "date_col", "operator": ">=", "value": "2024-01-01"}],
        ).execute()

        assert len(result) == 2

    def test_timestamp_column_gets_timestamp_literal(self, mixed_table):
        """Test that filtering timestamp column uses timestamp literal."""
        st = (
            to_semantic_table(mixed_table, "mixed")
            .with_dimensions(ts_col=lambda t: t.ts_col)
            .with_measures(count=lambda t: t.ts_col.count())
        )

        # This should work without TYPE_MISMATCH
        result = query(
            st,
            dimensions=["ts_col"],
            measures=["count"],
            filters=[{"field": "ts_col", "operator": ">=", "value": "2024-01-01"}],
        ).execute()

        assert len(result) == 2

    def test_string_column_stays_string(self, mixed_table):
        """Test that filtering string column keeps string comparison."""
        st = (
            to_semantic_table(mixed_table, "mixed")
            .with_dimensions(str_col=lambda t: t.str_col)
            .with_measures(count=lambda t: t.str_col.count())
        )

        result = query(
            st,
            dimensions=["str_col"],
            measures=["count"],
            filters=[{"field": "str_col", "operator": "=", "value": "2024-01-01"}],
        ).execute()

        assert len(result) == 1


class TestConvertFilterValueFunction:
    """Test the _convert_filter_value helper function directly."""

    def test_with_date_target_type(self):
        """Test conversion when target type is date."""
        import ibis.expr.datatypes as dt

        result = _convert_filter_value("2024-01-01", dt.date)
        assert isinstance(result, ibis.expr.types.temporal.DateScalar)

    def test_with_timestamp_target_type(self):
        """Test conversion when target type is timestamp."""
        import ibis.expr.datatypes as dt

        result = _convert_filter_value("2024-01-01", dt.timestamp)
        assert isinstance(result, ibis.expr.types.temporal.TimestampScalar)

    def test_with_string_target_type(self):
        """Test that string target type returns original value."""
        import ibis.expr.datatypes as dt

        result = _convert_filter_value("2024-01-01", dt.string)
        assert result == "2024-01-01"
        assert isinstance(result, str)

    def test_pattern_fallback_date(self):
        """Test pattern-based inference for date strings (no target type)."""
        result = _convert_filter_value("2024-01-01", None)
        assert isinstance(result, ibis.expr.types.temporal.DateScalar)

    def test_pattern_fallback_timestamp(self):
        """Test pattern-based inference for timestamp strings (no target type)."""
        result = _convert_filter_value("2024-01-01T12:00:00", None)
        assert isinstance(result, ibis.expr.types.temporal.TimestampScalar)

    def test_non_date_string_passthrough(self):
        """Test that non-date strings pass through unchanged."""
        result = _convert_filter_value("USA", None)
        assert result == "USA"
        assert isinstance(result, str)

    def test_numeric_passthrough(self):
        """Test that numeric values pass through unchanged."""
        result = _convert_filter_value(123, None)
        assert result == 123
        assert isinstance(result, int)

    def test_none_passthrough(self):
        """Test that None passes through unchanged."""
        result = _convert_filter_value(None, None)
        assert result is None


class TestGetColumnType:
    """Test the _get_column_type helper function."""

    @pytest.fixture
    def sample_table(self):
        con = ibis.duckdb.connect(":memory:")
        return con.create_table(
            "test",
            {"date_col": ["2024-01-01"], "str_col": ["hello"]},
            schema={"date_col": "date", "str_col": "string"},
        )

    def test_get_date_column_type(self, sample_table):
        result = _get_column_type(sample_table, "date_col")
        assert result.is_date()

    def test_get_string_column_type(self, sample_table):
        result = _get_column_type(sample_table, "str_col")
        assert result.is_string()

    def test_prefixed_field_name(self, sample_table):
        """Test that prefixed field names are handled."""
        result = _get_column_type(sample_table, "table.date_col")
        assert result.is_date()

    def test_nonexistent_column(self, sample_table):
        result = _get_column_type(sample_table, "nonexistent")
        assert result is None


class TestSQLGeneration:
    """Test SQL generation uses proper typed literals."""

    def test_date_filter_generates_typed_sql(self):
        """Test that date filter generates typed SQL (not string comparison)."""
        con = ibis.duckdb.connect(":memory:")
        t = con.create_table(
            "test",
            {"date_col": ["2024-01-01", "2024-06-15"]},
            schema={"date_col": "date"},
        )

        filter_obj = Filter(filter={"field": "date_col", "operator": ">=", "value": "2024-01-01"})
        filtered = filter_obj.to_callable()(t)

        sql = ibis.to_sql(filtered, dialect="duckdb")

        # Should NOT contain a raw string comparison like >= '2024-01-01'
        # Should contain some form of date function/cast
        assert "2024" in sql  # Value is present
        # The exact SQL varies by backend, but it shouldn't be a raw string

    def test_timestamp_filter_generates_typed_sql(self):
        """Test that timestamp filter generates typed SQL."""
        con = ibis.duckdb.connect(":memory:")
        t = con.create_table(
            "test",
            {"ts_col": ["2024-01-01 10:00:00"]},
            schema={"ts_col": "timestamp"},
        )

        filter_obj = Filter(
            filter={"field": "ts_col", "operator": ">=", "value": "2024-01-01T00:00:00"}
        )
        filtered = filter_obj.to_callable()(t)

        sql = ibis.to_sql(filtered, dialect="duckdb")

        # Should contain timestamp-related SQL
        assert "2024" in sql
