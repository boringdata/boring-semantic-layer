"""
Tests for the query interface with filters and time dimensions.
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture(scope="module")
def con():
    """DuckDB connection for tests."""
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def flights_data(con):
    """Sample flights data."""
    df = pd.DataFrame(
        {
            "carrier": ["AA", "UA", "DL", "AA", "UA", "DL"] * 5,
            "origin": ["JFK", "SFO", "LAX", "ORD", "DEN", "ATL"] * 5,
            "distance": [100, 200, 300, 150, 250, 350] * 5,
            "passengers": [50, 75, 100, 60, 80, 110] * 5,
        },
    )
    return con.create_table("flights", df)


@pytest.fixture(scope="module")
def sales_data(con):
    """Sample sales data with timestamps."""
    df = pd.DataFrame(
        {
            "order_date": pd.date_range("2024-01-01", periods=100, freq="D"),
            "amount": [100 + i * 10 for i in range(100)],
            "quantity": [1 + i % 5 for i in range(100)],
        },
    )
    return con.create_table("sales", df)


class TestBasicQuery:
    """Test basic query functionality."""

    def test_simple_query(self, flights_data):
        """Test basic query with dimensions and measures."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
        ).execute()

        assert len(result) == 3
        assert "carrier" in result.columns
        assert "total_passengers" in result.columns

    def test_query_without_dimensions(self, flights_data):
        """Test query with only measures (grand total)."""
        st = to_semantic_table(flights_data, "flights").with_measures(
            total_passengers=lambda t: t.passengers.sum(),
        )

        result = st.query(measures=["total_passengers"]).execute()

        assert len(result) == 1
        assert "total_passengers" in result.columns

    def test_query_with_order_by(self, flights_data):
        """Test query with ordering."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            order_by=[("total_passengers", "desc")],
        ).execute()

        assert result["total_passengers"].iloc[0] >= result["total_passengers"].iloc[1]

    def test_query_with_limit(self, flights_data):
        """Test query with limit."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            limit=2,
        ).execute()

        assert len(result) == 2


class TestFilters:
    """Test filter functionality."""

    def test_lambda_filter(self, flights_data):
        """Test query with lambda filter."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[lambda t: t.distance > 200],
        ).execute()

        assert len(result) > 0

    def test_json_filter_simple(self, flights_data):
        """Test query with JSON dict filter."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[{"field": "distance", "operator": ">", "value": 200}],
        ).execute()

        assert len(result) > 0

    def test_json_filter_in_operator(self, flights_data):
        """Test JSON filter with 'in' operator."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[{"field": "carrier", "operator": "in", "values": ["AA", "UA"]}],
        ).execute()

        assert len(result) == 2
        assert all(c in ["AA", "UA"] for c in result["carrier"])

    def test_json_filter_compound_and(self, flights_data):
        """Test JSON filter with compound AND."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[
                {
                    "operator": "AND",
                    "conditions": [
                        {"field": "distance", "operator": ">", "value": 150},
                        {"field": "passengers", "operator": ">=", "value": 75},
                    ],
                },
            ],
        ).execute()

        assert len(result) > 0

    def test_multiple_filters(self, flights_data):
        """Test query with multiple filters."""
        st = (
            to_semantic_table(flights_data, "flights")
            .with_dimensions(carrier=lambda t: t.carrier)
            .with_measures(total_passengers=lambda t: t.passengers.sum())
        )

        result = st.query(
            dimensions=["carrier"],
            measures=["total_passengers"],
            filters=[
                lambda t: t.distance > 100,
                {"field": "passengers", "operator": ">=", "value": 60},
            ],
        ).execute()

        assert len(result) > 0


class TestTimeDimensions:
    """Test time dimension functionality."""

    def test_time_dimension_metadata(self, sales_data):
        """Test that time dimensions can be defined with metadata."""
        st = to_semantic_table(sales_data, "sales").with_dimensions(
            order_date={
                "expr": lambda t: t.order_date,
                "description": "Date of order",
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            },
        )

        dims_dict = st.get_dimensions()
        assert dims_dict["order_date"].is_time_dimension is True
        assert dims_dict["order_date"].smallest_time_grain == "day"

    def test_time_grain_month(self, sales_data):
        """Test querying with monthly time grain."""
        st = (
            to_semantic_table(sales_data, "sales")
            .with_dimensions(
                order_date={
                    "expr": lambda t: t.order_date,
                    "is_time_dimension": True,
                    "smallest_time_grain": "day",
                },
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        result = st.query(
            dimensions=["order_date"],
            measures=["total_amount"],
            time_grain="TIME_GRAIN_MONTH",
        ).execute()

        assert len(result) <= 4
        assert "order_date" in result.columns
        assert "total_amount" in result.columns

    def test_time_range_filter(self, sales_data):
        """Test querying with time range filter."""
        st = (
            to_semantic_table(sales_data, "sales")
            .with_dimensions(
                order_date={
                    "expr": lambda t: t.order_date,
                    "is_time_dimension": True,
                    "smallest_time_grain": "day",
                },
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        result = st.query(
            dimensions=["order_date"],
            measures=["total_amount"],
            time_range={"start": "2024-01-01", "end": "2024-01-31"},
        ).execute()

        assert len(result) <= 31

    def test_time_grain_validation(self, sales_data):
        """Test that requesting finer grain than allowed raises error."""
        st = (
            to_semantic_table(sales_data, "sales")
            .with_dimensions(
                order_date={
                    "expr": lambda t: t.order_date,
                    "is_time_dimension": True,
                    "smallest_time_grain": "month",
                },
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        with pytest.raises(ValueError, match="finer than the smallest allowed grain"):
            st.query(
                dimensions=["order_date"],
                measures=["total_amount"],
                time_grain="TIME_GRAIN_DAY",
            ).execute()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
