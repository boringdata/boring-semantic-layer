"""
Tests for the query interface (build_query and SemanticTable.query).

Covers:
- Time dimension metadata and transformations
- Time grain aggregations
- Time range filtering
- Mixed dimension queries
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
def sales_data(con):
    """Sample sales data with timestamps."""
    sales_df = pd.DataFrame(
        {
            "order_date": pd.date_range("2024-01-01", periods=100, freq="D"),
            "amount": [100 + i * 10 for i in range(100)],
            "quantity": [1 + i % 5 for i in range(100)],
        }
    )
    return con.create_table("sales", sales_df)


class TestTimeDimensionBasics:
    """Test basic time dimension functionality."""

    def test_time_dimension_with_metadata(self, sales_data):
        """Test that time dimensions can be defined with metadata."""
        st = to_semantic_table(sales_data, "sales").with_dimensions(
            order_date={
                "expr": lambda t: t.order_date,
                "description": "Date of order",
                "is_time_dimension": True,
                "smallest_time_grain": "day",
            }
        )

        # Verify metadata
        assert st._dims["order_date"].is_time_dimension is True
        assert st._dims["order_date"].smallest_time_grain == "day"
        assert st._dims["order_date"].description == "Date of order"

    def test_non_time_dimension(self, sales_data):
        """Test non-time dimensions don't get time treatment."""
        st = to_semantic_table(sales_data, "sales").with_dimensions(
            quantity={
                "expr": lambda t: t.quantity,
                "description": "Order quantity",
                "is_time_dimension": False,
            }
        )

        assert st._dims["quantity"].is_time_dimension is False


class TestTimeGrainTransformations:
    """Test time grain transformations in queries."""

    def test_time_grain_month(self, sales_data):
        """Test querying with monthly time grain."""
        st = (
            to_semantic_table(sales_data, "sales")
            .with_dimensions(
                order_date={
                    "expr": lambda t: t.order_date,
                    "is_time_dimension": True,
                    "smallest_time_grain": "day",
                }
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        # Query with monthly grain
        result = st.query(
            dimensions=["order_date"],
            measures=["total_amount"],
            time_grain="TIME_GRAIN_MONTH",
        ).execute()

        # Should have ~4 months of data (100 days)
        assert len(result) <= 4
        assert "order_date" in result.columns
        assert "total_amount" in result.columns

    def test_time_grain_year(self, sales_data):
        """Test querying with yearly time grain."""
        st = (
            to_semantic_table(sales_data, "sales")
            .with_dimensions(
                order_date={
                    "expr": lambda t: t.order_date,
                    "is_time_dimension": True,
                    "smallest_time_grain": "day",
                }
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        result = st.query(
            dimensions=["order_date"],
            measures=["total_amount"],
            time_grain="TIME_GRAIN_YEAR",
        ).execute()

        # Should have 1 year (all data is in 2024)
        assert len(result) == 1

    def test_time_grain_validation(self, sales_data):
        """Test that invalid time grain raises error."""
        st = (
            to_semantic_table(sales_data, "sales")
            .with_dimensions(
                order_date={
                    "expr": lambda t: t.order_date,
                    "is_time_dimension": True,
                    "smallest_time_grain": "month",
                }
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        # Cannot query at day level if smallest grain is month
        with pytest.raises(ValueError, match="finer than the smallest allowed grain"):
            st.query(
                dimensions=["order_date"],
                measures=["total_amount"],
                time_grain="TIME_GRAIN_DAY",
            ).execute()


class TestTimeRangeFiltering:
    """Test time range filtering functionality."""

    def test_time_range_basic(self, sales_data):
        """Test basic time range filtering."""
        st = (
            to_semantic_table(sales_data, "sales")
            .with_dimensions(
                order_date={
                    "expr": lambda t: t.order_date,
                    "is_time_dimension": True,
                    "smallest_time_grain": "day",
                }
            )
            .with_measures(
                total_amount=lambda t: t.amount.sum(), order_count=lambda t: t.count()
            )
        )

        # Query January only
        result = st.query(
            dimensions=["order_date"],
            measures=["order_count"],
            time_range={"start": "2024-01-01", "end": "2024-01-31"},
        ).execute()

        # Should have 31 days
        assert len(result) == 31

    def test_time_range_with_grain(self, sales_data):
        """Test combining time range with time grain."""
        st = (
            to_semantic_table(sales_data, "sales")
            .with_dimensions(
                order_date={
                    "expr": lambda t: t.order_date,
                    "is_time_dimension": True,
                    "smallest_time_grain": "day",
                }
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        # Query Q1 2024 by month
        result = st.query(
            dimensions=["order_date"],
            measures=["total_amount"],
            time_range={"start": "2024-01-01", "end": "2024-03-31"},
            time_grain="TIME_GRAIN_MONTH",
        ).execute()

        # Should have 3 months (Jan, Feb, Mar)
        assert len(result) == 3
        # Verify only Q1 months are included
        months = pd.to_datetime(result["order_date"]).dt.month.tolist()
        assert all(m in [1, 2, 3] for m in months)

    def test_time_range_invalid_format(self, sales_data):
        """Test that invalid time range format raises error."""
        st = (
            to_semantic_table(sales_data, "sales")
            .with_dimensions(
                order_date={"expr": lambda t: t.order_date, "is_time_dimension": True}
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        # Missing 'end' key
        with pytest.raises(ValueError, match="time_range must be a dict"):
            st.query(
                dimensions=["order_date"],
                measures=["total_amount"],
                time_range={"start": "2024-01-01"},
            ).execute()


class TestTimeDimensionWithNonTimeDimensions:
    """Test mixing time and non-time dimensions."""

    def test_mixed_dimensions(self, con):
        """Test querying with both time and non-time dimensions."""
        df = pd.DataFrame(
            {
                "order_date": pd.date_range("2024-01-01", periods=50, freq="D"),
                "category": ["A"] * 25 + ["B"] * 25,
                "amount": [100 + i * 10 for i in range(50)],
            }
        )
        tbl = con.create_table("sales_cat", df)

        st = (
            to_semantic_table(tbl, "sales")
            .with_dimensions(
                order_date={
                    "expr": lambda t: t.order_date,
                    "is_time_dimension": True,
                    "smallest_time_grain": "day",
                },
                category=lambda t: t.category,  # Non-time dimension
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        # Query by category and month
        result = st.query(
            dimensions=["category", "order_date"],
            measures=["total_amount"],
            time_grain="TIME_GRAIN_MONTH",
        ).execute()

        # Should have 2 categories * ~2 months = ~4 rows
        assert len(result) <= 6
        assert "category" in result.columns
        assert "order_date" in result.columns

    def test_time_grain_only_affects_time_dimensions(self, con):
        """Test that time_grain only affects time dimensions, not regular dimensions."""
        df = pd.DataFrame(
            {
                "order_date": pd.date_range("2024-01-01", periods=30, freq="D"),
                "product_id": [i % 5 for i in range(30)],
                "amount": [100 + i * 10 for i in range(30)],
            }
        )
        tbl = con.create_table("sales_prod", df)

        st = (
            to_semantic_table(tbl, "sales")
            .with_dimensions(
                order_date={
                    "expr": lambda t: t.order_date,
                    "is_time_dimension": True,
                    "smallest_time_grain": "day",
                },
                product_id={
                    "expr": lambda t: t.product_id,
                    "is_time_dimension": False,  # Explicitly not a time dimension
                },
            )
            .with_measures(total_amount=lambda t: t.amount.sum())
        )

        # Query with time grain - should only affect order_date, not product_id
        result = st.query(
            dimensions=["order_date", "product_id"],
            measures=["total_amount"],
            time_grain="TIME_GRAIN_MONTH",
        ).execute()

        # product_id should still have 5 distinct values per month
        assert result["product_id"].nunique() == 5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
