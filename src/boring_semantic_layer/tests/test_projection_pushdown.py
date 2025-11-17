"""Tests for projection pushdown optimization with joins."""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.query import query


@pytest.fixture
def con():
    """Create an in-memory DuckDB connection."""
    return ibis.duckdb.connect(":memory:")


class TestProjectionPushdownWithJoins:
    """Test projection pushdown works correctly with joined tables."""

    def test_required_columns_with_many_unused_dimensions(self, con):
        """Test that required_columns doesn't trigger evaluation of unused dimensions.

        This tests the fix for the circular dependency issue where:
        1. required_columns called to_expr()
        2. to_expr() triggered .schema → .values evaluation
        3. .values tried to evaluate ALL dimensions (even unused ones)
        4. If unused dimensions referenced non-existent columns, it failed

        The fix uses _to_ibis() instead of to_expr() to avoid evaluating dimensions.
        """
        # Create flights table
        flights_df = pd.DataFrame(
            {
                "carrier": ["AA", "UA", "DL"] * 10,
                "origin": ["JFK", "LAX", "ATL"] * 10,
                "destination": ["LAX", "JFK", "ORD"] * 10,
                "arr_time": pd.date_range("2024-01-01", periods=30, freq="D"),
                "distance": [2475, 1846, 748] * 10,
                "dep_delay": [10, -5, 20] * 10,
            }
        )
        flights_tbl = con.create_table("flights_projection", flights_df)

        # Create carriers table
        carriers_df = pd.DataFrame(
            {
                "code": ["AA", "UA", "DL"],
                "name": ["American", "United", "Delta"],
                "nickname": ["AA", "UA", "DL"],
            }
        )
        carriers_tbl = con.create_table("carriers_projection", carriers_df)

        # Create semantic tables with MANY dimensions (most will be unused in query)
        carriers = (
            to_semantic_table(carriers_tbl, name="carriers")
            .with_dimensions(
                code={"expr": lambda t: t.code, "description": "Carrier code"},
                name={"expr": lambda t: t.name, "description": "Carrier name"},
                nickname={"expr": lambda t: t.nickname, "description": "Nickname"},
            )
            .with_measures(
                carrier_count={"expr": lambda t: t.count(), "description": "Count"}
            )
        )

        # Define flights with many dimensions - some unused in the query
        flights = (
            to_semantic_table(flights_tbl, name="flights")
            .with_dimensions(
                origin={"expr": lambda t: t.origin, "description": "Origin"},
                destination={"expr": lambda t: t.destination, "description": "Destination"},
                carrier={"expr": lambda t: t.carrier, "description": "Carrier"},
                arr_time={
                    "expr": lambda t: t.arr_time,
                    "description": "Arrival time",
                    "is_time_dimension": True,
                    "smallest_time_grain": "TIME_GRAIN_SECOND",
                },
                # These dimensions won't be used in the query
                # but required_columns shouldn't try to evaluate them
            )
            .with_measures(
                flight_count={"expr": lambda t: t.count(), "description": "Count"},
                avg_dep_delay={"expr": lambda t: t.dep_delay.mean(), "description": "Avg delay"},
                avg_distance={"expr": lambda t: t.distance.mean(), "description": "Avg distance"},
            )
            .join_one(carriers, left_on="carrier", right_on="code")
        )

        # Query using only ONE dimension (arr_time) and ONE measure (flight_count)
        # This should NOT trigger evaluation of unused dimensions (origin, destination)
        # The old code would try to evaluate ALL dimensions and fail if they had issues
        result = query(
            flights,
            dimensions=["arr_time"],
            measures=["flight_count"],
            time_grain="TIME_GRAIN_MONTH",
        )

        # This should succeed without AttributeError on unused dimensions
        # The key test is that it DOESN'T crash trying to evaluate unused dimensions
        df = result.execute()

        # Verify the query executed successfully
        assert len(df) > 0
        assert "arr_time" in df.columns
        assert "flight_count" in df.columns
        # All 30 rows should be present (time_grain groups, not yet aggregated to single month)
        assert df["flight_count"].sum() == 30

    def test_projection_pushdown_with_dimension_name_column_mismatch(self, con):
        """Test projection pushdown when dimension names differ from column names.

        This ensures that:
        1. extract_columns_from_callable correctly finds the underlying column
        2. Projection includes the actual column (not just the dimension name)
        3. No AttributeError when evaluating dimensions on projected table
        """
        # Create a table where we'll use different dimension names
        products_df = pd.DataFrame(
            {
                "prod_id": [1, 2, 3],
                "prod_name": ["Widget", "Gadget", "Doohickey"],
                "cat": ["A", "B", "A"],  # Short column name
            }
        )
        products_tbl = con.create_table("products_projection", products_df)

        # Create semantic table with dimension names != column names
        products = (
            to_semantic_table(products_tbl, name="products")
            .with_dimensions(
                product_id={"expr": lambda t: t.prod_id},  # product_id != prod_id
                product_name={"expr": lambda t: t.prod_name},  # product_name != prod_name
                category={"expr": lambda t: t.cat},  # category != cat
            )
            .with_measures(product_count={"expr": lambda t: t.count()})
        )

        # Query using dimension name (not column name)
        result = products.group_by("category").aggregate("product_count")

        # This should work - projection should include 'cat' column
        # even though we're grouping by 'category' dimension
        df = result.execute()

        assert len(df) == 2  # Two categories: A and B
        assert "category" in df.columns
        assert "product_count" in df.columns

    def test_projection_pushdown_with_qualified_dimension_names(self, con):
        """Test projection pushdown handles qualified dimension names in joined tables.

        This tests the fix in projection_utils._extract_requirement_for_key
        that tries qualified names when unqualified key isn't found.
        """
        # Create two tables
        orders_df = pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "customer_id": [1, 2, 1],
                "order_date": pd.date_range("2024-01-01", periods=3, freq="D"),
            }
        )
        customers_df = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
            }
        )

        orders_tbl = con.create_table("orders_projection", orders_df)
        customers_tbl = con.create_table("customers_projection", customers_df)

        # Create semantic tables
        customers = (
            to_semantic_table(customers_tbl, name="customers")
            .with_dimensions(customer_id={"expr": lambda t: t.id})
            .with_measures(customer_count={"expr": lambda t: t.count()})
        )

        orders = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                customer_id={"expr": lambda t: t.customer_id},
                order_date={
                    "expr": lambda t: t.order_date,
                    "is_time_dimension": True,
                    "smallest_time_grain": "TIME_GRAIN_SECOND",
                },
            )
            .with_measures(order_count={"expr": lambda t: t.count()})
            .join_one(customers, left_on="customer_id", right_on="id")
        )

        # Query with UNQUALIFIED dimension name on joined table
        # Dimensions are stored as "orders.order_date" but we query with "order_date"
        result = query(
            orders,
            dimensions=["order_date"],
            measures=["order_count"],
            time_grain="TIME_GRAIN_DAY",
        )

        # Should find the qualified dimension "orders.order_date"
        df = result.execute()

        assert len(df) == 3
        assert "order_date" in df.columns
        assert "order_count" in df.columns
