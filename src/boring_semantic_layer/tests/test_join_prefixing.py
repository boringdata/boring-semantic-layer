"""
Comprehensive tests for table.measure prefixing in joined tables.

Tests the dot notation for accessing measures and dimensions
from different tables after joins, including edge cases and deep join scenarios.
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture(scope="module")
def con():
    """DuckDB connection for all tests."""
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def ecommerce_tables(con):
    """Create a realistic e-commerce schema with multiple tables."""
    # Orders table
    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4],
            "customer_id": [101, 102, 101, 103],
            "order_date": pd.to_datetime(
                ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
            ),
            "total_amount": [100.0, 200.0, 150.0, 300.0],
        },
    )

    # Customers table
    customers_df = pd.DataFrame(
        {
            "customer_id": [101, 102, 103],
            "name": ["Alice", "Bob", "Charlie"],
            "country": ["US", "UK", "US"],
        },
    )

    # Order items table
    order_items_df = pd.DataFrame(
        {
            "item_id": [1, 2, 3, 4, 5],
            "order_id": [1, 1, 2, 3, 4],
            "product_id": [501, 502, 501, 503, 502],
            "quantity": [2, 1, 3, 1, 2],
            "price": [25.0, 50.0, 25.0, 150.0, 50.0],
        },
    )

    # Products table
    products_df = pd.DataFrame(
        {
            "product_id": [501, 502, 503],
            "product_name": ["Widget A", "Widget B", "Widget C"],
            "category": ["electronics", "electronics", "home"],
        },
    )

    return {
        "orders": con.create_table("orders", orders_df),
        "customers": con.create_table("customers", customers_df),
        "order_items": con.create_table("order_items", order_items_df),
        "products": con.create_table("products", products_df),
    }


class TestBasicPrefixing:
    """Test basic prefixing behavior with simple joins."""

    def test_conflicting_measure_names_both_accessible(self, ecommerce_tables):
        """Test that measures with same name from different tables are both accessible."""
        orders_tbl = ecommerce_tables["orders"]
        order_items_tbl = ecommerce_tables["order_items"]

        # Both tables have a 'count' measure
        orders_st = (
            to_semantic_table(orders_tbl, "orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(record_count=lambda t: t.count())
        )

        items_st = to_semantic_table(order_items_tbl, "items").with_measures(
            record_count=lambda t: t.count(),
        )

        joined = orders_st.join_many(items_st, lambda o, i: o.order_id == i.order_id)

        result = (
            joined.group_by("orders.customer_id")
            .aggregate("orders.record_count", "items.record_count")
            .execute()
        )

        assert "orders.record_count" in result.columns
        assert "items.record_count" in result.columns
        # After join, counts are per joined rows, not original table counts
        # Alice (101) has 2 orders with 3 total items, so after join we get 3 rows
        alice_row = result[result["orders.customer_id"] == 101].iloc[0]
        # Both counts will be 3 because the join creates 3 rows for Alice
        assert alice_row["orders.record_count"] == 3
        assert alice_row["items.record_count"] == 3

    def test_short_name_resolves_to_first_match(self, ecommerce_tables):
        """Test that short names resolve to the first prefixed match."""
        orders_tbl = ecommerce_tables["orders"]
        customers_tbl = ecommerce_tables["customers"]

        orders_st = (
            to_semantic_table(orders_tbl, "orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(total=lambda t: t.total_amount.sum())
        )

        customers_st = (
            to_semantic_table(customers_tbl, "customers")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(total=lambda t: t.count())  # Different meaning of "total"
        )

        joined = orders_st.join_many(
            customers_st,
            lambda o, c: o.customer_id == c.customer_id,
        )

        # Short name should resolve to orders.total (first match)
        result = (
            joined.group_by("orders.customer_id")
            .aggregate("total")  # Should be orders.total
            .execute()
        )

        assert "total" in result.columns
        # Alice's total should be sum of order amounts (100 + 150 = 250), not count
        alice_total = result[result["orders.customer_id"] == 101]["total"].iloc[0]
        assert alice_total == 250.0

    def test_explicit_prefix_overrides_short_name(self, ecommerce_tables):
        """Test that explicit prefixed names work even when short names exist."""
        orders_tbl = ecommerce_tables["orders"]
        customers_tbl = ecommerce_tables["customers"]

        orders_st = (
            to_semantic_table(orders_tbl, "orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(item_count=lambda t: t.count())
        )

        customers_st = (
            to_semantic_table(customers_tbl, "customers")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(item_count=lambda t: t.count())
        )

        joined = orders_st.join_many(
            customers_st,
            lambda o, c: o.customer_id == c.customer_id,
        )

        # Both explicit prefixed names should work
        result = (
            joined.group_by("orders.customer_id")
            .aggregate("orders.item_count", "customers.item_count")
            .execute()
        )

        assert "orders.item_count" in result.columns
        assert "customers.item_count" in result.columns
        # After join, counts are the same (count of joined rows)
        assert all(result["orders.item_count"] == result["customers.item_count"])


class TestDotAndBracketNotation:
    """Test that both dot and bracket notation work with prefixed names."""

    def test_dot_notation_with_prefixed_measures(self, ecommerce_tables):
        """Test t.table__measure notation."""
        orders_tbl = ecommerce_tables["orders"]
        customers_tbl = ecommerce_tables["customers"]

        orders_st = (
            to_semantic_table(orders_tbl, "orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(order_count=lambda t: t.count())
        )

        customers_st = (
            to_semantic_table(customers_tbl, "customers")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(customer_count=lambda t: t.count())
        )

        joined = orders_st.join_many(
            customers_st,
            lambda o, c: o.customer_id == c.customer_id,
        )

        # Use bracket notation for accessing prefixed measures (dots not allowed in Python identifiers)
        result = (
            joined.with_measures(
                combined=lambda t: t["orders.order_count"] + t["customers.customer_count"],
            )
            .group_by("orders.customer_id")
            .aggregate("combined")
            .execute()
        )

        assert "combined" in result.columns
        assert all(result["combined"] > 0)

    def test_bracket_notation_with_prefixed_measures(self, ecommerce_tables):
        """Test t['table.measure'] notation."""
        orders_tbl = ecommerce_tables["orders"]
        customers_tbl = ecommerce_tables["customers"]

        orders_st = (
            to_semantic_table(orders_tbl, "orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(order_count=lambda t: t.count())
        )

        customers_st = (
            to_semantic_table(customers_tbl, "customers")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(customer_count=lambda t: t.count())
        )

        joined = orders_st.join_many(
            customers_st,
            lambda o, c: o.customer_id == c.customer_id,
        )

        # Use bracket notation in with_measures
        result = (
            joined.with_measures(
                combined=lambda t: t["orders.order_count"] + t["customers.customer_count"],
            )
            .group_by("orders.customer_id")
            .aggregate("combined")
            .execute()
        )

        assert "combined" in result.columns

    def test_mixed_notation_in_calculations(self, ecommerce_tables):
        """Test mixing bracket notation for accessing prefixed measures."""
        orders_tbl = ecommerce_tables["orders"]
        customers_tbl = ecommerce_tables["customers"]

        orders_st = (
            to_semantic_table(orders_tbl, "orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(order_count=lambda t: t.count())
        )

        customers_st = (
            to_semantic_table(customers_tbl, "customers")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(customer_count=lambda t: t.count())
        )

        joined = orders_st.join_many(
            customers_st,
            lambda o, c: o.customer_id == c.customer_id,
        )

        # Use bracket notation for accessing prefixed measures
        result = (
            joined.with_measures(
                mixed=lambda t: t["orders.order_count"] + t["customers.customer_count"],
            )
            .group_by("orders.customer_id")
            .aggregate("mixed")
            .execute()
        )

        assert "mixed" in result.columns


class TestMultipleJoins:
    """Test prefixing with multiple joined tables."""

    def test_three_way_join_all_prefixed(self, ecommerce_tables):
        """Test that all measures are properly prefixed in three-way join."""
        orders_tbl = ecommerce_tables["orders"]
        customers_tbl = ecommerce_tables["customers"]
        order_items_tbl = ecommerce_tables["order_items"]

        orders_st = (
            to_semantic_table(orders_tbl, "orders")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                order_id=lambda t: t.order_id,
            )
            .with_measures(order_count=lambda t: t.count())
        )

        customers_st = (
            to_semantic_table(customers_tbl, "customers")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(customer_count=lambda t: t.count())
        )

        items_st = (
            to_semantic_table(order_items_tbl, "items")
            .with_dimensions(order_id=lambda t: t.order_id)
            .with_measures(item_count=lambda t: t.count())
        )

        # Join all three tables - use raw column access in join predicates
        joined = orders_st.join_many(
            customers_st,
            lambda o, c: o.customer_id == c.customer_id,
        ).join_many(items_st, lambda oc, i: oc.order_id == i.order_id)

        # All three measures should be accessible with prefixes
        # After multiple joins, dimension names also get nested prefixes
        # Use the customer_id dimension from the original orders table
        all_dims = list(joined._dims.keys())
        customer_dim = [d for d in all_dims if "customer_id" in d][0]

        result = (
            joined.group_by(customer_dim)
            .aggregate(
                "orders.order_count",
                "customers.customer_count",
                "items.item_count",
            )
            .execute()
        )

        assert "orders.order_count" in result.columns
        assert "customers.customer_count" in result.columns
        assert "items.item_count" in result.columns

    def test_chained_joins_preserve_prefixes(self, ecommerce_tables):
        """Test that prefixes are preserved through chained joins."""
        orders_tbl = ecommerce_tables["orders"]
        order_items_tbl = ecommerce_tables["order_items"]
        products_tbl = ecommerce_tables["products"]

        orders_st = (
            to_semantic_table(orders_tbl, "orders")
            .with_dimensions(order_id=lambda t: t.order_id)
            .with_measures(revenue=lambda t: t.total_amount.sum())
        )

        items_st = (
            to_semantic_table(order_items_tbl, "items")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                product_id=lambda t: t.product_id,
            )
            .with_measures(quantity_sold=lambda t: t.quantity.sum())
        )

        products_st = (
            to_semantic_table(products_tbl, "products")
            .with_dimensions(
                product_id=lambda t: t.product_id,
                category=lambda t: t.category,
            )
            .with_measures(product_count=lambda t: t.count())
        )

        # Chain joins - use raw column access in join predicates
        joined = orders_st.join_many(
            items_st,
            lambda o, i: o.order_id == i.order_id,
        ).join_many(products_st, lambda oi, p: oi.product_id == p.product_id)

        # Access measures from all three tables using bracket notation
        result = (
            joined.with_measures(
                combined_metric=lambda t: (
                    t["orders.revenue"] + t["items.quantity_sold"] + t["products.product_count"]
                ),
            )
            .group_by("products.category")
            .aggregate("combined_metric")
            .execute()
        )

        assert "combined_metric" in result.columns


class TestCalculatedMeasuresWithPrefixes:
    """Test that calculated measures work correctly with prefixed base measures."""

    def test_calculated_measure_references_prefixed_measures(self, ecommerce_tables):
        """Test that calculated measures can reference prefixed base measures."""
        orders_tbl = ecommerce_tables["orders"]
        order_items_tbl = ecommerce_tables["order_items"]

        orders_st = (
            to_semantic_table(orders_tbl, "orders")
            .with_dimensions(order_id=lambda t: t.order_id)
            .with_measures(
                order_count=lambda t: t.count(),
                total_revenue=lambda t: t.total_amount.sum(),
            )
            .with_measures(avg_order_value=lambda t: t.total_revenue / t.order_count)
        )

        items_st = (
            to_semantic_table(order_items_tbl, "items")
            .with_dimensions(order_id=lambda t: t.order_id)
            .with_measures(
                item_count=lambda t: t.count(),
                total_quantity=lambda t: t.quantity.sum(),
            )
        )

        joined = orders_st.join_many(items_st, lambda o, i: o.order_id == i.order_id)

        # The calculated measure should be properly prefixed
        assert "orders.avg_order_value" in joined._calc_measures

        result = joined.group_by("orders.order_id").aggregate("orders.avg_order_value").execute()

        assert "orders.avg_order_value" in result.columns

    def test_post_join_calculated_measures(self, ecommerce_tables):
        """Test creating calculated measures after joining tables."""
        orders_tbl = ecommerce_tables["orders"]
        customers_tbl = ecommerce_tables["customers"]

        orders_st = (
            to_semantic_table(orders_tbl, "orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(
                order_count=lambda t: t.count(),
                revenue=lambda t: t.total_amount.sum(),
            )
        )

        customers_st = (
            to_semantic_table(customers_tbl, "customers")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(customer_count=lambda t: t.count())
        )

        joined = orders_st.join_many(
            customers_st,
            lambda o, c: o.customer_id == c.customer_id,
        )

        # Create new calculated measure using prefixed measures from both tables (use bracket notation)
        result = (
            joined.with_measures(
                orders_per_customer=lambda t: t["orders.order_count"]
                / t["customers.customer_count"],
            )
            .group_by("orders.customer_id")
            .aggregate("orders_per_customer")
            .execute()
        )

        assert "orders_per_customer" in result.columns
        # Each customer should have at least 1 order per customer (since we joined)
        assert all(result["orders_per_customer"] >= 1.0)


class TestDimensionPrefixing:
    """Test that dimensions are also properly prefixed."""

    def test_dimension_prefixing_on_join(self, ecommerce_tables):
        """Test that dimensions get prefixed with table names."""
        orders_tbl = ecommerce_tables["orders"]
        customers_tbl = ecommerce_tables["customers"]

        orders_st = (
            to_semantic_table(orders_tbl, "orders")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                order_date=lambda t: t.order_date,
            )
            .with_measures(order_count=lambda t: t.count())
        )

        customers_st = (
            to_semantic_table(customers_tbl, "customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                country=lambda t: t.country,
            )
            .with_measures(customer_count=lambda t: t.count())
        )

        joined = orders_st.join_many(
            customers_st,
            lambda o, c: o.customer_id == c.customer_id,
        )

        # Both customer_id dimensions should be prefixed
        assert "orders.customer_id" in joined._dims
        assert "customers.customer_id" in joined._dims
        assert "customers.country" in joined._dims

    def test_group_by_with_prefixed_dimensions(self, ecommerce_tables):
        """Test grouping by prefixed dimension names."""
        orders_tbl = ecommerce_tables["orders"]
        customers_tbl = ecommerce_tables["customers"]

        orders_st = (
            to_semantic_table(orders_tbl, "orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(revenue=lambda t: t.total_amount.sum())
        )

        customers_st = (
            to_semantic_table(customers_tbl, "customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                country=lambda t: t.country,
            )
            .with_measures(customer_count=lambda t: t.count())
        )

        joined = orders_st.join_many(
            customers_st,
            lambda o, c: o.customer_id == c.customer_id,
        )

        # Group by prefixed dimension from customers table
        result = joined.group_by("customers.country").aggregate("orders.revenue").execute()

        assert "customers.country" in result.columns
        assert "orders.revenue" in result.columns
        # US should have more revenue (Alice + Charlie)
        us_revenue = result[result["customers.country"] == "US"]["orders.revenue"].sum()
        uk_revenue = result[result["customers.country"] == "UK"]["orders.revenue"].sum()
        assert us_revenue > uk_revenue


class TestJoinOneMethod:
    """Test join_one() method with left_on/right_on parameters."""

    @pytest.mark.xfail(
        reason="ibis formatter has issues with truthiness checks on table expressions"
    )
    def test_join_one_repr_does_not_crash(self, ecommerce_tables):
        """Test that repr() on a grouped join does not crash with AttributeError."""
        orders_tbl = ecommerce_tables["orders"]
        customers_tbl = ecommerce_tables["customers"]

        # Create orders with product_category dimension
        orders_df = pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4],
                "customer_id": [101, 102, 101, 103],
                "amount": [100.0, 200.0, 150.0, 300.0],
                "product_category": ["electronics", "books", "electronics", "books"],
            }
        )
        orders_tbl = (
            ecommerce_tables["orders"]._find_backend().create_table("orders_repr_test", orders_df)
        )

        orders = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                category=lambda t: t.product_category,
            )
            .with_measures(
                total_revenue=lambda t: t.amount.sum(),
            )
        )

        customers = (
            to_semantic_table(customers_tbl, name="customers")
            .with_measures(
                customer_count=lambda t: t.customer_id.count(),
            )
            .with_dimensions(
                country=lambda t: t.country,
            )
        )

        joined = orders.join_one(
            customers,
            lambda o, c: o.customer_id == c.customer_id,
        )

        # Create a group_by operation
        grouped = joined.group_by("customers.country").aggregate("orders.total_revenue")

        # This should not crash with AttributeError: 'SemanticJoinOp' object has no attribute 'calc_measures'
        repr_str = repr(grouped)
        assert "SemanticGroupByOp" in repr_str or "SemanticAggregateOp" in repr_str

    def test_join_one_with_dimension_access(self, ecommerce_tables):
        """Test that dimensions work correctly after join_one()."""
        orders_tbl = ecommerce_tables["orders"]
        customers_tbl = ecommerce_tables["customers"]

        # Create orders with product_category dimension
        orders_df = pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4],
                "customer_id": [101, 102, 101, 103],
                "amount": [100.0, 200.0, 150.0, 300.0],
                "product_category": ["electronics", "books", "electronics", "books"],
            }
        )
        orders_tbl = (
            ecommerce_tables["orders"]
            ._find_backend()
            .create_table("orders_with_category", orders_df)
        )

        orders = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                category=lambda t: t.product_category,
            )
            .with_measures(
                total_revenue=lambda t: t.amount.sum(),
                avg_order_value=lambda t: t.amount.mean(),
                order_count=lambda t: t.order_id.count(),
            )
        )

        customers_tbl = ecommerce_tables["customers"]
        customers = (
            to_semantic_table(customers_tbl, name="customers")
            .with_measures(
                customer_count=lambda t: t.customer_id.count(),
            )
            .with_dimensions(
                country=lambda t: t.country,
            )
        )

        joined = orders.join_one(
            customers,
            lambda o, c: o.customer_id == c.customer_id,
        )

        # Check that dimensions are properly prefixed
        assert "orders.category" in joined.dimensions
        assert "customers.country" in joined.dimensions

        # Test aggregation with single dimension (the bug scenario)
        result = joined.group_by("orders.category").aggregate("orders.total_revenue").execute()

        assert "orders.category" in result.columns
        assert "orders.total_revenue" in result.columns
        # Electronics should have 250.0 total (100 + 150)
        electronics_revenue = result[result["orders.category"] == "electronics"][
            "orders.total_revenue"
        ].iloc[0]
        assert electronics_revenue == 250.0

    def test_join_one_with_multiple_dimensions_and_measures(self, ecommerce_tables):
        """Test join_one() with multiple dimensions and measures in aggregation."""
        orders_tbl = ecommerce_tables["orders"]
        customers_tbl = ecommerce_tables["customers"]

        # Create orders with product_category dimension
        orders_df = pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4],
                "customer_id": [101, 102, 101, 103],
                "amount": [100.0, 200.0, 150.0, 300.0],
                "product_category": ["electronics", "books", "electronics", "books"],
            }
        )
        orders_tbl = (
            ecommerce_tables["orders"]
            ._find_backend()
            .create_table("orders_with_category2", orders_df)
        )

        orders = (
            to_semantic_table(orders_tbl, name="orders")
            .with_dimensions(
                category=lambda t: t.product_category,
            )
            .with_measures(
                total_revenue=lambda t: t.amount.sum(),
                avg_order_value=lambda t: t.amount.mean(),
                order_count=lambda t: t.order_id.count(),
            )
        )

        customers = (
            to_semantic_table(customers_tbl, name="customers")
            .with_measures(
                customer_count=lambda t: t.customer_id.count(),
            )
            .with_dimensions(
                country=lambda t: t.country,
            )
        )

        joined = orders.join_one(
            customers,
            lambda o, c: o.customer_id == c.customer_id,
        )

        # Test with multiple dimensions and measures (the failing notebook case)
        result = joined.group_by("customers.country", "orders.category").aggregate(
            "orders.total_revenue",
            "orders.avg_order_value",
            "orders.order_count",
            "customers.customer_count",
        )

        executed = result.execute()

        assert "customers.country" in executed.columns
        assert "orders.category" in executed.columns
        assert "orders.total_revenue" in executed.columns
        assert "orders.avg_order_value" in executed.columns
        assert "orders.order_count" in executed.columns
        assert "customers.customer_count" in executed.columns


class TestChainedJoinsProjection:
    """Test that projection pushdown works correctly with chained joins."""

    def test_chained_joins_projection_pushdown(self, ecommerce_tables):
        """Test that only required columns are selected in chained joins."""
        orders_tbl = ecommerce_tables["orders"]
        customers_tbl = ecommerce_tables["customers"]
        order_items_tbl = ecommerce_tables["order_items"]

        # Add extra columns to test projection pushdown
        # Use different names to avoid collisions across tables
        orders_extra = orders_tbl.mutate(
            orders_extra1=1, orders_extra2=2, orders_extra3=3, orders_extra4=4, orders_extra5=5
        )
        customers_extra = customers_tbl.mutate(cust_extra1=10, cust_extra2=20, cust_extra3=30)
        items_extra = order_items_tbl.mutate(
            item_extra1=100, item_extra2=200, item_extra3=300, item_extra4=400
        )

        # Create semantic tables with only specific dimensions/measures
        orders_st = (
            to_semantic_table(orders_extra, "orders")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                order_id=lambda t: t.order_id,
            )
            .with_measures(revenue=lambda t: t.total_amount.sum())
        )

        customers_st = (
            to_semantic_table(customers_extra, "customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                country=lambda t: t.country,
            )
            .with_measures(customer_count=lambda t: t.count())
        )

        items_st = (
            to_semantic_table(items_extra, "items")
            .with_dimensions(order_id=lambda t: t.order_id)
            .with_measures(quantity_sold=lambda t: t.quantity.sum())
        )

        # Chain joins: orders -> customers -> items
        joined = orders_st.join_many(
            customers_st,
            lambda o, c: o.customer_id == c.customer_id,
        ).join_many(items_st, lambda oc, i: oc.order_id == i.order_id)

        # Query that only needs country and revenue
        result = joined.group_by("customers.country").aggregate("orders.revenue")
        # Verify the query still works
        df = result.execute()
        assert "customers.country" in df.columns
        assert "orders.revenue" in df.columns

    def test_chained_joins_with_all_tables_measures(self, ecommerce_tables):
        """Test chained joins when we need measures from all three tables."""
        orders_tbl = ecommerce_tables["orders"]
        customers_tbl = ecommerce_tables["customers"]
        order_items_tbl = ecommerce_tables["order_items"]

        # Add extra columns with unique names to avoid collisions
        orders_extra = orders_tbl.mutate(orders_extra1=1, orders_extra2=2)
        customers_extra = customers_tbl.mutate(cust_extra1=10, cust_extra2=20)
        items_extra = order_items_tbl.mutate(item_extra1=100, item_extra2=200)

        orders_st = (
            to_semantic_table(orders_extra, "orders")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                order_id=lambda t: t.order_id,
            )
            .with_measures(revenue=lambda t: t.total_amount.sum())
        )

        customers_st = (
            to_semantic_table(customers_extra, "customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                country=lambda t: t.country,
            )
            .with_measures(customer_count=lambda t: t.count())
        )

        items_st = (
            to_semantic_table(items_extra, "items")
            .with_dimensions(order_id=lambda t: t.order_id)
            .with_measures(
                quantity_sold=lambda t: t.quantity.sum(),
                item_revenue=lambda t: t.price.sum(),
            )
        )

        # Chain joins
        joined = orders_st.join_many(
            customers_st,
            lambda o, c: o.customer_id == c.customer_id,
        ).join_many(items_st, lambda oc, i: oc.order_id == i.order_id)

        # Query all measures from all tables
        result = joined.group_by("customers.country").aggregate(
            "orders.revenue",
            "customers.customer_count",
            "items.item_revenue",
        )

        # Verify query works
        df = result.execute()
        assert all(
            col in df.columns
            for col in [
                "customers.country",
                "orders.revenue",
                "customers.customer_count",
                "items.item_revenue",
            ]
        )

    def test_deeply_nested_chained_joins(self, con):
        """Test projection with 4-way chained joins."""
        # Create a chain: A -> B -> C -> D
        df_a = pd.DataFrame(
            {"a_id": [1, 2], "b_id": [10, 20], "a_val": [100, 200], "extra_a": [1, 2]}
        )
        df_b = pd.DataFrame(
            {"b_id": [10, 20], "c_id": [30, 40], "b_val": [300, 400], "extra_b": [3, 4]}
        )
        df_c = pd.DataFrame(
            {"c_id": [30, 40], "d_id": [50, 60], "c_val": [500, 600], "extra_c": [5, 6]}
        )
        df_d = pd.DataFrame({"d_id": [50, 60], "d_val": [700, 800], "extra_d": [7, 8]})

        tbl_a = con.create_table("tbl_a", df_a)
        tbl_b = con.create_table("tbl_b", df_b)
        tbl_c = con.create_table("tbl_c", df_c)
        tbl_d = con.create_table("tbl_d", df_d)

        st_a = (
            to_semantic_table(tbl_a, "a")
            .with_dimensions(a_id=lambda t: t.a_id, b_id=lambda t: t.b_id)
            .with_measures(a_sum=lambda t: t.a_val.sum())
        )

        st_b = (
            to_semantic_table(tbl_b, "b")
            .with_dimensions(b_id=lambda t: t.b_id, c_id=lambda t: t.c_id)
            .with_measures(b_sum=lambda t: t.b_val.sum())
        )

        st_c = (
            to_semantic_table(tbl_c, "c")
            .with_dimensions(c_id=lambda t: t.c_id, d_id=lambda t: t.d_id)
            .with_measures(c_sum=lambda t: t.c_val.sum())
        )

        st_d = (
            to_semantic_table(tbl_d, "d")
            .with_dimensions(d_id=lambda t: t.d_id)
            .with_measures(d_sum=lambda t: t.d_val.sum())
        )

        # Chain all four tables: A -> B -> C -> D
        joined = (
            st_a.join_many(st_b, lambda a, b: a.b_id == b.b_id)
            .join_many(st_c, lambda ab, c: ab.c_id == c.c_id)
            .join_many(st_d, lambda abc, d: abc.d_id == d.d_id)
        )

        # Query only needs measures from first and last table
        all_dims = list(joined._dims.keys())
        a_id_dim = [d for d in all_dims if "a_id" in d][0]

        result = joined.group_by(a_id_dim).aggregate("a.a_sum", "d.d_sum")

        # Verify query executes correctly
        df = result.execute()
        assert len(df) > 0


class TestEdgeCases:
    """Test edge cases and potential error scenarios."""

    def test_tables_with_auto_names_get_prefixed(self, con):
        """Test that tables with auto-detected names get prefixed."""
        df1 = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        df2 = pd.DataFrame({"id": [1, 2], "value": [30, 40]})

        tbl1 = con.create_table("tbl1", df1)
        tbl2 = con.create_table("tbl2", df2)

        # Create semantic tables without explicit names - names will be auto-detected from ibis
        st1 = to_semantic_table(tbl1).with_measures(count1=lambda t: t.count())
        st2 = to_semantic_table(tbl2).with_measures(count2=lambda t: t.count())

        joined = st1.join_many(st2, lambda a, b: a.id == b.id)

        # With auto-detected names (tbl1, tbl2), both measures should be prefixed
        measure_names = list(joined._base_measures.keys())
        assert any("count1" in m for m in measure_names)
        assert any("count2" in m for m in measure_names)

    def test_same_table_different_aliases(self, ecommerce_tables):
        """Test self-join with different table aliases."""
        orders_tbl = ecommerce_tables["orders"]

        # For self-joins, we need to create distinct table references using view()
        orders1 = (
            to_semantic_table(orders_tbl.view(), "orders_left")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                order_id=lambda t: t.order_id,
            )
            .with_measures(count_left=lambda t: t.count())
        )

        orders2 = (
            to_semantic_table(orders_tbl.view(), "orders_right")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                order_id=lambda t: t.order_id,
            )
            .with_measures(count_right=lambda t: t.count())
        )

        # Self-join on customer_id
        # Use bitwise & instead of 'and' for combining ibis expressions
        joined = orders1.join_many(
            orders2,
            on=lambda left, right: (left.order_id != right.order_id)
            & (left.customer_id == right.customer_id),
        )

        # Both measures should be accessible with their table aliases
        assert "orders_left.count_left" in joined._base_measures
        assert "orders_right.count_right" in joined._base_measures

        # Verify we can aggregate them
        result = (
            joined.group_by("orders_left.customer_id").aggregate("orders_left.count_left").execute()
        )
        # Alice (101) has 2 orders, so should have 2 self-join rows (1 paired with 2)
        assert len(result) > 0

    def test_percent_of_total_with_prefixed_measures(self, ecommerce_tables):
        """Test that t.all() works with prefixed measure names."""
        orders_tbl = ecommerce_tables["orders"]
        customers_tbl = ecommerce_tables["customers"]

        orders_st = (
            to_semantic_table(orders_tbl, "orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(revenue=lambda t: t.total_amount.sum())
        )

        customers_st = (
            to_semantic_table(customers_tbl, "customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                country=lambda t: t.country,
            )
            .with_measures(customer_count=lambda t: t.count())
        )

        joined = orders_st.join_many(
            customers_st,
            lambda o, c: o.customer_id == c.customer_id,
        )

        # Calculate percent of total revenue using prefixed measure (use bracket notation)
        result = (
            joined.with_measures(
                revenue_pct=lambda t: t["orders.revenue"] / t.all(t["orders.revenue"]),
            )
            .group_by("customers.country")
            .aggregate("revenue_pct")
            .execute()
        )

        assert "revenue_pct" in result.columns
        # Sum of percentages should be 1.0
        assert pytest.approx(result["revenue_pct"].sum(), abs=0.01) == 1.0

    def test_duplicate_column_names_in_joins(self, con):
        """Test that dimensions with duplicate column names resolve to correct table.

        Regression test for issue #132: When joining tables with columns that have
        the same name (e.g., 'city' in both aircraft and airports), dimension
        expressions should resolve to the correct column from the correct table.

        Ibis renames duplicate columns with '_right' suffix, and our dimension
        resolution logic must account for this.
        """
        # Create test data - aircraft and airports both have 'city' column
        aircraft_df = pd.DataFrame(
            {
                "tail_num": ["N123", "N456"],
                "city": ["FOREST HILLS", "ATLANTA"],  # Aircraft registrant city
            },
        )
        airports_df = pd.DataFrame(
            {
                "code": ["JFK", "ATL"],
                "city": ["NEW YORK", "ATLANTA"],  # Airport city
            },
        )
        flights_df = pd.DataFrame(
            {
                "origin": ["JFK", "ATL", "JFK"],
                "tail_num": ["N123", "N456", "N123"],
            },
        )

        aircraft_tbl = con.create_table("aircraft_dup_test", aircraft_df)
        airports_tbl = con.create_table("airports_dup_test", airports_df)
        flights_tbl = con.create_table("flights_dup_test", flights_df)

        # Create semantic tables
        aircraft = to_semantic_table(aircraft_tbl, "aircraft").with_dimensions(
            tail_num=lambda t: t.tail_num,
            city=lambda t: t.city,
        )
        airports = to_semantic_table(airports_tbl, "airports").with_dimensions(
            code=lambda t: t.code,
            city=lambda t: t.city,
        )
        flights = (
            to_semantic_table(flights_tbl, "flights")
            .with_dimensions(
                origin=lambda t: t.origin,
                tail_num=lambda t: t.tail_num,
            )
            .with_measures(flight_count=lambda t: t.count())
            .join_many(aircraft, lambda f, a: f.tail_num == a.tail_num)
            .join_many(airports, lambda f, ap: f.origin == ap.code)
        )

        # Query airports.city for JFK flights - should return NEW YORK, not FOREST HILLS
        result = (
            flights.filter(lambda t: t.origin == "JFK")
            .group_by("airports.city")
            .aggregate("flights.flight_count")
            .execute()
        )

        # Verify we get NEW YORK (from airports), not FOREST HILLS (from aircraft)
        assert "NEW YORK" in result["airports.city"].values
        ny_row = result[result["airports.city"] == "NEW YORK"]
        assert ny_row["flights.flight_count"].values[0] == 2

        # Also test aircraft.city to ensure both dimensions work correctly
        result_aircraft = (
            flights.filter(lambda t: t.origin == "JFK")
            .group_by("aircraft.city")
            .aggregate("flights.flight_count")
            .execute()
        )

        # Verify we get FOREST HILLS (from aircraft) for JFK flights
        assert "FOREST HILLS" in result_aircraft["aircraft.city"].values
        fh_row = result_aircraft[result_aircraft["aircraft.city"] == "FOREST HILLS"]
        assert fh_row["flights.flight_count"].values[0] == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
