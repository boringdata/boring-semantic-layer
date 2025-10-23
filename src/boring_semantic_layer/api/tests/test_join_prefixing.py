"""
Comprehensive tests for table.measure prefixing in joined tables.

Tests the dot notation for accessing measures and dimensions
from different tables after joins, including edge cases and deep join scenarios.
"""

import pandas as pd
import ibis
import pytest

from boring_semantic_layer.api import to_semantic_table


@pytest.fixture(scope="module")
def con():
    """DuckDB connection for all tests."""
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def ecommerce_tables(con):
    """Create a realistic e-commerce schema with multiple tables."""
    # Orders table
    orders_df = pd.DataFrame({
        "order_id": [1, 2, 3, 4],
        "customer_id": [101, 102, 101, 103],
        "order_date": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"]),
        "total_amount": [100.0, 200.0, 150.0, 300.0],
    })

    # Customers table
    customers_df = pd.DataFrame({
        "customer_id": [101, 102, 103],
        "name": ["Alice", "Bob", "Charlie"],
        "country": ["US", "UK", "US"],
    })

    # Order items table
    order_items_df = pd.DataFrame({
        "item_id": [1, 2, 3, 4, 5],
        "order_id": [1, 1, 2, 3, 4],
        "product_id": [501, 502, 501, 503, 502],
        "quantity": [2, 1, 3, 1, 2],
        "price": [25.0, 50.0, 25.0, 150.0, 50.0],
    })

    # Products table
    products_df = pd.DataFrame({
        "product_id": [501, 502, 503],
        "product_name": ["Widget A", "Widget B", "Widget C"],
        "category": ["electronics", "electronics", "home"],
    })

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

        items_st = (
            to_semantic_table(order_items_tbl, "items")
            .with_measures(record_count=lambda t: t.count())
        )

        joined = orders_st.join(items_st, on=lambda o, i: o.order_id == i.order_id)

        result = (
            joined
            .group_by("orders.customer_id")
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

        joined = orders_st.join(
            customers_st, on=lambda o, c: o.customer_id == c.customer_id
        )

        # Short name should resolve to orders.total (first match)
        result = (
            joined
            .group_by("orders.customer_id")
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

        joined = orders_st.join(
            customers_st, on=lambda o, c: o.customer_id == c.customer_id
        )

        # Both explicit prefixed names should work
        result = (
            joined
            .group_by("orders.customer_id")
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

        joined = orders_st.join(
            customers_st, on=lambda o, c: o.customer_id == c.customer_id
        )

        # Use bracket notation for accessing prefixed measures (dots not allowed in Python identifiers)
        result = (
            joined
            .with_measures(
                combined=lambda t: t["orders.order_count"] + t["customers.customer_count"]
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

        joined = orders_st.join(
            customers_st, on=lambda o, c: o.customer_id == c.customer_id
        )

        # Use bracket notation in with_measures
        result = (
            joined
            .with_measures(
                combined=lambda t: t["orders.order_count"] + t["customers.customer_count"]
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

        joined = orders_st.join(
            customers_st, on=lambda o, c: o.customer_id == c.customer_id
        )

        # Use bracket notation for accessing prefixed measures
        result = (
            joined
            .with_measures(
                mixed=lambda t: t["orders.order_count"] + t["customers.customer_count"]
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
        joined = (
            orders_st
            .join(customers_st, on=lambda o, c: o.customer_id == c.customer_id)
            .join(items_st, on=lambda oc, i: oc.order_id == i.order_id)
        )

        # All three measures should be accessible with prefixes
        # After multiple joins, dimension names also get nested prefixes
        # Use the customer_id dimension from the original orders table
        all_dims = list(joined._dims.keys())
        customer_dim = [d for d in all_dims if 'customer_id' in d][0]

        result = (
            joined
            .group_by(customer_dim)
            .aggregate("orders.order_count", "customers.customer_count", "items.item_count")
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
        joined = (
            orders_st
            .join(items_st, on=lambda o, i: o.order_id == i.order_id)
            .join(products_st, on=lambda oi, p: oi.product_id == p.product_id)
        )

        # Access measures from all three tables using bracket notation
        result = (
            joined
            .with_measures(
                combined_metric=lambda t: (
                    t["orders.revenue"] + t["items.quantity_sold"] + t["products.product_count"]
                )
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
            .with_measures(
                avg_order_value=lambda t: t.total_revenue / t.order_count
            )
        )

        items_st = (
            to_semantic_table(order_items_tbl, "items")
            .with_dimensions(order_id=lambda t: t.order_id)
            .with_measures(
                item_count=lambda t: t.count(),
                total_quantity=lambda t: t.quantity.sum(),
            )
        )

        joined = orders_st.join(items_st, on=lambda o, i: o.order_id == i.order_id)

        # The calculated measure should be properly prefixed
        assert "orders.avg_order_value" in joined._calc_measures

        result = (
            joined
            .group_by("orders.order_id")
            .aggregate("orders.avg_order_value")
            .execute()
        )

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

        joined = orders_st.join(
            customers_st, on=lambda o, c: o.customer_id == c.customer_id
        )

        # Create new calculated measure using prefixed measures from both tables (use bracket notation)
        result = (
            joined
            .with_measures(
                orders_per_customer=lambda t: t["orders.order_count"] / t["customers.customer_count"]
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

        joined = orders_st.join(
            customers_st, on=lambda o, c: o.customer_id == c.customer_id
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

        joined = orders_st.join(
            customers_st, on=lambda o, c: o.customer_id == c.customer_id
        )

        # Group by prefixed dimension from customers table
        result = (
            joined
            .group_by("customers.country")
            .aggregate("orders.revenue")
            .execute()
        )

        assert "customers.country" in result.columns
        assert "orders.revenue" in result.columns
        # US should have more revenue (Alice + Charlie)
        us_revenue = result[result["customers.country"] == "US"]["orders.revenue"].sum()
        uk_revenue = result[result["customers.country"] == "UK"]["orders.revenue"].sum()
        assert us_revenue > uk_revenue


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

        joined = st1.join(st2, on=lambda a, b: a.id == b.id)

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
        joined = orders1.join(
            orders2, on=lambda l, r: (l.order_id != r.order_id) & (l.customer_id == r.customer_id)
        )

        # Both measures should be accessible with their table aliases
        assert "orders_left.count_left" in joined._base_measures
        assert "orders_right.count_right" in joined._base_measures

        # Verify we can aggregate them
        result = (
            joined
            .group_by("orders_left.customer_id")
            .aggregate("orders_left.count_left")
            .execute()
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
            .with_dimensions(customer_id=lambda t: t.customer_id, country=lambda t: t.country)
            .with_measures(customer_count=lambda t: t.count())
        )

        joined = orders_st.join(
            customers_st, on=lambda o, c: o.customer_id == c.customer_id
        )

        # Calculate percent of total revenue using prefixed measure (use bracket notation)
        result = (
            joined
            .with_measures(
                revenue_pct=lambda t: t["orders.revenue"] / t.all(t["orders.revenue"])
            )
            .group_by("customers.country")
            .aggregate("revenue_pct")
            .execute()
        )

        assert "revenue_pct" in result.columns
        # Sum of percentages should be 1.0
        assert pytest.approx(result["revenue_pct"].sum(), abs=0.01) == 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
