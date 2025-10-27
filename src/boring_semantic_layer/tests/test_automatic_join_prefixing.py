"""
Tests for automatic column prefixing in joins using lname/rname parameters.

This tests the Ibis-level column renaming to avoid collisions during joins,
ensuring that columns are automatically prefixed based on semantic table names.
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
def test_tables(con):
    """Create test tables with overlapping column names."""
    # Orders table
    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [101, 102, 101],
            "amount": [100.0, 200.0, 150.0],
        },
    )

    # Customers table (shares customer_id with orders)
    customers_df = pd.DataFrame(
        {
            "customer_id": [101, 102, 103],
            "name": ["Alice", "Bob", "Charlie"],
            "country": ["US", "UK", "US"],
        },
    )

    # Items table (shares order_id with orders)
    items_df = pd.DataFrame(
        {
            "item_id": [1, 2, 3, 4],
            "order_id": [1, 1, 2, 3],
            "product_id": [501, 502, 501, 503],
            "quantity": [2, 1, 3, 1],
        },
    )

    return {
        "orders": con.create_table("orders", orders_df),
        "customers": con.create_table("customers", customers_df),
        "items": con.create_table("items", items_df),
    }


class TestAutomaticColumnPrefixing:
    """Test automatic column prefixing using semantic table names."""

    def test_single_join_auto_prefix(self, test_tables):
        """Test that a single join automatically prefixes right table columns when requested."""
        orders_st = to_semantic_table(test_tables["orders"], name="orders")
        customers_st = to_semantic_table(test_tables["customers"], name="customers")

        # Join with explicit lname="" to trigger automatic prefixing
        joined = orders_st.join(
            customers_st,
            on=lambda o, c: o.customer_id == c.customer_id,
            lname="",  # Explicitly enable Ibis-level prefixing
        )

        result = joined.to_ibis()
        columns = result.columns

        # Left table columns should be unprefixed
        assert "order_id" in columns
        assert "customer_id" in columns
        assert "amount" in columns

        # Right table columns should be prefixed with "customers_"
        assert "customers_customer_id" in columns
        assert "customers_name" in columns
        assert "customers_country" in columns

        # Should not have unprefixed duplicates from right
        assert columns.count("customer_id") == 1  # Only from left

    def test_multiple_joins_no_collision(self, test_tables):
        """Test that multiple joins don't create column name collisions."""
        orders_st = to_semantic_table(test_tables["orders"], name="orders")
        customers_st = to_semantic_table(test_tables["customers"], name="customers")
        items_st = to_semantic_table(test_tables["items"], name="items")

        # First join
        joined1 = orders_st.join(
            customers_st,
            on=lambda o, c: o.customer_id == c.customer_id,
            lname="",  # Enable Ibis-level prefixing
        )

        # Second join - order_id exists in both orders and items
        joined2 = joined1.join(
            items_st,
            on=lambda oc, i: oc.order_id == i.order_id,
            lname="",  # Enable Ibis-level prefixing
        )

        result = joined2.to_ibis()
        columns = result.columns

        # Verify no duplicate column names
        assert len(columns) == len(set(columns)), f"Duplicate columns found: {columns}"

        # First table (orders) columns unprefixed
        assert "order_id" in columns
        assert "customer_id" in columns

        # Second join (customers) columns prefixed
        assert "customers_customer_id" in columns
        assert "customers_name" in columns

        # Third join (items) columns prefixed with items_
        assert "items_order_id" in columns
        assert "items_item_id" in columns
        assert "items_product_id" in columns

    def test_custom_lname_rname(self, test_tables):
        """Test that custom lname/rname parameters work correctly."""
        orders_st = to_semantic_table(test_tables["orders"], name="orders")
        customers_st = to_semantic_table(test_tables["customers"], name="customers")

        # Use custom prefixes
        joined = orders_st.join(
            customers_st,
            on=lambda o, c: o.customer_id == c.customer_id,
            lname="o_{name}",
            rname="c_{name}",
        )

        result = joined.to_ibis()
        columns = result.columns

        # Left table columns prefixed with "o_"
        assert "o_order_id" in columns
        assert "o_customer_id" in columns
        assert "o_amount" in columns

        # Right table columns prefixed with "c_"
        assert "c_customer_id" in columns
        assert "c_name" in columns
        assert "c_country" in columns

    def test_join_without_semantic_name_uses_fallback(self, con):
        """Test that joins work even when semantic tables don't have explicit names."""
        df1 = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        df2 = pd.DataFrame({"id": [1, 2], "value": [30, 40]})

        tbl1 = con.create_table("tbl1", df1)
        tbl2 = con.create_table("tbl2", df2)

        # Create semantic tables without explicit names
        # They will auto-derive names from Ibis table names
        st1 = to_semantic_table(tbl1)
        st2 = to_semantic_table(tbl2)

        joined = st1.join(st2, on=lambda a, b: a.id == b.id, lname="")
        result = joined.to_ibis()
        columns = result.columns

        # Left table columns should be unprefixed
        assert "id" in columns
        assert "value" in columns

        # Right table columns should have some prefix (either tbl2 or full qualified name)
        # to avoid collision with left table
        prefixed_columns = [col for col in columns if col not in ["id", "value"]]
        assert len(prefixed_columns) == 2  # id and value from right table with prefixes

        # Verify no duplicate columns
        assert len(columns) == len(set(columns))

    def test_join_one_with_auto_prefix(self, test_tables):
        """Test join_one method with automatic prefixing."""
        orders_st = to_semantic_table(test_tables["orders"], name="orders")
        customers_st = to_semantic_table(test_tables["customers"], name="customers")

        joined = orders_st.join_one(customers_st, "customer_id", "customer_id", lname="")
        result = joined.to_ibis()
        columns = result.columns

        # Verify automatic prefixing works with join_one
        assert "customer_id" in columns
        assert "customers_customer_id" in columns
        assert "customers_name" in columns

    def test_join_many_with_auto_prefix(self, test_tables):
        """Test join_many method with automatic prefixing."""
        orders_st = to_semantic_table(test_tables["orders"], name="orders")
        items_st = to_semantic_table(test_tables["items"], name="items")

        joined = orders_st.join_many(items_st, "order_id", "order_id", lname="")
        result = joined.to_ibis()
        columns = result.columns

        # Verify automatic prefixing works with join_many
        assert "order_id" in columns
        assert "items_order_id" in columns
        assert "items_item_id" in columns

    def test_cross_join_with_auto_prefix(self, test_tables):
        """Test cross join with automatic prefixing."""
        orders_st = to_semantic_table(test_tables["orders"], name="orders")
        customers_st = to_semantic_table(test_tables["customers"], name="customers")

        joined = orders_st.join_cross(customers_st, lname="")
        result = joined.to_ibis()
        columns = result.columns

        # Cross join should also prefix columns
        assert "customer_id" in columns
        assert "customers_customer_id" in columns

    def test_execute_joined_table(self, test_tables):
        """Test that joined tables can be executed successfully."""
        orders_st = to_semantic_table(test_tables["orders"], name="orders")
        customers_st = to_semantic_table(test_tables["customers"], name="customers")

        joined = orders_st.join(
            customers_st,
            on=lambda o, c: o.customer_id == c.customer_id,
            lname="",
        )

        # Should execute without errors
        result_df = joined.execute()

        # Verify data integrity
        assert len(result_df) == 3  # 3 orders matched with customers
        assert "customer_id" in result_df.columns
        assert "customers_name" in result_df.columns

        # Verify Alice appears twice (has 2 orders)
        alice_rows = result_df[result_df["customers_name"] == "Alice"]
        assert len(alice_rows) == 2

    def test_triple_join_unique_columns(self, test_tables):
        """Test that three-way joins maintain unique column names."""
        orders_st = to_semantic_table(test_tables["orders"], name="orders")
        customers_st = to_semantic_table(test_tables["customers"], name="customers")
        items_st = to_semantic_table(test_tables["items"], name="items")

        # Chain three joins
        result = (
            orders_st.join(
                customers_st,
                on=lambda o, c: o.customer_id == c.customer_id,
                lname="",
            )
            .join(
                items_st,
                on=lambda oc, i: oc.order_id == i.order_id,
                lname="",
            )
            .execute()
        )

        columns = list(result.columns)

        # All column names should be unique
        assert len(columns) == len(set(columns)), f"Found duplicate columns: {columns}"

        # Verify key columns exist with proper prefixes
        assert "order_id" in columns
        assert "customer_id" in columns
        assert "customers_customer_id" in columns
        assert "items_order_id" in columns

    def test_custom_prefix_per_join(self, test_tables):
        """Test using different custom prefixes for each join in a chain."""
        orders_st = to_semantic_table(test_tables["orders"], name="orders")
        customers_st = to_semantic_table(test_tables["customers"], name="customers")
        items_st = to_semantic_table(test_tables["items"], name="items")

        # Use custom prefixes for each join
        result = (
            orders_st.join(
                customers_st,
                on=lambda o, c: o.customer_id == c.customer_id,
                lname="",
                rname="cust_{name}",
            )
            .join(
                items_st,
                on=lambda oc, i: oc.order_id == i.order_id,
                lname="",
                rname="item_{name}",
            )
            .to_ibis()
        )

        columns = result.columns

        # Verify custom prefixes were used
        assert "cust_customer_id" in columns
        assert "cust_name" in columns
        assert "item_order_id" in columns
        assert "item_item_id" in columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
