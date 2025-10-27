"""
Tests demonstrating projection pushdown optimization can be toggled via config.

These tests show the SQL generated before and after enabling projection pushdown,
highlighting the column filtering benefits of the optimization.
"""

import contextlib

import ibis
import pytest

from boring_semantic_layer import options, to_ibis, to_semantic_table


@pytest.fixture(scope="module")
def wide_table(ibis_con):
    """Create a wide table with many unused columns."""
    # Create a table with 10 columns, but we'll only use 2 in our queries
    data = {
        "customer_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "phone": ["555-0001", "555-0002", "555-0003"],
        "address": ["123 Main St", "456 Oak Ave", "789 Pine Rd"],
        "city": ["New York", "Los Angeles", "Chicago"],
        "state": ["NY", "CA", "IL"],
        "zipcode": ["10001", "90001", "60601"],
        "country": ["USA", "USA", "USA"],
        "total_orders": [10, 20, 15],
    }
    tbl = ibis.memtable(data)
    # Drop table if it exists from a previous test
    with contextlib.suppress(Exception):
        ibis_con.drop_table("wide_customers", force=True)
    ibis_con.create_table("wide_customers", tbl.execute())
    return ibis_con.table("wide_customers")


@pytest.fixture(scope="module")
def orders_table(ibis_con):
    """Create orders table."""
    data = {
        "order_id": [1, 2, 3, 4, 5],
        "customer_id": [1, 1, 2, 3, 3],
        "order_date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
        "amount": [100.0, 150.0, 200.0, 75.0, 125.0],
        "status": ["completed", "completed", "pending", "completed", "completed"],
    }
    tbl = ibis.memtable(data)
    # Drop table if it exists from a previous test
    with contextlib.suppress(Exception):
        ibis_con.drop_table("orders", force=True)
    ibis_con.create_table("orders", tbl.execute())
    return ibis_con.table("orders")


class TestProjectionPushdownConfig:
    """Test projection pushdown optimization can be toggled via global config."""

    def test_projection_pushdown_disabled(self, wide_table, orders_table):
        """Test SQL generation with projection pushdown DISABLED."""
        # Disable projection pushdown optimization
        original_value = options.rewrites.enable_projection_pushdown
        try:
            options.rewrites.enable_projection_pushdown = False

            # Create semantic tables - customers has 10 columns but we only use customer_id and name
            customers = to_semantic_table(wide_table, name="customers").with_dimensions(
                customer_id=lambda t: t.customer_id,
                name=lambda t: t.name,
            )

            orders = (
                to_semantic_table(orders_table, name="orders")
                .with_dimensions(
                    order_id=lambda t: t.order_id,
                    customer_id=lambda t: t.customer_id,
                )
                .with_measures(
                    total_amount=lambda t: t.amount.sum(),
                )
            )

            # Join and query - only using customer_id, name, and total_amount
            joined = customers.join(orders, lambda c, o: c.customer_id == o.customer_id)
            result = joined.group_by("customers.customer_id", "customers.name").aggregate(
                "total_amount"
            )

            # Generate SQL
            sql = str(ibis.to_sql(to_ibis(result)))

            print("\n" + "=" * 80)
            print("SQL WITH PROJECTION PUSHDOWN DISABLED:")
            print("=" * 80)
            print(sql)
            print("=" * 80)

            # Without optimization, ALL columns from customers table should be in SQL
            # even though we only use customer_id and name
            assert "email" in sql.lower()
            assert "phone" in sql.lower()
            assert "address" in sql.lower()
            assert "city" in sql.lower()
            assert "state" in sql.lower()
            assert "zipcode" in sql.lower()
            assert "country" in sql.lower()
            assert "total_orders" in sql.lower()

            # The columns we actually use should also be present
            assert "customer_id" in sql.lower()
            assert "name" in sql.lower()
            assert "amount" in sql.lower()  # Used in measure

        finally:
            # Restore original value
            options.rewrites.enable_projection_pushdown = original_value

    def test_projection_pushdown_enabled(self, wide_table, orders_table):
        """Test SQL generation with projection pushdown ENABLED."""
        # Enable projection pushdown optimization (default)
        original_value = options.rewrites.enable_projection_pushdown
        try:
            options.rewrites.enable_projection_pushdown = True

            # Create semantic tables - customers has 10 columns but we only use customer_id and name
            customers = to_semantic_table(wide_table, name="customers").with_dimensions(
                customer_id=lambda t: t.customer_id,
                name=lambda t: t.name,
            )

            orders = (
                to_semantic_table(orders_table, name="orders")
                .with_dimensions(
                    order_id=lambda t: t.order_id,
                    customer_id=lambda t: t.customer_id,
                )
                .with_measures(
                    total_amount=lambda t: t.amount.sum(),
                )
            )

            # Join and query - only using customer_id, name, and total_amount
            joined = customers.join(orders, lambda c, o: c.customer_id == o.customer_id)
            result = joined.group_by("customers.customer_id", "customers.name").aggregate(
                "total_amount"
            )

            # Generate SQL
            sql = str(ibis.to_sql(to_ibis(result)))

            print("\n" + "=" * 80)
            print("SQL WITH PROJECTION PUSHDOWN ENABLED:")
            print("=" * 80)
            print(sql)
            print("=" * 80)

            # With optimization, ONLY used columns should be in SQL
            # These unused columns should NOT appear:
            assert "email" not in sql.lower()
            assert "phone" not in sql.lower()
            assert "address" not in sql.lower()
            assert "city" not in sql.lower()
            assert "state" not in sql.lower()
            assert "zipcode" not in sql.lower()
            assert "country" not in sql.lower()
            assert "total_orders" not in sql.lower()

            # The columns we actually use SHOULD be present
            assert "customer_id" in sql.lower()
            assert "name" in sql.lower()
            assert "amount" in sql.lower()  # Used in measure

        finally:
            # Restore original value
            options.rewrites.enable_projection_pushdown = original_value

    def test_before_after_comparison(self, wide_table, orders_table):
        """Side-by-side comparison of SQL before and after optimization."""
        original_value = options.rewrites.enable_projection_pushdown

        try:
            # Create semantic tables
            customers = to_semantic_table(wide_table, name="customers").with_dimensions(
                customer_id=lambda t: t.customer_id,
                name=lambda t: t.name,
            )

            orders = (
                to_semantic_table(orders_table, name="orders")
                .with_dimensions(
                    order_id=lambda t: t.order_id,
                    customer_id=lambda t: t.customer_id,
                )
                .with_measures(
                    total_amount=lambda t: t.amount.sum(),
                )
            )

            joined = customers.join(orders, lambda c, o: c.customer_id == o.customer_id)
            result = joined.group_by("customers.customer_id", "customers.name").aggregate(
                "total_amount"
            )

            # Get SQL WITHOUT optimization
            options.rewrites.enable_projection_pushdown = False
            sql_before = str(ibis.to_sql(to_ibis(result)))

            # Get SQL WITH optimization
            options.rewrites.enable_projection_pushdown = True
            sql_after = str(ibis.to_sql(to_ibis(result)))

            print("\n" + "=" * 80)
            print("BEFORE OPTIMIZATION (All columns selected):")
            print("=" * 80)
            print(sql_before)
            print("\n" + "=" * 80)
            print("AFTER OPTIMIZATION (Only required columns selected):")
            print("=" * 80)
            print(sql_after)
            print("\n" + "=" * 80)
            print("BENEFIT: Reduced columns scanned from wide_customers table")
            print(
                "  - Before: 10 columns (customer_id, name, email, phone, address, city, state, zipcode, country, total_orders)"
            )
            print("  - After:  2 columns (customer_id, name)")
            print("  - Savings: 80% fewer columns scanned!")
            print("=" * 80)

            # Count unused columns in before vs after
            unused_columns = [
                "email",
                "phone",
                "address",
                "city",
                "state",
                "zipcode",
                "country",
                "total_orders",
            ]

            unused_in_before = sum(1 for col in unused_columns if col in sql_before.lower())
            unused_in_after = sum(1 for col in unused_columns if col in sql_after.lower())

            assert unused_in_before == 8, "All 8 unused columns should appear without optimization"
            assert unused_in_after == 0, "No unused columns should appear with optimization"

        finally:
            options.rewrites.enable_projection_pushdown = original_value

    def test_config_can_be_toggled_mid_session(self, wide_table, orders_table):
        """Test that config can be toggled on/off during a session."""
        original_value = options.rewrites.enable_projection_pushdown

        try:
            # Create semantic tables once
            customers = to_semantic_table(wide_table, name="customers").with_dimensions(
                customer_id=lambda t: t.customer_id,
                name=lambda t: t.name,
            )
            orders = (
                to_semantic_table(orders_table, name="orders")
                .with_dimensions(
                    order_id=lambda t: t.order_id,
                    customer_id=lambda t: t.customer_id,
                )
                .with_measures(
                    total_amount=lambda t: t.amount.sum(),
                )
            )

            # First query with optimization ON
            options.rewrites.enable_projection_pushdown = True
            joined1 = customers.join(orders, lambda c, o: c.customer_id == o.customer_id)
            result1 = joined1.group_by("customers.customer_id").aggregate("total_amount")
            sql1 = str(ibis.to_sql(to_ibis(result1)))

            # Second query with optimization OFF
            options.rewrites.enable_projection_pushdown = False
            joined2 = customers.join(orders, lambda c, o: c.customer_id == o.customer_id)
            result2 = joined2.group_by("customers.customer_id").aggregate("total_amount")
            sql2 = str(ibis.to_sql(to_ibis(result2)))

            # Verify different SQL was generated
            assert sql1 != sql2, "SQL should differ based on config setting"

            # Verify optimization behavior
            assert "email" not in sql1.lower(), (
                "Query with optimization should filter unused columns"
            )
            assert "email" in sql2.lower(), "Query without optimization should include all columns"

        finally:
            options.rewrites.enable_projection_pushdown = original_value
