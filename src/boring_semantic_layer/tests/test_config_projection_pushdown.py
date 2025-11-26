"""
Tests demonstrating projection pushdown optimization is always enabled.

These tests show the SQL generated with projection pushdown,
highlighting the column filtering benefits of the optimization.

NOTE: Projection pushdown has been disabled for xorq compatibility.
These tests are marked as xfail to document the expected behavior.
"""

import contextlib

import ibis
import pytest

from boring_semantic_layer import to_semantic_table, to_untagged

# Projection pushdown disabled for xorq compatibility
pytestmark = pytest.mark.xfail(
    reason="Projection pushdown disabled for xorq vendored ibis compatibility"
)


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


class TestProjectionPushdown:
    """Test projection pushdown optimization is always enabled."""

    def test_projection_pushdown_filters_unused_columns(self, wide_table, orders_table):
        """Test SQL generation with projection pushdown - unused columns should be filtered."""
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
        sql = str(ibis.to_sql(to_untagged(result)))

        print("\n" + "=" * 80)
        print("SQL WITH PROJECTION PUSHDOWN:")
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

    def test_projection_pushdown_multiple_tables(self, wide_table, orders_table):
        """Test projection pushdown works across multiple tables in a join."""
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

        # Generate SQL
        sql = str(ibis.to_sql(to_untagged(result)))

        print("\n" + "=" * 80)
        print("PROJECTION PUSHDOWN - MULTIPLE TABLES:")
        print("=" * 80)
        print(sql)
        print("=" * 80)

        # Count unused columns that appear in SQL
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

        unused_in_sql = sum(1 for col in unused_columns if col in sql.lower())
        assert unused_in_sql == 0, "No unused columns should appear with projection pushdown"

        # Verify we reduced the columns scanned
        print(
            "\nBENEFIT: Reduced columns scanned from wide_customers table"
            "\n  - Total columns: 10 (customer_id, name, email, phone, address, city, state, zipcode, country, total_orders)"
            "\n  - Used columns:  2 (customer_id, name)"
            "\n  - Savings: 80% fewer columns scanned!"
        )
