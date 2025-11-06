"""Test for issue #43: Join methods should support dimension names.

When a dimension is renamed (semantic name != column name), join methods
should resolve the dimension name to the underlying column.
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture
def tables():
    """Create test tables with mismatched dimension/column names."""
    con = ibis.duckdb.connect(":memory:")

    customers_df = pd.DataFrame(
        {
            "cust_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )

    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [1, 2, 1],
            "amount": [100, 200, 150],
        }
    )

    return {
        "customers": con.create_table("customers", customers_df),
        "orders": con.create_table("orders", orders_df),
    }


def test_join_one_with_renamed_dimension(tables):
    """Test that join_one resolves dimension names when they differ from columns."""
    customers = to_semantic_table(tables["customers"], "customers").with_dimensions(
        customer_id=lambda t: t.cust_id,  # Renamed: cust_id -> customer_id
        name=lambda t: t.name,
    )

    orders = (
        to_semantic_table(tables["orders"], "orders")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
        )
        .with_measures(
            revenue=lambda t: t.amount.sum(),
        )
    )

    # This should work - dimension names should be resolved
    joined = orders.join_one(customers, left_on="customer_id", right_on="customer_id")
    result = joined.group_by("customers.name").aggregate("orders.revenue").execute()

    assert len(result) == 2
    assert "customers.name" in result.columns
    assert "orders.revenue" in result.columns


def test_join_many_with_renamed_dimension(tables):
    """Test that join_many resolves dimension names when they differ from columns."""
    customers = to_semantic_table(tables["customers"], "customers").with_dimensions(
        customer_id=lambda t: t.cust_id,  # Renamed: cust_id -> customer_id
    )

    orders = (
        to_semantic_table(tables["orders"], "orders")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
        )
        .with_measures(
            order_count=lambda t: t.count(),
        )
    )

    # This should work - dimension names should be resolved
    joined = customers.join_many(orders, left_on="customer_id", right_on="customer_id")
    result = joined.group_by("customers.customer_id").aggregate("orders.order_count").execute()

    assert len(result) == 2
    assert "customers.customer_id" in result.columns
    assert "orders.order_count" in result.columns


def test_flexible_join_with_renamed_dimension(tables):
    """Test that flexible join() resolves dimension names in lambda."""
    customers = to_semantic_table(tables["customers"], "customers").with_dimensions(
        customer_id=lambda t: t.cust_id,  # Renamed: cust_id -> customer_id
        name=lambda t: t.name,
    )

    orders = (
        to_semantic_table(tables["orders"], "orders")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
        )
        .with_measures(
            revenue=lambda t: t.amount.sum(),
        )
    )

    # This should work - dimension names should be resolved via _Resolver
    joined = orders.join(
        customers,
        on=lambda o, c: o.customer_id == c.customer_id,
    )
    result = joined.group_by("customers.name").aggregate("orders.revenue").execute()

    assert len(result) == 2
    assert "customers.name" in result.columns
    assert "orders.revenue" in result.columns
