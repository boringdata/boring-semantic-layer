#!/usr/bin/env python3
"""
Test bracket-style filtering for joined and aggregated semantic tables.
"""

import ibis
import pandas as pd

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.api import aggregate_, group_by_, join_one


def test_bracket_filter_after_join_and_aggregate():
    """
    Test that bracket-style access works for prefixed dimensions after a
    join → group_by → aggregate → join chain.
    """
    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [101, 102, 103],
            "region": ["North", "South", "North"],
        },
    )
    products_df = pd.DataFrame(
        {
            "product_id": [1, 2, 3],
            "order_id": [1, 2, 3],
            "price": [100, 200, 150],
        },
    )
    customers_df = pd.DataFrame(
        {
            "customer_id": [101, 102, 103],
            "country": ["US", "UK", "US"],
        },
    )

    con = ibis.duckdb.connect(":memory:")
    orders_tbl = con.create_table("orders", orders_df)
    products_tbl = con.create_table("products", products_df)
    customers_tbl = con.create_table("customers", customers_df)

    model_a = (
        to_semantic_table(orders_tbl, name="orders")
        .with_dimensions(
            order_id=lambda t: t.order_id,
            customer_id=lambda t: t.customer_id,
            region=lambda t: t.region,
        )
        .with_measures(order_count=lambda t: t.count())
    )
    model_b = (
        to_semantic_table(products_tbl, name="products")
        .with_dimensions(
            product_id=lambda t: t.product_id,
            order_id=lambda t: t.order_id,
        )
        .with_measures(avg_price=lambda t: t.price.mean())
    )
    model_c = to_semantic_table(customers_tbl, name="customers").with_dimensions(
        customer_id=lambda t: t.customer_id,
        country=lambda t: t.country,
    )

    step1 = join_one(model_a, model_b, lambda a, b: a.order_id == b.order_id)
    step2 = aggregate_(
        group_by_(step1, "orders.region", "orders.customer_id"),
        lambda t: t["orders.order_count"],
    )
    final = join_one(step2, model_c, lambda s, c: s["orders.customer_id"] == c.customer_id)

    df = final.filter(lambda t: t["orders.region"] == "North").execute()
    assert df.shape[0] == 2


def test_filter_before_aggregation_on_joined_table():
    """
    Test that filters applied before aggregation work correctly on joined tables.

    This is the critical test case that was missing - it tests:
    join → filter → group_by → aggregate

    This pattern exposed a bug where SemanticAggregateOp.to_untagged() would skip
    directly to the join for projection pushdown, bypassing the filter.
    """
    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4, 5],
            "customer_id": [101, 102, 101, 103, 102],
            "amount": [100, 200, 150, 300, 250],
        },
    )
    customers_df = pd.DataFrame(
        {
            "customer_id": [101, 102, 103],
            "name": ["Alice", "Bob", "Charlie"],
            "country": ["US", "UK", "US"],
        },
    )

    con = ibis.duckdb.connect(":memory:")
    orders_tbl = con.create_table("orders", orders_df, overwrite=True)
    customers_tbl = con.create_table("customers", customers_df, overwrite=True)

    orders_sm = (
        to_semantic_table(orders_tbl, name="orders")
        .with_dimensions(
            order_id=lambda t: t.order_id,
            customer_id=lambda t: t.customer_id,
        )
        .with_measures(
            total_amount=lambda t: t.amount.sum(),
            order_count=lambda t: t.count(),
        )
    )

    customers_sm = (
        to_semantic_table(customers_tbl, name="customers")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
            name=lambda t: t.name,
            country=lambda t: t.country,
        )
        .with_measures(customer_count=lambda t: t.count())
    )

    # Join orders with customers
    joined = join_one(orders_sm, customers_sm, lambda o, c: o.customer_id == c.customer_id)

    # Filter THEN aggregate - this is the critical pattern that was broken
    filtered = joined.filter(lambda t: t.country == "US")
    aggregated = aggregate_(
        group_by_(filtered, "customers.name"),
        lambda t: t["orders.total_amount"],
    )

    result = aggregated.execute()

    # Should only include US customers (Alice and Charlie), not Bob (UK)
    assert len(result) == 2
    assert set(result["customers.name"]) == {"Alice", "Charlie"}

    # Verify the amounts are correct
    # Note: aggregate_() with callable creates a generated column name
    measure_col = [col for col in result.columns if col != "customers.name"][0]
    alice_total = result[result["customers.name"] == "Alice"][measure_col].iloc[0]
    charlie_total = result[result["customers.name"] == "Charlie"][measure_col].iloc[0]
    assert alice_total == 250  # Orders 1 + 3
    assert charlie_total == 300  # Order 4
