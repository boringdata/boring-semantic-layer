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

    step1 = join_one(model_a, model_b, "order_id", "order_id")
    step2 = aggregate_(
        group_by_(step1, "orders.region", "orders.customer_id"),
        lambda t: t["orders.order_count"],
    )
    final = join_one(step2, model_c, "orders.customer_id", "customer_id")

    df = final.filter(lambda t: t["orders.region"] == "North").execute()
    assert df.shape[0] == 2
