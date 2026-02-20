#!/usr/bin/env python3
"""
Deferred API Example

Demonstrates using Deferred expressions (_) for referencing dimensions and
measures by name, for a more Pythonic, IDE-friendly workflow:

  - group_by: group_by(_.origin) instead of group_by("origin")
  - aggregate: aggregate(_.flight_count) instead of aggregate("flight_count")
  - Joins: join_one(other, on=_.customer_id)
  - Joined refs: group_by(_["customers.region"])
  - Mutate/order_by: mutate(ratio=_.total_revenue / _.order_count)
"""

import pandas as pd
import xorq.api as xo
from xorq.api import _

from boring_semantic_layer import to_semantic_table


def main():
    # -- data setup --------------------------------------------------------
    orders = xo.memtable(
        pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4, 5, 6],
                "customer_id": [10, 20, 10, 30, 20, 30],
                "amount": [50, 80, 120, 90, 60, 110],
                "quantity": [1, 2, 3, 1, 2, 4],
            }
        ),
        name="orders",
    )
    customers = xo.memtable(
        pd.DataFrame(
            {
                "customer_id": [10, 20, 30],
                "name": ["Alice", "Bob", "Carol"],
                "region": ["West", "East", "West"],
            }
        ),
        name="customers",
    )

    # -- semantic models using Deferred for measures & dimensions ----------
    orders_st = (
        to_semantic_table(orders, "orders")
        .with_dimensions(
            customer_id=_.customer_id,
        )
        .with_measures(
            total_revenue=_.amount.sum(),
            order_count=_.count(),
            total_qty=_.quantity.sum(),
        )
    )
    customers_st = (
        to_semantic_table(customers, "customers")
        .with_dimensions(
            customer_id=_.customer_id,
            name=_.name,
            region=_.region,
        )
    )

    # -- Example 1: Deferred in group_by and aggregate ---------------------
    print("=== Example 1: Deferred group_by / aggregate ===")
    df = (
        orders_st
        .group_by(_.customer_id)
        .aggregate(_.total_revenue, _.order_count)
        .execute()
    )
    print(df, "\n")

    # -- Example 2: Join using Deferred predicate --------------------------
    print("=== Example 2: Deferred join predicate ===")
    joined = orders_st.join_one(customers_st, on=_.customer_id)

    df = (
        joined
        .group_by(_["customers.region"])
        .aggregate(_["orders.total_revenue"], _["orders.order_count"])
        .execute()
    )
    print(df, "\n")

    # -- Example 3: Filter with Deferred -----------------------------------
    print("=== Example 3: Filter with Deferred ===")
    big_orders = joined.filter(_.amount > 70)
    df = (
        big_orders
        .group_by(_["customers.region"])
        .aggregate(_["orders.total_revenue"])
        .execute()
    )
    print(df, "\n")

    # -- Example 4: Mutate with Deferred -----------------------------------
    print("=== Example 4: Mutate (computed column) ===")
    df = (
        joined
        .group_by(_["customers.region"])
        .aggregate(_["orders.total_revenue"], _["orders.order_count"])
        .mutate(avg_order_value=lambda t: t["orders.total_revenue"] / t["orders.order_count"])
        .order_by(lambda t: t.avg_order_value.desc())
        .execute()
    )
    print(df, "\n")

    # -- Example 5: Mixing strings and Deferred ----------------------------
    print("=== Example 5: Mixed string / Deferred syntax ===")
    df = (
        joined
        .group_by(_["customers.name"], _["customers.region"])
        .aggregate("orders.total_revenue", _["orders.total_qty"])
        .order_by(lambda t: t["orders.total_revenue"].desc())
        .execute()
    )
    print(df)


if __name__ == "__main__":
    main()
