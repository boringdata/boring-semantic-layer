#!/usr/bin/env python3
"""Fan Traps and Chasm Traps — and how Boring Semantic Layer prevents them.

Background
----------
These are two classic data-modeling pitfalls that silently produce **wrong
numbers** when analysts write joins by hand.

Fan Trap
~~~~~~~~
A *fan trap* occurs with a one-to-many join.  When you join a "one" table
(e.g. customers) to a "many" table (e.g. orders), measures on the "one"
side get duplicated — once for every matching row on the "many" side.

    customers (3 rows)  ──1:M──▶  orders (8 rows)
    revenue lives on customers     order_count lives on orders

    Naive JOIN produces 8 rows → SUM(revenue) is inflated!

Chasm Trap
~~~~~~~~~~
A *chasm trap* occurs when a shared table has *two independent* one-to-many
relationships and you try to combine them in a single query.  The join
creates a partial cross-product, inflating *both* sides.

    orders (8 rows) ◀──M:1──  customers (3 rows)  ──1:M──▶  support_tickets (5 rows)

    Naive:  customers JOIN orders JOIN tickets → cross-product per customer
    Customer with 3 orders and 2 tickets → 6 rows  → both sums are wrong!

How BSL fixes this
~~~~~~~~~~~~~~~~~~
`join_many` tells BSL about the cardinality.  At query time, BSL
**pre-aggregates each fact table at its own grain** before joining, so
fan-out never reaches the measure computation.  This is the same strategy
used by Looker, Malloy, and other modern semantic layers.
"""

import ibis
import pandas as pd

from boring_semantic_layer import to_semantic_table


def print_section(title: str):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


def main():
    con = ibis.sqlite.connect(":memory:")

    # ------------------------------------------------------------------
    # Seed data
    # ------------------------------------------------------------------
    customers_df = pd.DataFrame(
        {
            "customer_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "region": ["West", "East", "West"],
            "lifetime_value": [1000, 2000, 1500],  # one row per customer
        }
    )

    orders_df = pd.DataFrame(
        {
            "order_id": [101, 102, 103, 104, 105, 106, 107, 108],
            "customer_id": [1, 1, 1, 2, 2, 3, 3, 3],
            "amount": [50, 75, 60, 200, 150, 80, 90, 100],
        }
    )

    support_tickets_df = pd.DataFrame(
        {
            "ticket_id": [201, 202, 203, 204, 205],
            "customer_id": [1, 1, 2, 3, 3],
            "severity": ["high", "low", "high", "low", "low"],
        }
    )

    customers_tbl = con.create_table("customers", customers_df)
    orders_tbl = con.create_table("orders", orders_df)
    tickets_tbl = con.create_table("support_tickets", support_tickets_df)

    # ==================================================================
    # PART 1 — THE FAN TRAP
    # ==================================================================
    print_section("PART 1: THE FAN TRAP")

    # --- 1a. Ground truth -------------------------------------------------
    true_revenue = customers_df["lifetime_value"].sum()  # 4500
    true_order_count = len(orders_df)  # 8
    true_total_amount = orders_df["amount"].sum()  # 805

    print(f"\nGround truth:")
    print(f"  Total lifetime_value (customers): {true_revenue}")
    print(f"  Total order_count (orders):       {true_order_count}")
    print(f"  Total order_amount (orders):      {true_total_amount}")

    # --- 1b. Naive ibis join (WRONG) --------------------------------------
    print("\n--- Naive JOIN (raw ibis) ---")
    naive_join = customers_tbl.join(
        orders_tbl, customers_tbl.customer_id == orders_tbl.customer_id
    )
    naive_result = naive_join.aggregate(
        total_ltv=naive_join.lifetime_value.sum(),
        order_count=naive_join.count(),
        total_order_amount=naive_join.amount.sum(),
    ).execute()

    print(naive_result.to_string(index=False))
    print(
        f"\n  ❌  total_ltv = {naive_result['total_ltv'].iloc[0]}  "
        f"(expected {true_revenue})"
    )
    print(
        f"  ✓  order_count = {naive_result['order_count'].iloc[0]}  "
        f"(expected {true_order_count})"
    )
    print(
        f"  ✓  total_order_amount = {naive_result['total_order_amount'].iloc[0]}  "
        f"(expected {true_total_amount})"
    )
    print(
        "\n  → lifetime_value is INFLATED because each customer row was "
        "duplicated per order!"
    )

    # --- 1c. BSL with join_many (CORRECT) ---------------------------------
    print("\n--- BSL with join_many (pre-aggregation) ---")

    customers_st = (
        to_semantic_table(customers_tbl, name="customers")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
            region=lambda t: t.region,
        )
        .with_measures(
            total_ltv=lambda t: t.lifetime_value.sum(),
            customer_count=lambda t: t.count(),
        )
    )

    orders_st = (
        to_semantic_table(orders_tbl, name="orders")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
        )
        .with_measures(
            order_count=lambda t: t.count(),
            total_order_amount=lambda t: t.amount.sum(),
        )
    )

    # join_many tells BSL that one customer has many orders
    customer_orders = customers_st.join_many(
        orders_st,
        on="customer_id",
    )

    # Scalar aggregate — measures from BOTH tables
    fan_result = (
        customer_orders
        .aggregate(
            "customers.total_ltv",
            "orders.order_count",
            "orders.total_order_amount",
        )
        .execute()
    )
    print(fan_result.to_string(index=False))
    print(
        f"\n  ✓  total_ltv = {fan_result['customers.total_ltv'].iloc[0]}  "
        f"(expected {true_revenue})"
    )
    print(
        f"  ✓  order_count = {fan_result['orders.order_count'].iloc[0]}  "
        f"(expected {true_order_count})"
    )
    print(
        f"  ✓  total_order_amount = {fan_result['orders.total_order_amount'].iloc[0]}  "
        f"(expected {true_total_amount})"
    )

    # --- 1d. Group-by a dimension from the "one" side --------------------
    print("\n--- BSL: group by region, aggregate both sides ---")
    fan_by_region = (
        customer_orders
        .group_by("customers.region")
        .aggregate(
            "customers.total_ltv",
            "orders.order_count",
            "orders.total_order_amount",
        )
        .execute()
    )
    print(fan_by_region.to_string(index=False))
    print(
        "\n  → Each table's measures are pre-aggregated at their own grain, "
        "then joined.\n    No double-counting!"
    )

    # ==================================================================
    # PART 2 — THE CHASM TRAP
    # ==================================================================
    print_section("PART 2: THE CHASM TRAP")

    # --- 2a. Ground truth -------------------------------------------------
    true_ticket_count = len(support_tickets_df)  # 5

    print(f"\nGround truth:")
    print(f"  Total order_count (orders):       {true_order_count}")
    print(f"  Total ticket_count (tickets):     {true_ticket_count}")
    print(f"  Total lifetime_value (customers): {true_revenue}")

    # --- 2b. Naive ibis join (WRONG) --------------------------------------
    print("\n--- Naive JOIN (raw ibis) ---")
    naive_chasm = customers_tbl.join(
        orders_tbl, customers_tbl.customer_id == orders_tbl.customer_id
    ).join(
        tickets_tbl,
        customers_tbl.customer_id == tickets_tbl.customer_id,
    )
    naive_chasm_result = naive_chasm.aggregate(
        total_ltv=naive_chasm.lifetime_value.sum(),
        order_count=naive_chasm.count(),
        total_order_amount=naive_chasm.amount.sum(),
        ticket_count=naive_chasm.ticket_id.count(),
    ).execute()

    print(naive_chasm_result.to_string(index=False))

    actual_orders = naive_chasm_result["order_count"].iloc[0]
    actual_tickets = naive_chasm_result["ticket_count"].iloc[0]
    print(
        f"\n  ❌  order_count = {actual_orders}  "
        f"(expected {true_order_count})"
    )
    print(
        f"  ❌  ticket_count = {actual_tickets}  "
        f"(expected {true_ticket_count})"
    )
    print(
        "\n  → Cross-product: Alice (3 orders × 2 tickets = 6 rows), "
        "Bob (2×1 = 2), Carol (3×2 = 6).\n"
        "    Total 14 rows instead of 8 orders or 5 tickets.  "
        "BOTH measures are wrong!"
    )

    # --- 2c. BSL with two join_many arms (CORRECT) ------------------------
    print("\n--- BSL with join_many (pre-aggregation on each arm) ---")

    tickets_st = (
        to_semantic_table(tickets_tbl, name="tickets")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
            severity=lambda t: t.severity,
        )
        .with_measures(
            ticket_count=lambda t: t.count(),
        )
    )

    # Two independent 1:M relationships from customers
    full_model = (
        customers_st
        .join_many(orders_st, on="customer_id")
        .join_many(tickets_st, on="customer_id")
    )

    chasm_result = (
        full_model
        .aggregate(
            "customers.total_ltv",
            "orders.order_count",
            "orders.total_order_amount",
            "tickets.ticket_count",
        )
        .execute()
    )
    print(chasm_result.to_string(index=False))
    print(
        f"\n  ✓  total_ltv = {chasm_result['customers.total_ltv'].iloc[0]}  "
        f"(expected {true_revenue})"
    )
    print(
        f"  ✓  order_count = {chasm_result['orders.order_count'].iloc[0]}  "
        f"(expected {true_order_count})"
    )
    print(
        f"  ✓  ticket_count = {chasm_result['tickets.ticket_count'].iloc[0]}  "
        f"(expected {true_ticket_count})"
    )

    # --- 2d. Group-by region with all three fact tables -------------------
    print("\n--- BSL: group by region, aggregate across all three tables ---")
    chasm_by_region = (
        full_model
        .group_by("customers.region")
        .aggregate(
            "customers.total_ltv",
            "customers.customer_count",
            "orders.order_count",
            "orders.total_order_amount",
            "tickets.ticket_count",
        )
        .execute()
    )
    print(chasm_by_region.to_string(index=False))

    # Verify per-region correctness
    print("\nPer-region verification:")
    for _, row in chasm_by_region.iterrows():
        region = row["customers.region"]
        cids = customers_df[customers_df["region"] == region]["customer_id"]
        expected_ltv = customers_df[customers_df["region"] == region][
            "lifetime_value"
        ].sum()
        expected_orders = orders_df[orders_df["customer_id"].isin(cids)].shape[0]
        expected_tickets = support_tickets_df[
            support_tickets_df["customer_id"].isin(cids)
        ].shape[0]
        ltv_ok = row["customers.total_ltv"] == expected_ltv
        ord_ok = row["orders.order_count"] == expected_orders
        tkt_ok = row["tickets.ticket_count"] == expected_tickets
        print(
            f"  {region}: ltv={'✓' if ltv_ok else '❌'}  "
            f"orders={'✓' if ord_ok else '❌'}  "
            f"tickets={'✓' if tkt_ok else '❌'}"
        )

    # ==================================================================
    # PART 3 — INSPECTION: SEE THE SQL
    # ==================================================================
    print_section("PART 3: INSPECT THE GENERATED SQL")

    print("\nSQL for chasm-safe query (group by region, all measures):")
    query = (
        full_model
        .group_by("customers.region")
        .aggregate(
            "customers.total_ltv",
            "orders.order_count",
            "tickets.ticket_count",
        )
    )
    print(query.sql())

    # ==================================================================
    # Summary
    # ==================================================================
    print_section("SUMMARY")
    print(
        """
How BSL prevents these traps:

1. DECLARE CARDINALITY — use `join_many()` instead of raw joins to tell
   BSL that one side can produce multiple rows.

2. PRE-AGGREGATION — BSL automatically aggregates each fact table's
   measures at its native grain *before* joining.  This means:
     - Fan trap:  customer measures are summed on the customer table first,
       then joined to the pre-aggregated order totals.
     - Chasm trap: orders and tickets are each aggregated independently,
       then joined back to the customer dimension — no cross-product.

3. DIMENSION BRIDGE — the shared dimension (customer_id / region) acts as
   the join key between pre-aggregated results, ensuring correct alignment.

The analyst just writes:
    model.group_by("customers.region")
         .aggregate("orders.order_count", "tickets.ticket_count")

BSL handles the rest.
"""
    )


if __name__ == "__main__":
    main()
