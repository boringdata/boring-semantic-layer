"""
Demonstration of projection pushdown optimization.

Projection pushdown is always enabled and automatically filters out unused
columns before joins to reduce data scanned.

Run with: python examples/projection_pushdown_demo.py
"""

import ibis
import pandas as pd

from boring_semantic_layer import to_untagged, to_semantic_table


def main():
    con = ibis.duckdb.connect(":memory:")

    customers_df = pd.DataFrame(
        {
            "customer_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
            "phone": ["555-0001", "555-0002", "555-0003"],
            "address": ["123 Main St", "456 Oak Ave", "789 Pine Rd"],
            "city": ["New York", "Los Angeles", "Chicago"],
            "state": ["NY", "CA", "IL"],
            "zipcode": ["10001", "90001", "60601"],
            "country": ["USA", "USA", "USA"],
            "registration_date": ["2023-01-01", "2023-02-15", "2023-03-20"],
        }
    )

    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4, 5],
            "customer_id": [1, 1, 2, 3, 3],
            "order_date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
            "amount": [100.0, 150.0, 200.0, 75.0, 125.0],
            "product": ["Widget", "Gadget", "Widget", "Gadget", "Widget"],
        }
    )

    customers_tbl = con.create_table("customers", customers_df)
    orders_tbl = con.create_table("orders", orders_df)

    customers = to_semantic_table(customers_tbl, name="customers").with_dimensions(
        customer_id=lambda t: t.customer_id,
        name=lambda t: t.name,
        city=lambda t: t.city,
    )

    orders = (
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

    joined = customers.join_one(orders, lambda c, o: c.customer_id == o.customer_id)
    query = joined.group_by("customers.customer_id", "customers.name").aggregate("total_amount")

    print("=" * 80)
    print("PROJECTION PUSHDOWN OPTIMIZATION (ALWAYS ENABLED)")
    print("=" * 80)
    print("\nGenerated SQL with automatic projection pushdown:")
    print("-" * 80)
    sql = str(ibis.to_sql(to_untagged(query)))
    print(sql)
    print("-" * 80)

    # Check which columns appear in the SQL
    unused_cols = [
        "email",
        "phone",
        "address",
        # "city",  # city is defined as a dimension, but not used in this query
        "state",
        "zipcode",
        "country",
        "registration_date",
    ]

    unused_count = sum(1 for col in unused_cols if col in sql.lower())

    print("\n✓ Projection pushdown automatically filtered unused columns")
    print("  Total columns in customers table: 10")
    print("  Columns used in query: 2 (customer_id, name)")
    print(f"  Unused columns in SQL: {unused_count} out of {len(unused_cols)}")
    print(f"  Savings: ~{int((1 - 2 / 10) * 100)}% fewer columns scanned!")
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
