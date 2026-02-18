#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "boring-semantic-layer[examples] >= 0.2.0"
# ]
# ///
"""
Example: Generate ERD diagrams for the cohort analysis semantic models.

This script demonstrates the new to_erd() method functionality added in recent commits:
- ASCII ERD-style representation for semantic models and joins
- Horizontal layout for relationship visualization
- Optional ERD rendering via to_erd() method

The cohort example includes three models with joins:
- customers: Basic customer information
- orders: Order data with join to customers
- cohorts: Cohort analysis data

This shows how the ERD visualization displays:
- Individual models with their dimensions (◆) and measures (●)
- One-to-one relationships (────)
- Model metadata in box format
"""

import ibis

from boring_semantic_layer import to_semantic_table

# Create a DuckDB connection for in-memory table creation
con = ibis.duckdb.connect()

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"
customers_tbl = con.read_parquet(f"{BASE_URL}/cohort_customers.parquet")
orders_tbl = con.read_parquet(f"{BASE_URL}/cohort_orders.parquet")

# Register the dataframes as DuckDB tables
customers_tbl = con.create_table("customers_tbl", customers_tbl)
orders_tbl = con.create_table("orders_tbl", orders_tbl)

# Define the customers semantic table
customers_model = (
    to_semantic_table(customers_tbl, name="customers")
    .with_dimensions(
        customer_id={
            "expr": lambda t: t.customer_id,
            "description": "Unique customer identifier",
        },
        country_name={
            "expr": lambda t: t.country_name,
            "description": "Customer's country name",
        },
    )
    .with_measures(
        customer_count={
            "expr": lambda t: t.customer_id.count(),
            "description": "Total number of customers",
        }
    )
)

# Define the orders semantic table with join to customers
orders_model = (
    to_semantic_table(orders_tbl, name="orders")
    .with_dimensions(
        order_id={
            "expr": lambda t: t.order_id,
            "description": "Unique order identifier",
        },
        order_date={
            "expr": lambda t: t.order_date,
            "description": "Date the order was placed",
            "is_time_dimension": True,
            "smallest_time_grain": "TIME_GRAIN_DAY",
        },
        customer_id={
            "expr": lambda t: t.customer_id,
            "description": "Customer who placed the order",
        },
    )
    .with_measures(
        order_count={
            "expr": lambda t: t.order_id.count(),
            "description": "Total number of orders",
        },
        total_revenue={
            "expr": lambda t: t.order_amount.sum(),
            "description": "Total revenue from orders",
        },
        avg_order_value={
            "expr": lambda t: t.order_amount.mean(),
            "description": "Average order value",
        },
        total_products={
            "expr": lambda t: t.product_count.sum(),
            "description": "Total number of products sold",
        },
        avg_products_per_order={
            "expr": lambda t: t.product_count.mean(),
            "description": "Average products per order",
        },
    )
    .join_one(customers_model, on="customer_id")
)

print("=" * 80)
print("ERD: Individual Customers Model")
print("=" * 80)
print(customers_model.to_erd())
print()

print("=" * 80)
print("ERD: Orders Model (with join to Customers)")
print("=" * 80)
print(orders_model.to_erd())
print()

print("=" * 80)
print("Legend")
print("=" * 80)
print("◆ = Dimension")
print("● = Measure")
print("──── = One-to-One relationship (with join columns shown)")
print("──< = One-to-Many relationship")
print(">──< = Many-to-Many relationship")
print()
print("Note: The connector now shows the join columns:")
print("  ──── (column_a = column_b)")
print("  This indicates the tables are joined on column_a from the left table")
print("  and column_b from the right table.")
