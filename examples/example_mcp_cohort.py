#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "boring-semantic-layer[fastmcp] >= 0.2.0"
# ]
# ///
"""
Example: Cohort Analysis with SemanticTable for Orders and Customers (BSL v2)

This example demonstrates how to use to_semantic_table to analyze customer cohorts
using orders and customers data. It shows how to define semantic models with joins
to analyze customer behavior, order patterns, and regional analysis.

Tables:

Customers table:
- customer_id (primary key)
- country_name

Orders table:
- order_id (primary key)
- order_date
- order_amount
- customer_id (foreign key)
- product_count

The example shows how to query customer and order data with joins for cohort analysis.

Usage:
    Add to your MCP configuration file:
    {
        "mcpServers": {
            "cohort-semantic-layer": {
                "command": "uv",
                "args": ["--directory", "/path/to/boring-semantic-layer/examples", "run", "example_mcp_cohort.py"]
            }
        }
    }
"""

import ibis
from boring_semantic_layer import MCPSemanticModel, to_semantic_table

# Create a DuckDB connection for in-memory table creation
con = ibis.duckdb.connect()

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"
customers_tbl = con.read_parquet(f"{BASE_URL}/cohort_customers.parquet")
orders_tbl = con.read_parquet(f"{BASE_URL}/cohort_orders.parquet")

# Register the dataframes as DuckDB tables
customers_tbl = con.create_table("customers_tbl", customers_tbl)
orders_tbl = con.create_table("orders_tbl", orders_tbl)

# Create cohort analysis table using SQL
# First, create a table with customer first order dates (cohort definition)
cohort_base_query = """
    WITH customer_cohorts AS (
        SELECT
            customer_id,
            MIN(CAST(order_date AS DATE)) as first_order_date,
            DATE_TRUNC('month', MIN(CAST(order_date AS DATE))) as cohort_month
        FROM orders_tbl
        GROUP BY customer_id
    ),
    cohort_data AS (
        SELECT
            cc.customer_id,
            cc.cohort_month,
            cc.first_order_date,
            o.order_id,
            CAST(o.order_date AS DATE) as order_date,
            o.order_amount,
            o.product_count,
            DATEDIFF('month', cc.cohort_month, DATE_TRUNC('month', CAST(o.order_date AS DATE))) + 1 as period_number
        FROM customer_cohorts cc
        JOIN orders_tbl o ON cc.customer_id = o.customer_id
        WHERE DATEDIFF('month', cc.cohort_month, DATE_TRUNC('month', CAST(o.order_date AS DATE))) BETWEEN 0 AND 5
    ),
    cohort_sizes AS (
        SELECT
            cohort_month,
            COUNT(DISTINCT customer_id) as cohort_size
        FROM customer_cohorts
        GROUP BY cohort_month
    )
    SELECT
        cd.customer_id,
        cd.order_id,
        cd.order_date,
        cd.order_amount,
        cd.product_count,
        cd.cohort_month,
        cd.period_number,
        cs.cohort_size,
        CONCAT('month_', cd.period_number) as cohort_period
    FROM cohort_data cd
    JOIN cohort_sizes cs ON cd.cohort_month = cs.cohort_month
"""

cohort_tbl = con.sql(cohort_base_query)

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
    .join_one(customers_model, left_on="customer_id", right_on="customer_id")
)

# Define the cohort semantic table
cohort_model = (
    to_semantic_table(cohort_tbl, name="cohorts")
    .with_dimensions(
        cohort_month={
            "expr": lambda t: t.cohort_month,
            "description": "Month when the customer made their first purchase",
            "is_time_dimension": True,
            "smallest_time_grain": "TIME_GRAIN_MONTH",
        },
        cohort_period={
            "expr": lambda t: t.cohort_period,
            "description": "Cohort period label (month_1, month_2, etc.)",
        },
        period_number={
            "expr": lambda t: t.period_number,
            "description": "Numeric period number (1, 2, 3, etc.)",
        },
    )
    .with_measures(
        total_revenue={
            "expr": lambda t: t.order_amount.sum(),
            "description": "Total revenue for the cohort period",
        },
        total_product={
            "expr": lambda t: t.product_count.sum(),
            "description": "Total products sold in the cohort period",
        },
        avg_order_value={
            "expr": lambda t: t.order_amount.mean(),
            "description": "Average order value in the cohort period",
        },
        active_customers={
            "expr": lambda t: t.customer_id.nunique(),
            "description": "Number of active customers in the period",
        },
        initial_cohort_size={
            "expr": lambda t: t.cohort_size.max(),
            "description": "Initial size of the cohort",
        },
        retention_rate={
            "expr": lambda t: (
                t.customer_id.nunique().cast("float")
                / t.cohort_size.max().cast("float")
                * 100
            ),
            "description": "Percentage of customers retained from initial cohort",
        },
        churn_rate={
            "expr": lambda t: (
                100
                - (
                    t.customer_id.nunique().cast("float")
                    / t.cohort_size.max().cast("float")
                    * 100
                )
            ),
            "description": "Percentage of customers churned from initial cohort",
        },
    )
)

# Create MCP server
server = MCPSemanticModel(
    models={
        "customers": customers_model,
        "orders": orders_model,
        "cohorts": cohort_model,
    },
    name="Cohort Data Semantic Layer Server (BSL v2)",
)

if __name__ == "__main__":
    server.run()
