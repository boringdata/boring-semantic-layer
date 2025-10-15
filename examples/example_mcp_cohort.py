# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "boring-semantic-layer[fastmcp] >= 0.1.1"
# ]
# ///
"""
Example: Cohort Analysis with SemanticModel for Orders and Customers

This example demonstrates how to use SemanticModel to analyze customer cohorts
using orders.csv and customers.csv data. It shows how to define semantic models
with joins to analyze customer behavior, order patterns, and regional analysis.

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
"""

import ibis
from boring_semantic_layer.semantic_model import SemanticModel, Join
from boring_semantic_layer import MCPSemanticModel

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


# Define the customers semantic model
# - Primary key: customer_id
# - Dimensions: customer_id, country_name
# - Measures: customer_count (total number of customers)
customers_model = SemanticModel(
    name="customers",
    table=customers_tbl,
    dimensions={
        "customer_id": lambda t: t.customer_id,
        "country_name": lambda t: t.country_name,
    },
    measures={
        "customer_count": lambda t: t.customer_id.count(),
    },
    primary_key="customer_id",
)

# Define the orders semantic model
# - Primary key: order_id
# - Dimensions: order_id, order_date, customer_id
# - Time dimension: order_date for cohort analysis
# - Measures: order_count, total_revenue, avg_order_value, total_products
# - Joins: one-to-one join to customers on customer_id
orders_model = SemanticModel(
    name="orders",
    table=orders_tbl,
    dimensions={
        "order_id": lambda t: t.order_id,
        "order_date": lambda t: t.order_date,
        "customer_id": lambda t: t.customer_id,
    },
    time_dimension="order_date",
    smallest_time_grain="TIME_GRAIN_DAY",
    measures={
        "order_count": lambda t: t.order_id.count(),
        "total_revenue": lambda t: t.order_amount.sum(),
        "avg_order_value": lambda t: t.order_amount.mean(),
        "total_products": lambda t: t.product_count.sum(),
        "avg_products_per_order": lambda t: t.product_count.mean(),
    },
    joins={
        "customers": Join.one(
            alias="customers",
            model=customers_model,
            with_=lambda left: left.customer_id,
        ),
    },
    primary_key="order_id",
)


# Define the cohort semantic model
# - Dimensions: cohort_month, cohort_period (month_1, month_2, etc.), period_number
# - Measures: total_revenue, total_product, avg_order_value, churn percentage
cohort_model = SemanticModel(
    name="cohorts",
    table=cohort_tbl,
    dimensions={
        "cohort_month": lambda t: t.cohort_month,
        "cohort_period": lambda t: t.cohort_period,
        "period_number": lambda t: t.period_number,
    },
    time_dimension="cohort_month",
    smallest_time_grain="TIME_GRAIN_MONTH",
    measures={
        "total_revenue": lambda t: t.order_amount.sum(),
        "total_product": lambda t: t.product_count.sum(),
        "avg_order_value": lambda t: t.order_amount.mean(),
        "active_customers": lambda t: t.customer_id.nunique(),
        "initial_cohort_size": lambda t: t.cohort_size.max(),
        "retention_rate": lambda t: (
            t.customer_id.nunique().cast("float")
            / t.cohort_size.max().cast("float")
            * 100
        ),
        "churn_rate": lambda t: (
            100
            - (
                t.customer_id.nunique().cast("float")
                / t.cohort_size.max().cast("float")
                * 100
            )
        ),
    },
)


server = MCPSemanticModel(
    models={
        "customers": customers_model,
        "orders": orders_model,
        "cohorts": cohort_model,
    },
    name="Cohort Data Semantic Layer Server",
)

if __name__ == "__main__":
    server.run()
