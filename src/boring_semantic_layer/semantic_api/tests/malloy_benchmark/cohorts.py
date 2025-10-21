import ibis
import pandas as pd
from boring_semantic_layer.semantic_api import to_semantic_table

con = ibis.duckdb.connect()

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"

# Load the tables
order_items_tbl = con.read_parquet(f"{BASE_URL}/order_items.parquet")
users_tbl = con.read_parquet(f"{BASE_URL}/users.parquet")

# Create joined table first, then make it semantic
order_items_with_users = order_items_tbl.join(
    users_tbl, order_items_tbl.user_id == users_tbl.id, how="inner"
)

# Convert to semantic table with measures
order_items_st = (
    to_semantic_table(order_items_with_users)
    .with_measures(
        total_sales=lambda t: t.sale_price.sum(),
        user_count=lambda t: t.user_id.nunique(),
    )
    .with_dimensions(
        **{
            "Order Month": lambda t: t.created_at.truncate("month"),
            "User Signup Cohort": lambda t: t.created_at_right.truncate(
                "month"
            ),  # users.created_at becomes created_at_right after join
        }
    )
)

# BSL equivalent of the Malloy cohort queries

# Query 1: User Cohort Analysis by Order Count (flattened equivalent)
# BSL produces the flattened equivalent of Malloy's nested structure
query_1 = (
    order_items_st
    # Filter to 2022 data (6 months) - approximating Malloy's date range filter
    .filter(
        lambda t: (
            (t.created_at >= pd.Timestamp("2022-01-01"))
            & (t.created_at < pd.Timestamp("2022-07-01"))
            & (t.created_at_right >= pd.Timestamp("2022-01-01"))
            & (t.created_at_right < pd.Timestamp("2022-07-01"))
        )
    )
    .group_by("Order Month", "User Signup Cohort")
    .aggregate(
        **{
            # Users in Cohort that Ordered: users in this specific cohort who ordered in this month
            "Users in Cohort that Ordered": lambda t: t.user_id.nunique(),
        }
    )
    .mutate(
        **{
            # Users that Ordered Count: grand total via t.all()
            "Users that Ordered Count": lambda t: t.all(t["Users in Cohort that Ordered"]),
            # Percent of cohort that ordered: cohort users / grand total
            "Percent of cohort that ordered": lambda t: (
                t["Users in Cohort that Ordered"]
                / t.all(t["Users in Cohort that Ordered"])
            ),
            # Convert User Signup Cohort to date string to match Malloy format
            "User Signup Cohort": lambda t: t["User Signup Cohort"].date().cast(str),
        }
    )
    .order_by(ibis.desc("Order Month"), "User Signup Cohort")
)

# Query 2: Cohort Analysis by Sales Percentage (flattened equivalent)
# BSL produces the flattened equivalent of Malloy's nested structure
query_2 = (
    order_items_st
    # Filter to 2022 data (6 months)
    .filter(
        lambda t: (
            (t.created_at >= pd.Timestamp("2022-01-01"))
            & (t.created_at < pd.Timestamp("2022-07-01"))
            & (t.created_at_right >= pd.Timestamp("2022-01-01"))
            & (t.created_at_right < pd.Timestamp("2022-07-01"))
        )
    )
    .group_by("Order Month", "User Signup Cohort")
    .aggregate(
        **{
            # Sales for this specific cohort in this month
            "cohort_sales": lambda t: t.sale_price.sum()
        }
    )
    .mutate(
        **{
            # Total Sales: grand total via t.all()
            "Total Sales": lambda t: t.all(t.cohort_sales),
            # Cohort as Percent of Sales: this cohort's sales / grand total
            "Cohort as Percent of Sales": lambda t: t.cohort_sales / t.all(t.cohort_sales),
            # Convert User Signup Cohort to date string to match Malloy format
            "User Signup Cohort": lambda t: t["User Signup Cohort"].date().cast(str),
        }
    )
    .order_by(ibis.desc("Order Month"), "User Signup Cohort")
)

# Query 3: Simplified flat cohort analysis (matches the Malloy query_3)
# This is the most direct BSL equivalent without nesting complexities
query_3 = (
    order_items_st
    # Filter to 2022 data (6 months)
    .filter(
        lambda t: (
            (t.created_at >= pd.Timestamp("2022-01-01"))
            & (t.created_at < pd.Timestamp("2022-07-01"))
            & (t.created_at_right >= pd.Timestamp("2022-01-01"))
            & (t.created_at_right < pd.Timestamp("2022-07-01"))
        )
    )
    .group_by("Order Month", "User Signup Cohort")
    .aggregate(
        **{
            "Users in Cohort that Ordered": lambda t: t.user_id.nunique(),
            "Total Sales by Cohort": lambda t: t.sale_price.sum(),
        }
    )
    .order_by("Order Month", "User Signup Cohort")
)
