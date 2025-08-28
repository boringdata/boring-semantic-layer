import ibis
import pandas as pd
from boring_semantic_layer.semantic_api.api import to_semantic_table

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
        order_month=lambda t: t.created_at.truncate("month"),
        user_signup_cohort=lambda t: t.created_at_right.truncate(
            "month"
        ),  # users.created_at becomes created_at_right after join
    )
)

# BSL equivalent of the Malloy cohort queries

# Query 1: User Cohort Analysis by Order Count
# This is a simplified version since BSL doesn't have nested queries like Malloy
# We implement the cohort analysis as a flat table with cohort dimensions
# Malloy's nested structure would need to be reconstructed in post-processing
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
    .group_by("order_month", "user_signup_cohort")
    .aggregate(
        users_in_cohort_that_ordered=lambda t: t.user_id.nunique(),
        total_sales_by_cohort=lambda t: t.sale_price.sum(),
    )
    .mutate(
        # Calculate percent of cohort - equivalent to user_count/all(user_count) in the cohort
        percent_of_cohort_that_ordered=lambda t: t.users_in_cohort_that_ordered
        / t.users_in_cohort_that_ordered.sum().over(
            ibis.window(group_by="user_signup_cohort")
        )
    )
    .order_by("order_month", "user_signup_cohort")
)

# Query 2: Cohort Analysis by Sales Percentage
# Similar to query_1 but focused on sales percentages
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
    .group_by("order_month", "user_signup_cohort")
    .aggregate(
        cohort_total_sales=lambda t: t.sale_price.sum(),
        user_count_in_cohort=lambda t: t.user_id.nunique(),
    )
    .mutate(
        # Calculate cohort as percent of total sales - equivalent to total_sales/all(total_sales)
        cohort_as_percent_of_sales=lambda t: t.cohort_total_sales
        / t.cohort_total_sales.sum().over()
    )
    .order_by("order_month", "user_signup_cohort")
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
    .group_by("order_month", "user_signup_cohort")
    .aggregate(
        users_in_cohort_that_ordered=lambda t: t.user_id.nunique(),
        total_sales_by_cohort=lambda t: t.sale_price.sum(),
    )
    .order_by("order_month", "user_signup_cohort")
)
