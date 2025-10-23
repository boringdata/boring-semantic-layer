import ibis
import pandas as pd
from boring_semantic_layer.api import to_semantic_table

con = ibis.duckdb.connect()
BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"

order_items_tbl = con.read_parquet(f"{BASE_URL}/order_items.parquet")
users_tbl = con.read_parquet(f"{BASE_URL}/users.parquet")

order_items_with_users = order_items_tbl.join(
    users_tbl, order_items_tbl.user_id == users_tbl.id, how="inner"
)

order_items_st = (
    to_semantic_table(order_items_with_users)
    .with_measures(
        total_sales=lambda t: t.sale_price.sum(),
        user_count=lambda t: t.user_id.nunique(),
    )
    .with_dimensions(
        **{
            "Order Month": lambda t: t.created_at.truncate("month"),
            "User Signup Cohort": lambda t: t.created_at_right.truncate("month"),
        }
    )
)

DATE_FILTER = lambda t: (
    (t.created_at >= pd.Timestamp("2022-01-01"))
    & (t.created_at < pd.Timestamp("2022-07-01"))
    & (t.created_at_right >= pd.Timestamp("2022-01-01"))
    & (t.created_at_right < pd.Timestamp("2022-07-01"))
)

query_1 = (
    order_items_st.filter(DATE_FILTER)
    .group_by("Order Month", "User Signup Cohort")
    .aggregate(**{"Users in Cohort that Ordered": lambda t: t.user_id.nunique()})
    .mutate(
        **{
            "Users that Ordered Count": lambda t: t["Users in Cohort that Ordered"].sum().over(
                ibis.window(group_by="Order Month")
            ),
            "Percent of cohort that ordered": lambda t: (
                t["Users in Cohort that Ordered"]
                / t["Users in Cohort that Ordered"].sum().over(ibis.window(group_by="Order Month"))
            ),
            "User Signup Cohort": lambda t: t["User Signup Cohort"].date().cast(str),
        }
    )
    .order_by(ibis.desc("Order Month"), "User Signup Cohort")
)

query_2 = (
    order_items_st.filter(DATE_FILTER)
    .group_by("Order Month", "User Signup Cohort")
    .aggregate(**{"cohort_sales": lambda t: t.sale_price.sum()})
    .mutate(
        **{
            "Total Sales": lambda t: t.cohort_sales.sum().over(ibis.window(group_by="Order Month")),
            "Cohort as Percent of Sales": lambda t: t.cohort_sales
            / t.cohort_sales.sum().over(ibis.window(group_by="Order Month")),
            "User Signup Cohort": lambda t: t["User Signup Cohort"].date().cast(str),
        }
    )
    .order_by(ibis.desc("Order Month"), "User Signup Cohort")
)

query_3 = (
    order_items_st.filter(DATE_FILTER)
    .group_by("Order Month", "User Signup Cohort")
    .aggregate(
        **{
            "Users in Cohort that Ordered": lambda t: t.user_id.nunique(),
            "Total Sales by Cohort": lambda t: t.sale_price.sum(),
        }
    )
    .order_by("Order Month", "User Signup Cohort")
)
