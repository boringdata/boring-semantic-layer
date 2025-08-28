import ibis
from boring_semantic_layer.semantic_api.api import to_semantic_table

con = ibis.duckdb.connect()

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"

tables = {
    "flights_tbl": con.read_parquet(f"{BASE_URL}/flights.parquet"),
    "carriers_tbl": con.read_parquet(f"{BASE_URL}/carriers.parquet"),
    "inventory_items_tbl": con.read_parquet(f"{BASE_URL}/inventory_items.parquet"),
    "order_items_tbl": con.read_parquet(f"{BASE_URL}/order_items.parquet"),
}

flight_st = to_semantic_table(tables["flights_tbl"]).with_dimensions(
    flight_count=lambda t: t.count(),
    month_of_year=lambda t: t.dep_time.month(),
    flight_year=lambda t: t.dep_time.truncate("year"),
)

query_1 = (
    flight_st.group_by("month_of_year", "flight_year")
    .aggregate(flight_count=lambda t: t.count())
    .order_by("month_of_year", "flight_year")
)

query_2 = (
    flight_st.group_by("carrier")
    .aggregate(
        flights_in_2002=lambda t: (t.dep_time.year() == 2002).sum(),
        flights_in_2003=lambda t: (t.dep_time.year() == 2003).sum(),
    )
    .mutate(
        percent_change=lambda t: (
            (t.flights_in_2003 - t.flights_in_2002) / t.flights_in_2003.nullif(0)
        )
    )
    .order_by("carrier")
)

# Add dep_year dimension to semantic table for query_3
flight_st_with_year = flight_st.with_dimensions(
    dep_year=lambda t: t.dep_time.truncate("year")
)

query_3 = (
    flight_st_with_year.group_by("dep_year")
    .aggregate(flight_count=lambda t: t.count())
    .mutate(
        last_year=lambda t: t.flight_count.lag(1),
        growth=lambda t: (t.flight_count.lag(1) - t.flight_count)
        / t.flight_count.lag(1),
    )
    .order_by("dep_year")
)

# Create joined table first, then make it semantic
# joined_table = tables["order_items_tbl"].join(
#     tables["inventory_items_tbl"],
#     tables["order_items_tbl"].inventory_item_id == tables["inventory_items_tbl"].id,
#     how="inner"
# )

# order_items_st = (
#     to_semantic_table(joined_table)
#     .with_dimensions(
#         product_category=lambda t: t.product_category,  # from inventory_items after join
#     )
# )

# # For query_4, we need to determine the reference year context
# # Based on typical e-commerce data, let's assume "now" is 2024
# # so last_year = 2023, prior_year = 2022
# REFERENCE_YEAR = 2024

# query_4 = (
#     order_items_st
#     .group_by("product_category")
#     .aggregate(
#         last_year=lambda t: (t.created_at.year() == (REFERENCE_YEAR - 1)).sum(),  # 2023
#         prior_year=lambda t: (t.created_at.year() == (REFERENCE_YEAR - 2)).sum(),  # 2022
#     )
#     .mutate(
#         percent_change=lambda t: (
#             (t.last_year - t.prior_year) /
#             t.last_year.nullif(0)
#         )
#     )
#     # Note: limit: 10 would be applied in execution if needed
# )
