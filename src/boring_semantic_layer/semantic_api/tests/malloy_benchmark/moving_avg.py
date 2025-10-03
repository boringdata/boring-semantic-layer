import ibis
from boring_semantic_layer.semantic_api.api import to_semantic_table

con = ibis.duckdb.connect()

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"

# Load the flights table
flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")

# Convert to semantic table with measures and dimensions
flights_st = (
    to_semantic_table(flights_tbl)
    .with_measures(
        flight_count=lambda t: t.count(), avg_delay=lambda t: t.arr_delay.mean()
    )
    .with_dimensions(
        dep_month=lambda t: t.dep_time.truncate(
            "M"
        ),  # Truncate to month (first day of month)
        dep_year=lambda t: t.dep_time.truncate(
            "Y"
        ),  # Truncate to year (first day of year)
    )
)

# Query 1: Moving average of flight count by month (3-month window)
query_1 = (
    flights_st.group_by("dep_month")
    .aggregate(flight_count=lambda t: t.count())
    .mutate(
        moving_avg_flight_count=lambda t: t.flight_count.mean().over(
            ibis.window(
                order_by="dep_month",
                preceding=3,  # 3 preceding rows + current row = 4-row window (Malloy's avg_moving(x, 3) means max 4 values)
                following=0,
            )
        )
    )
    .order_by("dep_month")
)

# Query 2: Moving average of delay by month (3-month window)
query_2 = (
    flights_st.group_by("dep_month")
    .aggregate(avg_delay=lambda t: t.arr_delay.mean())
    .mutate(
        moving_avg_delay=lambda t: t.avg_delay.mean().over(
            ibis.window(
                order_by="dep_month",
                preceding=3,  # 3 preceding rows + current row = 4-row window (Malloy's avg_moving(x, 3) means max 4 values)
                following=0,
            )
        )
    )
    .order_by("dep_month")
)

# Query 3: Moving averages by year and month (6-month window)
query_3 = (
    flights_st.group_by("dep_year", "dep_month")
    .aggregate(flight_count=lambda t: t.count(), avg_delay=lambda t: t.arr_delay.mean())
    .mutate(
        moving_avg_flight_count=lambda t: t.flight_count.mean().over(
            ibis.window(
                order_by=["dep_year", "dep_month"],
                preceding=6,  # 6 preceding rows + current row = 7-row window (Malloy's avg_moving(x, 6) means max 7 values)
                following=0,
            )
        ),
        moving_avg_delay=lambda t: t.avg_delay.mean().over(
            ibis.window(
                order_by=["dep_year", "dep_month"],
                preceding=6,  # 6 preceding rows + current row = 7-row window (Malloy's avg_moving(x, 6) means max 7 values)
                following=0,
            )
        ),
    )
    .order_by("dep_year", "dep_month")
)
