import ibis

from boring_semantic_layer import to_semantic_table

con = ibis.duckdb.connect()
BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"

flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")

flights_st = (
    to_semantic_table(flights_tbl)
    .with_measures(
        flight_count=lambda t: t.count(),
        avg_delay=lambda t: t.arr_delay.mean(),
    )
    .with_dimensions(
        dep_month=lambda t: t.dep_time.truncate("M"),
        dep_year=lambda t: t.dep_time.truncate("Y"),
    )
)


def moving_avg_window(order_by, preceding):
    return ibis.window(order_by=order_by, preceding=preceding, following=0)


query_1 = (
    flights_st.group_by("dep_month")
    .aggregate(flight_count=lambda t: t.count())
    .mutate(
        moving_avg_flight_count=lambda t: t.flight_count.mean().over(
            moving_avg_window("dep_month", 3),
        ),
    )
    .order_by("dep_month")
)

query_2 = (
    flights_st.group_by("dep_month")
    .aggregate(avg_delay=lambda t: t.arr_delay.mean())
    .mutate(
        moving_avg_delay=lambda t: t.avg_delay.mean().over(
            moving_avg_window("dep_month", 3),
        ),
    )
    .order_by("dep_month")
)

query_3 = (
    flights_st.group_by("dep_year", "dep_month")
    .aggregate(flight_count=lambda t: t.count(), avg_delay=lambda t: t.arr_delay.mean())
    .mutate(
        moving_avg_flight_count=lambda t: t.flight_count.mean().over(
            moving_avg_window(["dep_year", "dep_month"], 6),
        ),
        moving_avg_delay=lambda t: t.avg_delay.mean().over(
            moving_avg_window(["dep_year", "dep_month"], 6),
        ),
    )
    .order_by("dep_year", "dep_month")
)
