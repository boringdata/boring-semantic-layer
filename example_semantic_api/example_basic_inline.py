import ibis
from boring_semantic_layer.semantic_api.api import to_semantic_table

con = ibis.duckdb.connect(":memory:")
BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"
flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")

flights_st = (
    to_semantic_table(flights_tbl)
    .with_dimensions(
        origin=lambda t: t.origin,
        carrier=lambda t: t.carrier,
        arr_time=lambda t: t.arr_time.truncate("month"),
    )
    .with_measures(
        flight_count=lambda t: t.count(),
        avg_dep_delay=lambda t: t.dep_delay.mean(),
    )
)

carrier_share = (
    flights_st.group_by("carrier")
    .aggregate(lambda t: t.flight_count)
    .mutate(market_share=lambda t: 100 * t.flight_count / t.flight_count.sum())
)
print(carrier_share.execute())

rolling_window = ibis.window(order_by="month", rows=(0, 2))
monthly_trends = (
    flights_st.group_by("month", "carrier")
    .aggregate(lambda t: t.flight_count)
    .mutate(rolling_avg=lambda t: t.flight_count.mean().over(rolling_window))
)
print(monthly_trends.execute().head())
