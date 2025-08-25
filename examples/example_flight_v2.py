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
        month=lambda t: t.arr_time.truncate("month"),
    )
    .with_measures(
        flight_count=lambda t: t.count(),
        avg_dep_delay=lambda t: t.dep_delay.mean(),
    )
    .mutate(delay_efficiency=lambda t: t.flight_count / (t.avg_dep_delay + 1))
)

carrier_share = (
    flights_st.group_by("carrier")
    .aggregate(flight_count=flights_st.flight_count)
    .mutate(market_share=lambda t: t.flight_count / t.flight_count.sum())
    .select("carrier", "flight_count", "market_share")
)
carrier_expr = carrier_share.to_expr()
print(carrier_expr.execute().head())

rolling_window = ibis.window(order_by="month", rows=(2, 0))
monthly_trends = (
    flights_st.group_by("month", "carrier")
    .aggregate(monthly_flights=flights_st.flight_count)
    .mutate(rolling_avg=lambda t: t.monthly_flights.mean().over(rolling_window))
    .select("month", "carrier", "monthly_flights", "rolling_avg")
)
monthly_expr = monthly_trends.to_expr()
print(monthly_expr.execute().head())
