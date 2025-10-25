import ibis
from boring_semantic_layer import to_semantic_table

con = ibis.duckdb.connect()
BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"

flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")
carriers_tbl = con.read_parquet(f"{BASE_URL}/carriers.parquet")

flights_with_carriers = flights_tbl.join(
    carriers_tbl, flights_tbl.carrier == carriers_tbl.code, how="inner"
)

flights_st = to_semantic_table(flights_with_carriers).with_measures(
    flight_count=lambda t: t.count()
)

FLIGHT_COUNT_DESC = ibis.desc("flight_count")
BASE_GROUP_BY = ["nickname", "destination", "origin"]

query_1 = (
    flights_st.group_by("nickname")
    .aggregate(flight_count=lambda t: t.count())
    .mutate(all_flights=lambda t: t.all(t.flight_count))
    .order_by(FLIGHT_COUNT_DESC)
    .limit(2)
)

query_2 = (
    flights_st.group_by("nickname")
    .aggregate(flight_count=lambda t: t.count())
    .mutate(percent_of_flights=lambda t: t.flight_count / t.all(t.flight_count))
    .order_by(FLIGHT_COUNT_DESC)
    .limit(5)
)

query_3 = (
    flights_st.group_by(*BASE_GROUP_BY)
    .aggregate(flight_count=lambda t: t.count())
    .mutate(
        flights_by_this_carrier=lambda t: t.flight_count.sum().over(
            ibis.window(group_by="nickname")
        ),
        flights_to_this_destination=lambda t: t.flight_count.sum().over(
            ibis.window(group_by="destination")
        ),
        flights_by_this_origin=lambda t: t.flight_count.sum().over(
            ibis.window(group_by="origin")
        ),
    )
    .mutate(
        flights_on_this_route=lambda t: t.flight_count.sum().over(
            ibis.window(group_by=["destination", "origin"])
        ),
    )
    .order_by(*BASE_GROUP_BY)
    .limit(20)
)

query_4 = (
    flights_st.group_by(*BASE_GROUP_BY)
    .aggregate(flight_count=lambda t: t.count())
    .mutate(
        **{
            "carrier as a percent of all flights": lambda t: (
                t.flight_count.sum().over(ibis.window(group_by="nickname"))
                / t.flight_count.sum().over()
            ),
            "destination as a percent of all flights": lambda t: (
                t.flight_count.sum().over(ibis.window(group_by="destination"))
                / t.flight_count.sum().over()
            ),
            "origin as a percent of all flights": lambda t: (
                t.flight_count.sum().over(ibis.window(group_by="origin"))
                / t.flight_count.sum().over()
            ),
            "carriers as a percentage of route": lambda t: (
                t.flight_count
                / t.flight_count.sum().over(
                    ibis.window(group_by=["destination", "origin"])
                )
            ),
        }
    )
    .order_by(*BASE_GROUP_BY)
)
