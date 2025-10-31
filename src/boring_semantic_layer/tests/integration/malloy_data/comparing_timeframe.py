import ibis

from boring_semantic_layer import to_semantic_table

con = ibis.duckdb.connect()
BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"

flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")

flight_st = to_semantic_table(flights_tbl).with_dimensions(
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
        ),
    )
    .order_by("carrier")
)

flight_st_with_year = flight_st.with_dimensions(
    dep_year=lambda t: t.dep_time.truncate("year"),
)

query_3 = (
    flight_st_with_year.group_by("dep_year")
    .aggregate(flight_count=lambda t: t.count())
    .mutate(
        last_year=lambda t: t.flight_count.lag(1),
        growth=lambda t: (t.flight_count.lag(1) - t.flight_count) / t.flight_count.lag(1),
    )
    .order_by("dep_year")
)
