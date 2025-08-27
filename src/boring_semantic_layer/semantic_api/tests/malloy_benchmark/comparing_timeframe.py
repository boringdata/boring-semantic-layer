import ibis
from boring_semantic_layer.semantic_api.api import to_semantic_table

con = ibis.duckdb.connect("test.duckdb")

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"

tables = {
    "flights_tbl": con.read_parquet(f"{BASE_URL}/flights.parquet"),
    "carriers_tbl": con.read_parquet(f"{BASE_URL}/carriers.parquet"),
}

flight_st = to_semantic_table(tables["flights_tbl"]).with_dimensions(
    flight_count=lambda t: t.count(),
    month_of_year=lambda t: t.arr_time.month(),
    flight_year=lambda t: t.arr_time.truncate("year"),
).filter(lambda t: t.arr_time.year() > 1900)

query_1 = flight_st.group_by(
    "month_of_year", "flight_year"
).aggregate(
    flight_count=lambda t: t.count()
)#.order_by(["month_of_year", "flight_year"])
