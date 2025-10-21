import ibis
from boring_semantic_layer.semantic_api import to_semantic_table

con = ibis.duckdb.connect()

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"

# Load the tables
flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")
carriers_tbl = con.read_parquet(f"{BASE_URL}/carriers.parquet")

# Create joined table first, then make it semantic
flights_with_carriers = flights_tbl.join(
    carriers_tbl, flights_tbl.carrier == carriers_tbl.code, how="inner"
)

# Convert to semantic table with measures
flights_st = to_semantic_table(flights_with_carriers).with_measures(
    flight_count=lambda t: t.count()
)

# BSL equivalent of the Malloy percent_of_total queries

# Query 1: Basic all() function
# Malloy query:
# query: query_1 is flights -> {
#   group_by: carriers.nickname
#   aggregate:
#     flight_count
#     all_flights is all(flight_count)
#     limit: 2
# }
query_1 = (
    flights_st.group_by("nickname")
    .aggregate(flight_count=lambda t: t.count())
    .mutate(
        # all_flights: grand total via t.all()
        all_flights=lambda t: t.all(t.flight_count)
    )
    .order_by(ibis.desc("flight_count"))
    .limit(2)
)

# Query 2: Percent calculation
# Malloy query:
# query: query_2 is flights -> {
#   group_by: carriers.nickname
#   aggregate:
#     flight_count
#     percent_of_flights is flight_count / all(flight_count)
#     limit: 5
# }
query_2 = (
    flights_st.group_by("nickname")
    .aggregate(flight_count=lambda t: t.count())
    .mutate(
        percent_of_flights=lambda t: t.flight_count / t.all(t.flight_count)
    )
    .order_by(ibis.desc("flight_count"))
    .limit(5)
)

# Query 3: Complex partitioned aggregations
# Malloy query:
# query: query_3 is flights -> {
#   group_by: carriers.nickname, destination, origin
#   aggregate:
#     flight_count
#     flights_by_this_carrier is all(flight_count, nickname)
#     flights_to_this_destination is all(flight_count, destination)
#     flights_by_this_origin is all(flight_count, origin)
#     flights_on_this_route is exclude(flight_count, nickname)
#   limit: 20
# }
query_3 = (
    flights_st.group_by("nickname", "destination", "origin")
    .aggregate(flight_count=lambda t: t.count())
    .mutate(
        # all(flight_count, nickname) - sum partitioned by nickname
        flights_by_this_carrier=lambda t: t.flight_count.sum().over(
            ibis.window(group_by="nickname")
        ),
        # all(flight_count, destination) - sum partitioned by destination
        flights_to_this_destination=lambda t: t.flight_count.sum().over(
            ibis.window(group_by="destination")
        ),
        # all(flight_count, origin) - sum partitioned by origin
        flights_by_this_origin=lambda t: t.flight_count.sum().over(
            ibis.window(group_by="origin")
        ),
    )
    .mutate(
        # exclude(flight_count, nickname) - flights on this route (destination+origin), excluding carrier dimension
        flights_on_this_route=lambda t: t.flight_count.sum().over(
            ibis.window(group_by=["destination", "origin"])
        ),
    )
    .order_by("nickname", "destination", "origin")
    .limit(20)
)

# Query 4: Multiple percent calculations
# Malloy query:
# query: query_4 is flights -> {
#   group_by: carriers.nickname, destination, origin
#   aggregate:
#     flight_count
#     `carrier as a percent of all flights` is all(flight_count, nickname) / all(flight_count)
#     `destination as a percent of all flights` is all(flight_count, destination) / all(flight_count)
#     `origin as a percent of all flights` is all(flight_count, origin) / all(flight_count)
#     `carriers as a percentage of route` is flight_count / exclude(flight_count, nickname)
# }
query_4 = (
    flights_st.group_by("nickname", "destination", "origin")
    .aggregate(flight_count=lambda t: t.count())
    .mutate(
        # Direct calculations without intermediate variables
        **{
            "carrier as a percent of all flights": lambda t: t.flight_count.sum().over(
                ibis.window(group_by="nickname")
            )
            / t.flight_count.sum().over(),
            "destination as a percent of all flights": lambda t: t.flight_count.sum().over(
                ibis.window(group_by="destination")
            )
            / t.flight_count.sum().over(),
            "origin as a percent of all flights": lambda t: t.flight_count.sum().over(
                ibis.window(group_by="origin")
            )
            / t.flight_count.sum().over(),
            "carriers as a percentage of route": lambda t: t.flight_count
            / t.flight_count.sum().over(
                ibis.window(group_by=["destination", "origin"])
            ),
        }
    )
    .order_by("nickname", "destination", "origin")
)
