"""
Example: Basic Query with Semantic Model

This example demonstrates how to use a semantic model (`flights_sm`) to perform a basic query, retrieving available dimensions and measures, and running a grouped and aggregated query with ordering and limiting.

Semantic Model: `flights_sm`
- Represents a flights dataset with dimensions such as destination and measures such as flight count and average distance.

Query:
- Dimensions: destination
- Measures: flight_count, avg_distance
- Order by: flight_count (descending)
- Limit: 10

Expected Output (example):

| destination | flight_count | avg_distance |
|-------------|-------------|--------------|
|     JFK     |    1200     |    1450.2    |
|     LAX     |    1100     |    2100.5    |
|     ORD     |    950      |    980.7     |
|    ...      |    ...      |     ...      |

"""

import ibis
from boring_semantic_layer import SemanticModel, Join

con = ibis.duckdb.connect(":memory:")

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"
flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")
carriers_tbl = con.read_parquet(f"{BASE_URL}/carriers.parquet")

carriers_sm = SemanticModel(
    name="carriers",
    table=carriers_tbl,
    dimensions={
        "code": lambda t: t.code,
        "name": lambda t: t.name,
        "nickname": lambda t: t.nickname,
    },
    measures={
        "carrier_count": lambda t: t.count(),
    },
    primary_key="code",
)

flights_sm = SemanticModel(
    name="flights",
    table=flights_tbl,
    dimensions={
        "origin": lambda t: t.origin,
        "destination": lambda t: t.destination,
        "carrier": lambda t: t.carrier,
        "tail_num": lambda t: t.tail_num,
        "arr_time": lambda t: t.arr_time,
    },
    time_dimension="arr_time",
    smallest_time_grain="TIME_GRAIN_SECOND",
    measures={
        "flight_count": lambda t: t.count(),
        "avg_dep_delay": lambda t: t.dep_delay.mean(),
        "avg_distance": lambda t: t.distance.mean(),
    },
    joins={
        "carriers": Join.one(
            alias="carriers",
            model=carriers_sm,
            with_=lambda left: left.carrier,
        ),
    },
)

if __name__ == "__main__":
    print("Available dimensions:", flights_sm.available_dimensions)
    print("Available measures:", flights_sm.available_measures)

    expr = flights_sm.query(
        dimensions=["destination"],
        measures=["flight_count", "avg_distance"],
        order_by=[("flight_count", "desc")],
        limit=10,
    )
    df = expr.execute()
    print("\nTop 10 carriers by flight count:")
    print(df)
