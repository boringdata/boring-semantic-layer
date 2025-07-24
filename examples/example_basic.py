"""
Example: Basic Query with Semantic Model (YAML Loading)

This example demonstrates how to use semantic models loaded from YAML to perform a basic query, retrieving available dimensions and measures, and running a grouped and aggregated query with ordering and limiting.

YAML File: `example_basic.yml`
- Defines both `carriers` and `flights` models
- Includes joins between models
- Uses Ibis deferred expressions with `_` placeholder

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
from boring_semantic_layer import SemanticModel

con = ibis.duckdb.connect(":memory:")

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"
tables = {
    "flights_tbl": con.read_parquet(f"{BASE_URL}/flights.parquet"),
    "carriers_tbl": con.read_parquet(f"{BASE_URL}/carriers.parquet"),
}

models = SemanticModel.from_yaml("example_basic.yml", tables=tables)

carriers_sm = models["carriers"]
flights_sm = models["flights"]

if __name__ == "__main__":
    print("Available dimensions:", flights_sm.available_dimensions)
    print("Available measures:", flights_sm.available_measures)

    print("\n=== Example Query ===")
    expr = flights_sm.query(
        dimensions=["destination"],
        measures=["flight_count", "avg_distance"],
        order_by=[("flight_count", "desc")],
        limit=10,
    )
    df = expr.execute()
    print("Top 10 destinations by flight count:")
    print(df)

    print("\n=== Query with Join ===")
    expr_join = flights_sm.query(
        dimensions=["carrier", "carriers.name"],
        measures=["flight_count"],
        order_by=[("flight_count", "desc")],
        limit=5,
    )
    df_join = expr_join.execute()
    print("Top 5 carriers by flight count:")
    print(df_join)
