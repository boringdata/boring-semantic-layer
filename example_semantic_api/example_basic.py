"""
Example: Single Semantic Table with Joins (Malloy-style YAML Loading)

This example demonstrates the new Malloy-style semantic API where one YAML file defines
one main table with inline joins. Shows how to load and query a semantic table with joins.

YAML File: `example_basic.yml` (Malloy-style)
- Defines `flights` as the main table
- Includes inline `carriers` join with semantic definitions
- Uses Ibis deferred expressions with `_` placeholder

Features demonstrated:
- Single-table API: SemanticTable.from_yaml()
- Inline join definitions with dimensions/measures
- Querying main table dimensions and measures
- Accessing joined table data

Expected Output (example):

| destination | flight_count | avg_distance |
|-------------|-------------|--------------|
|     JFK     |    1200     |    1450.2    |
|     LAX     |    1100     |    2100.5    |
|     ORD     |    950      |    980.7     |
|    ...      |    ...      |     ...      |

"""

import ibis
from boring_semantic_layer.semantic_api.ops import SemanticTable

con = ibis.duckdb.connect(":memory:")

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"
tables = {
    "flights_tbl": con.read_parquet(f"{BASE_URL}/flights.parquet"),
    "carriers_tbl": con.read_parquet(f"{BASE_URL}/carriers.parquet"),
}

flights_sm = SemanticTable.from_yaml("example_basic.yml", tables=tables)

if __name__ == "__main__":
    print("=== Single Table API Demo ===")
    print(f"Loaded semantic table: {type(flights_sm).__name__}")

    print("\n=== Basic Query: Top Destinations ===")

    # Group by destination and aggregate flight count and average distance
    expr = (
        flights_sm.group_by("destination")
        .aggregate(
            flight_count=lambda t: t.count(), avg_distance=lambda t: t.distance.mean()
        )
        .order_by(ibis.desc("flight_count"))
        .limit(10)
    )
    df = expr.execute()
    print("Top 10 destinations by flight count:")
    print(df)

    print("\n=== Carrier Analysis ===")

    # Group by carrier code - using main table dimension
    expr_carrier = (
        flights_sm.group_by("carrier")
        .aggregate(
            flight_count=lambda t: t.count(), avg_dep_delay=lambda t: t.dep_delay.mean()
        )
        .order_by(ibis.desc("flight_count"))
        .limit(5)
    )
    df_carrier = expr_carrier.execute()
    print("Top 5 carriers by flight count (with carrier codes):")
    print(df_carrier)

    print("\n=== Joined Table Dimension: Carrier Nicknames ===")

    # Group by carrier nickname - accessing joined table dimension
    # Note: Currently need to use the direct dimension name, not alias.dimension syntax
    expr_nickname = (
        flights_sm.group_by("nickname")
        .aggregate(
            flight_count=lambda t: t.count(), avg_dep_delay=lambda t: t.dep_delay.mean()
        )
        .order_by(ibis.desc("flight_count"))
        .limit(5)
    )
    df_nickname = expr_nickname.execute()
    print("Top 5 carriers by flight count (with nicknames from joined table):")
    print(df_nickname)

    print("\n=== Time-based Analysis ===")

    # Group by origin and show flight patterns
    expr_time = (
        flights_sm.group_by("origin")
        .aggregate(
            flight_count=lambda t: t.count(),
            avg_distance=lambda t: t.distance.mean(),
            total_delay=lambda t: t.dep_delay.sum(),
        )
        .order_by(ibis.desc("avg_distance"))
        .limit(5)
    )
    df_time = expr_time.execute()
    print("Origins by average distance (longest routes):")
    print(df_time)
