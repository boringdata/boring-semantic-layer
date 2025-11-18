#!/usr/bin/env python3
"""Advanced xorq Features with BSL.

Demonstrates:
1. Manual caching: Explicitly call .cache() on the full expression
2. Auto caching: Automatic cache injection at aggregation points
"""

import ibis

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.xorq_convert import to_xorq

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def manual_caching_example():
    """Example: Manually cache the entire expression."""

    duckdb_con = ibis.duckdb.connect(":memory:")
    flights_tbl = duckdb_con.read_parquet(f"{BASE_URL}/flights.parquet")

    flights = to_semantic_table(flights_tbl, name="flights").with_measures(
        flight_count=lambda t: t.count(),
        total_distance=lambda t: t.distance.sum(),
        avg_delay=lambda t: t.dep_delay.mean(),
    )

    query = (
        flights.filter(lambda t: t.distance > 500)
        .group_by("carrier", "origin")
        .aggregate("flight_count", "total_distance", "avg_delay")
        .filter(lambda t: t.flight_count > 50)
        .order_by(lambda t: ibis.desc(t.total_distance))
        .limit(20)
    )

    import xorq.api as xo
    from xorq.caching import SourceStorage

    xo_con = xo.connect()

    xorq_expr = to_xorq(query).cache(storage=SourceStorage(source=xo_con))
    result = xorq_expr.execute()

    print(f"Result shape: {result.shape}")
    print(f"\nCached tables:\n{xo_con.tables}")
    print()


def smart_caching_example():
    """Example: Automatically cache at aggregation points."""

    duckdb_con = ibis.duckdb.connect(":memory:")
    flights_tbl = duckdb_con.read_parquet(f"{BASE_URL}/flights.parquet")

    flights = to_semantic_table(flights_tbl, name="flights").with_measures(
        flight_count=lambda t: t.count(),
        total_distance=lambda t: t.distance.sum(),
        avg_delay=lambda t: t.dep_delay.mean(),
    )

    query = (
        flights.filter(lambda t: t.distance > 500)
        .group_by("carrier", "origin")
        .aggregate("flight_count", "total_distance", "avg_delay")  # ← Auto-cached here
        .filter(lambda t: t.flight_count > 50)
        .order_by(lambda t: ibis.desc(t.total_distance))
        .limit(20)
    )

    import xorq.api as xo
    from xorq.caching import ParquetStorage

    xo_con = xo.connect()
    storage = ParquetStorage(source=xo_con)

    xorq_expr = to_xorq(query, aggregate_cache_storage=storage)
    result = xorq_expr.execute()

    print(f"Result shape: {result.shape}")
    print(f"\nCached tables:\n{xo_con.tables}")
    # Other queries with the same aggregation and storage will reuse this cache.


def main():
    manual_caching_example()
    smart_caching_example()


if __name__ == "__main__":
    main()
