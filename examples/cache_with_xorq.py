#!/usr/bin/env python3
"""Advanced xorq Features with BSL.

Demonstrates:
Deterministic caching
"""

import ibis

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.xorq_convert import to_xorq

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def main():
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
    from xorq.caching import SourceStorage  # or choose ParquetStorage for disk caching

    xo_con = xo.connect()

    xorq_expr = to_xorq(query).cache(storage=SourceStorage(source=xo_con))
    xorq_expr.execute()

    print(xo_con.tables)


if __name__ == "__main__":
    main()
