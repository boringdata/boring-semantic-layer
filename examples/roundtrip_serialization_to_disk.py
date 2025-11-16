#!/usr/bin/env python3
"""Roundtrip Serialization to Disk with BSL and xorq.
"""

import ibis

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.xorq_convert import from_xorq, to_xorq

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

    xorq_expr = to_xorq(query)

    from xorq.ibis_yaml.compiler import BuildManager

    build_manager = BuildManager("builds")
    expr_hash = build_manager.compile_expr(xorq_expr)

    print(f"Expression saved to disk with hash: {expr_hash}")

    roundtrip_expr = from_xorq(build_manager.load_expr(expr_hash))

    print("Expression successfully loaded from disk")

    print("\nExecuting original expression:")
    original_result = query.execute()
    print(original_result)

    print("\nExecuting roundtrip expression:")
    roundtrip_result = roundtrip_expr.execute()
    print(roundtrip_result)

    print("\nResults match:", original_result.equals(roundtrip_result))


if __name__ == "__main__":
    main()
