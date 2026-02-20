#!/usr/bin/env python3
"""Round-trip example: SemanticTable → to_tagged → from_tagged → query.

Demonstrates serializing a BSL semantic model into xorq tagged metadata
and reconstructing it back, preserving dimensions, measures, filters,
and full query pipelines.
"""

import xorq.api as xo
from ibis import _

from boring_semantic_layer import to_semantic_table
from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def main():
    flights_tbl = xo.deferred_read_parquet(f"{BASE_URL}/flights.parquet")

    # --- Build a semantic model ---
    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            origin=lambda t: t.origin,
            destination=lambda t: t.destination,
            carrier=lambda t: t.carrier,
        )
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
            avg_distance=lambda t: t.distance.mean(),
            short_flight_count=lambda t: xo.case()
            .when(t.distance < 500, 1)
            .else_(0)
            .end()
            .sum(),
        )
    )

    # --- Serialize to xorq tagged expression ---
    tagged_expr = to_tagged(flights)
    print("Tagged expression created:", type(tagged_expr).__name__)

    # --- Reconstruct from tagged metadata ---
    reconstructed = from_tagged(tagged_expr)
    print("Reconstructed model:", type(reconstructed).__name__)

    # --- Query the reconstructed model ---
    result = (
        reconstructed.group_by("origin")
        .aggregate("flight_count", "avg_distance")
        .order_by(_.flight_count.desc())
        .limit(10)
        .execute()
    )
    print("\nFlight counts by origin (from reconstructed model):")
    print(result)

    result = (
        reconstructed.group_by("carrier")
        .aggregate("flight_count", "total_distance", "short_flight_count")
        .order_by(_.flight_count.desc())
        .limit(10)
        .execute()
    )
    print("\nCarrier stats with case-expression measure (from reconstructed model):")
    print(result)

    # --- Round-trip a filtered pipeline ---
    long_haul = reconstructed.filter(lambda t: t.distance > 1000)
    tagged_filtered = to_tagged(long_haul)
    reconstructed_filtered = from_tagged(tagged_filtered)

    result = (
        reconstructed_filtered.group_by("carrier")
        .aggregate("flight_count", "avg_distance")
        .order_by(_.avg_distance.desc())
        .limit(10)
        .execute()
    )
    print("\nLong-haul flights after double round-trip:")
    print(result)


if __name__ == "__main__":
    main()
