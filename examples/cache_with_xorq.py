#!/usr/bin/env python3
"""Advanced xorq Features with BSL.

Demonstrates:
1. Manual caching: Explicitly call .cache() on the full expression
2. Auto caching: Automatic cache injection at aggregation points
"""

from pathlib import Path

import ibis

from boring_semantic_layer import from_yaml
from boring_semantic_layer.xorq_convert import to_xorq


def manual_caching_example():
    """Example: Manually cache the entire expression."""

    # Load semantic models from YAML with profile
    yaml_path = Path(__file__).parent / "flights.yml"
    profile_file = Path(__file__).parent / "profiles.yml"
    models = from_yaml(str(yaml_path), profile="example_db", profile_path=str(profile_file))

    # Use flights model from YAML (already has all measures)
    flights = models["flights"]

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

    # Load semantic models from YAML with profile
    yaml_path = Path(__file__).parent / "flights.yml"
    profile_file = Path(__file__).parent / "profiles.yml"
    models = from_yaml(str(yaml_path), profile="example_db", profile_path=str(profile_file))

    # Use flights model from YAML (already has all measures)
    flights = models["flights"]

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
