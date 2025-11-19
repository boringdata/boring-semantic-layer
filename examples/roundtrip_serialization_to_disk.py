#!/usr/bin/env python3
"""Roundtrip Serialization to Disk with BSL and xorq."""

from pathlib import Path

import ibis

from boring_semantic_layer import from_yaml
from boring_semantic_layer.xorq_convert import from_xorq, to_xorq


def main():
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
