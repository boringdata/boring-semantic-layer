#!/usr/bin/env python3
"""Joining Semantic Tables - Foreign Sums and Averages.
https://docs.malloydata.dev/documentation/patterns/foreign_sums

This example demonstrates:
1. Basic joins between semantic tables using YAML configuration
2. Handling name conflicts when joining tables with overlapping column names
"""

from pathlib import Path

import ibis

from boring_semantic_layer import from_yaml


def main():
    # Load semantic models from YAML with profile
    yaml_path = Path(__file__).parent / "flights.yml"
    profile_file = Path(__file__).parent / "profiles.yml"
    models = from_yaml(str(yaml_path), profile="example_db", profile_path=str(profile_file))

    # Get semantic models from YAML
    flights = models["flights"]
    aircraft = models["aircraft"]

    # ========================================================================
    # Example 1: Basic joins (multi-level) - defined in YAML
    # ========================================================================
    print("\n" + "=" * 70)
    print("Example 1: Multi-level joins (flights -> aircraft -> models)")
    print("=" * 70)

    flights_by_origin = flights.group_by("origin").aggregate("flight_count").limit(10).execute()
    print("\nFlights by origin:")
    print(flights_by_origin)

    aircraft_by_type = (
        aircraft.group_by("aircraft_type_id").aggregate("aircraft_count").limit(10).execute()
    )
    print("\nAircraft by type:")
    print(aircraft_by_type)

    # ========================================================================
    # Example 2: Query joined models - carriers and airports
    # ========================================================================
    print("\n" + "=" * 70)
    print("Example 2: Flights with carrier nickname and origin city")
    print("=" * 70)

    # Flights model has joins to carriers and airports defined in YAML
    # We can access their dimensions through the join relationship
    result = (
        flights.group_by("origin", "carrier")
        .aggregate("flight_count", "total_distance")
        .order_by(ibis._.flight_count.desc())
        .limit(10)
        .execute()
    )

    print("\nTop routes by carrier and origin:")
    print(result)


if __name__ == "__main__":
    main()
