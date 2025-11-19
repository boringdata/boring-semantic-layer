#!/usr/bin/env python3
"""
Basic Flights Example with Profiles and YAML

Demonstrates loading semantic models from YAML configuration with profile-based
database connections.
"""

from pathlib import Path

from ibis import _

from boring_semantic_layer import from_yaml


def main():
    # Load semantic models from YAML with profile
    yaml_path = Path(__file__).parent / "flights.yml"
    profile_file = Path(__file__).parent / "profiles.yml"
    models = from_yaml(str(yaml_path), profile="example_db", profile_path=str(profile_file))

    flights = models["flights"]

    result = flights.group_by("origin").aggregate("flight_count").limit(10).execute()
    print("\nFlight counts by origin:")
    print(result)

    result = (
        flights.group_by("origin", "carrier")
        .aggregate("flight_count", "avg_distance")
        .order_by(_.flight_count.desc())
        .limit(10)
        .execute()
    )
    print("\nFlights by origin and carrier:")
    print(result)

    # Add calculated measure dynamically
    flights_enhanced = flights.with_measures(
        distance_per_flight=lambda t: t.distance.sum() / t.count(),
    )

    result = (
        flights_enhanced.group_by("carrier")
        .aggregate("flight_count", "total_distance", "distance_per_flight")
        .order_by(_.distance_per_flight.desc())
        .limit(10)
        .execute()
    )
    print("\nDistance per flight by carrier:")
    print(result)

    long_haul_flights = flights_enhanced.filter(lambda t: t.distance > 1000)
    result = (
        long_haul_flights.group_by("carrier")
        .aggregate("flight_count", "avg_distance")
        .limit(10)
        .execute()
    )
    print("\nLong-haul flights (>1000 miles):")
    print(result)


if __name__ == "__main__":
    main()
