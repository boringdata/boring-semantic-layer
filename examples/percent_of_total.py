#!/usr/bin/env python3
"""Percent of Total - Market Share Analysis.

Malloy: https://docs.malloydata.dev/documentation/patterns/percent_of_total
"""

from pathlib import Path

from ibis import _

from boring_semantic_layer import from_yaml


def main():
    # Load semantic models from YAML with profile
    yaml_path = Path(__file__).parent / "flights.yml"
    profile_file = Path(__file__).parent / "profiles.yml"
    models = from_yaml(str(yaml_path), profile="example_db", profile_path=str(profile_file))

    # Use flights model from YAML (already has carriers join and distance_share measure)
    flights = models["flights"]

    result = (
        flights.group_by("carriers__nickname")
        .aggregate("flight_count", "distance_share")
        .order_by(_.distance_share.desc())
        .limit(10)
        .execute()
    )
    print("\nMarket share by carrier:")
    print(result)


if __name__ == "__main__":
    main()
