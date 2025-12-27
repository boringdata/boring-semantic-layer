#!/usr/bin/env python3
"""Bucketing with 'Other' - Top N with Rollup Pattern.

Malloy: https://docs.malloydata.dev/documentation/patterns/other
"""

from pathlib import Path

import xorq.api as xo
from ibis import _

from boring_semantic_layer import from_yaml


def main():
    # Load semantic models from YAML with profile
    yaml_path = Path(__file__).parent / "flights.yml"
    profile_file = Path(__file__).parent / "profiles.yml"
    models = from_yaml(str(yaml_path), profile="example_db", profile_path=str(profile_file))

    # Use airports model from YAML (already has avg_elevation measure)
    airports = models["airports"]

    result = (
        airports.group_by("state")
        .aggregate(
            "avg_elevation",
            nest={"data": lambda t: t.group_by(["code", "elevation"])},
        )
        .mutate(
            rank=lambda t: xo.row_number().over(
                xo.window(order_by=xo.desc(t.avg_elevation)),
            ),
        )
        .mutate(
            is_other=lambda t: t.rank > 4,
            state_grouped=lambda t: xo.ifelse(t.rank > 4, "OTHER", t.state),
        )
        .group_by("state_grouped")
        .aggregate(
            airport_count=_.data.count(),
            avg_elevation=_.data.elevation.mean(),
        )
        .order_by(_.avg_elevation.desc())
        .execute()
    )

    print("\nTop 5 States by Elevation + OTHER:")
    print(result)
    print(f"\nTotal airports: {result['airport_count'].sum():,}")


if __name__ == "__main__":
    main()
