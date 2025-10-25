#!/usr/bin/env python3
"""Bucketing with 'Other' - Top N with Rollup Pattern.

Malloy: https://malloydata.github.io/malloy/documentation/patterns/other.html
"""
import ibis
from ibis import _
from boring_semantic_layer import to_semantic_table

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def main():
    con = ibis.duckdb.connect(":memory:")
    airports_tbl = con.read_parquet(f"{BASE_URL}/airports.parquet")

    airports = to_semantic_table(airports_tbl, name="airports").with_measures(
        avg_elevation=lambda t: t.elevation.mean()
    )

    result = (
        airports.group_by("state")
        .aggregate(
            "avg_elevation",
            nest={"data": lambda t: t.group_by(["code", "elevation"])},
        )
        .mutate(
            rank=ibis.row_number().over(
                ibis.window(order_by=ibis.desc("avg_elevation"))
            ),
            is_other=_.rank > 4,
            state_grouped=ibis.cases((_.is_other, "OTHER"), else_=_.state),
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
