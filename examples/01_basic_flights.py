#!/usr/bin/env python3
"""
Example 1: Basic Semantic Table Usage with Flights Data

This example demonstrates the core concepts of the Boring Semantic Layer:
- Creating a semantic table from raw data
- Defining dimensions and measures
- Using the fluent API for queries
- Both lambda and Ibis deferred expression syntax

The flights dataset contains information about flights between airports including
origin, destination, distance, and carrier information.
"""

import pandas as pd
import ibis
from ibis import _
from boring_semantic_layer.semantic_api import to_semantic_table


def main():
    print("=" * 80)
    print("  Example 1: Basic Semantic Table Usage")
    print("=" * 80)

    # 1. Set up connection and load data
    con = ibis.duckdb.connect(":memory:")

    # Sample flights data
    flights_df = pd.DataFrame({
        "origin": ["JFK", "LAX", "JFK", "ORD", "LAX", "JFK", "ORD", "LAX"],
        "destination": ["LAX", "JFK", "ORD", "JFK", "ORD", "LAX", "LAX", "ORD"],
        "distance": [2475, 2475, 740, 740, 1744, 2475, 987, 1744],
        "carrier": ["AA", "UA", "AA", "UA", "AA", "UA", "AA", "UA"],
    })

    flights_tbl = con.create_table("flights", flights_df)

    print("\n📊 Raw data loaded:")
    print(flights_df)

    # 2. Create a semantic table
    print("\n" + "-" * 80)
    print("Step 1: Convert to semantic table and define dimensions/measures")
    print("-" * 80)

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
        )
    )

    print("✓ Semantic table created with:")
    print(f"  - Dimensions: {flights.dimensions}")
    print(f"  - Measures: {flights.measures}")

    # 3. Simple aggregation
    print("\n" + "-" * 80)
    print("Query 1: Flight counts by origin")
    print("-" * 80)

    result = (
        flights
        .group_by("origin")
        .aggregate("flight_count")
        .execute()
    )

    print(result)

    # 4. Multiple dimensions
    print("\n" + "-" * 80)
    print("Query 2: Average distance by origin and carrier")
    print("-" * 80)

    result = (
        flights
        .group_by("origin", "carrier")
        .aggregate("flight_count", "avg_distance")
        .order_by(_.flight_count.desc())
        .execute()
    )

    print(result)

    # 5. Ad-hoc measures
    print("\n" + "-" * 80)
    print("Query 3: Mix semantic measures with ad-hoc aggregations")
    print("-" * 80)

    result = (
        flights
        .group_by("origin")
        .aggregate(
            "flight_count",  # Semantic measure
            max_distance=lambda t: t.distance.max(),  # Ad-hoc measure
            min_distance=lambda t: t.distance.min(),
        )
        .order_by(_.flight_count.desc())
        .execute()
    )

    print(result)

    # 6. Post-aggregation calculations
    print("\n" + "-" * 80)
    print("Query 4: Calculate average distance per flight")
    print("-" * 80)

    result = (
        flights
        .group_by("carrier")
        .aggregate("flight_count", "total_distance")
        .mutate(
            avg_distance_per_flight=lambda t: t["total_distance"] / t["flight_count"]
        )
        .order_by(_.avg_distance_per_flight.desc())
        .execute()
    )

    print(result)

    # 7. Using Ibis deferred expressions
    print("\n" + "-" * 80)
    print("Query 5: Same query using Ibis deferred syntax (_.col)")
    print("-" * 80)

    result = (
        flights
        .group_by("carrier")
        .aggregate("flight_count", "total_distance")
        .mutate(
            avg_distance_per_flight=_.total_distance / _.flight_count
        )
        .order_by(_.avg_distance_per_flight.desc())
        .execute()
    )

    print(result)

    print("\n" + "=" * 80)
    print("✅ Example completed successfully!")
    print("=" * 80)
    print("\nKey Takeaways:")
    print("  • Use .with_dimensions() and .with_measures() to define semantics")
    print("  • Chain .group_by() → .aggregate() for queries")
    print("  • Mix semantic measures with ad-hoc aggregations")
    print("  • Use .mutate() for post-aggregation calculations")
    print("  • Both lambda and _ deferred syntax work")
    print("\nNext: See 02_percent_of_total.py for market share calculations")
    print()


if __name__ == "__main__":
    main()
