#!/usr/bin/env python3
"""
Example 1: Basic Semantic Table Usage with Flights Data

This example demonstrates the core concepts of the Boring Semantic Layer:
- Creating a semantic table from raw data
- Defining dimensions and measures UPFRONT (not in mutate steps!)
- Using the fluent API for queries
- Both lambda and Ibis deferred expression syntax

**KEY PRINCIPLE**: Push complex metric calculations into the semantic layer as measures.
Don't fall back to `.mutate()` or raw Ibis for metric calculations!

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

    # ============================================================================
    # STEP 1: Define Dataset
    # ============================================================================
    print("\n" + "-" * 80)
    print("STEP 1: Define Dataset")
    print("-" * 80)

    con = ibis.duckdb.connect(":memory:")

    # Sample flights data
    flights_df = pd.DataFrame(
        {
            "origin": ["JFK", "LAX", "JFK", "ORD", "LAX", "JFK", "ORD", "LAX"],
            "destination": ["LAX", "JFK", "ORD", "JFK", "ORD", "LAX", "LAX", "ORD"],
            "distance": [2475, 2475, 740, 740, 1744, 2475, 987, 1744],
            "carrier": ["AA", "UA", "AA", "UA", "AA", "UA", "AA", "UA"],
        }
    )

    flights_tbl = con.create_table("flights", flights_df)

    print("\n📊 Raw data loaded:")
    print(flights_df)

    # ============================================================================
    # STEP 2: Compute Semantic Layer - Define ALL measures upfront
    # ============================================================================
    print("\n" + "-" * 80)
    print("STEP 2: Compute Semantic Layer - Define dimensions AND measures")
    print("-" * 80)
    print(
        "\n💡 KEY INSIGHT: Define ALL your metrics as measures in the semantic layer."
    )
    print("   This makes them reusable and distributes calculation logic to users.")

    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            origin=lambda t: t.origin,
            destination=lambda t: t.destination,
            carrier=lambda t: t.carrier,
        )
        .with_measures(
            # Basic counts and sums
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
            # Statistical measures - define these as measures, not in mutate!
            avg_distance=lambda t: t.distance.mean(),
            max_distance=lambda t: t.distance.max(),
            min_distance=lambda t: t.distance.min(),
            # Complex calculated measures can also be defined here
            # (we'll add more complex ones later)
        )
    )

    print("\n✓ Semantic table created with:")
    print(f"  Dimensions: {list(flights.dimensions.keys())}")
    print(f"  Measures: {flights.measures}")

    # ============================================================================
    # STEP 3: Query the Semantic Layer
    # ============================================================================
    print("\n" + "=" * 80)
    print("STEP 3: Query the Semantic Layer")
    print("=" * 80)

    # Query 1: Simple aggregation
    print("\n" + "-" * 80)
    print("Query 1: Flight counts by origin - Using semantic measures")
    print("-" * 80)

    result = flights.group_by("origin").aggregate("flight_count").execute()

    print(result)

    # Query 2: Multiple dimensions and measures
    print("\n" + "-" * 80)
    print("Query 2: Multiple dimensions and measures")
    print("-" * 80)

    result = (
        flights.group_by("origin", "carrier")
        .aggregate("flight_count", "avg_distance")
        .order_by(_.flight_count.desc())
        .execute()
    )

    print(result)

    # Query 3: Using ALL semantic measures (not ad-hoc!)
    print("\n" + "-" * 80)
    print("Query 3: Use ALL pre-defined measures from semantic layer")
    print("-" * 80)
    print("✓ No ad-hoc lambdas - all metrics defined in semantic layer!")

    result = (
        flights.group_by("origin")
        .aggregate(
            "flight_count",
            "max_distance",  # Defined in semantic layer
            "min_distance",  # Defined in semantic layer
        )
        .order_by(_.flight_count.desc())
        .execute()
    )

    print(result)

    # Query 4: Show how to add more complex calculated measures
    print("\n" + "-" * 80)
    print("Query 4: Add complex calculated measure to semantic layer")
    print("-" * 80)
    print("Adding 'distance_per_flight' as a reusable measure...")

    # Add a new calculated measure to the semantic layer
    flights_enhanced = flights.with_measures(
        # Complex measure: average distance per flight
        # This is computed correctly at aggregation time
        distance_per_flight=lambda t: t.distance.sum() / t.count(),
    )

    result = (
        flights_enhanced.group_by("carrier")
        .aggregate("flight_count", "total_distance", "distance_per_flight")
        .order_by(_.distance_per_flight.desc())
        .execute()
    )

    print(result)
    print("\n✓ 'distance_per_flight' is now a reusable measure!")

    # Query 5: Filtering with semantic layer measures
    print("\n" + "-" * 80)
    print("Query 5: Filter data, keep using semantic layer measures")
    print("-" * 80)
    print("💡 Filters can be applied before aggregation without losing measures!")

    # Filter and create a new semantic table that inherits the same measure structure
    long_haul_flights = flights_enhanced.filter(lambda t: t.distance > 1000)

    result = (
        long_haul_flights.group_by("carrier")
        .aggregate("flight_count", "avg_distance")  # Same measures from SL!
        .execute()
    )

    print("Long-haul flights (>1000 miles) by carrier:")
    print(result)
    print("\n✓ Filtered data still uses semantic layer measures!")

    # Query 6: Using deferred syntax
    print("\n" + "-" * 80)
    print("Query 6: Same pattern with Ibis deferred syntax (_.col)")
    print("-" * 80)

    result = (
        flights_enhanced.group_by("carrier")
        .aggregate("flight_count", "total_distance", "distance_per_flight")
        .order_by(_.distance_per_flight.desc())
        .execute()
    )

    print(result)

    print("\n" + "=" * 80)
    print("✅ Example completed successfully!")
    print("=" * 80)
    print("\n🎯 KEY TAKEAWAYS:")
    print("  ✓ Structure: 1) Define dataset, 2) Compute SL, 3) Query SL")
    print("  ✓ Define ALL measures in .with_measures() - not in .mutate()!")
    print("  ✓ Measures are reusable across queries")
    print("  ✓ Complex calculations belong in semantic layer as measures")
    print("  ✓ Users can combine and query pre-defined measures")
    print("  ✓ Both lambda and _ deferred syntax work")
    print("\n📚 Next: See percent_of_total.py for market share calculations")
    print()


if __name__ == "__main__":
    main()
