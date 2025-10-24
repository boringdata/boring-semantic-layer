#!/usr/bin/env python3
"""
Example 2: Percent of Total (Market Share Analysis)

This example demonstrates calculating percentages of grand totals - commonly used
for market share, contribution analysis, and relative performance metrics.

**KEY PRINCIPLE**: Define base aggregation measures in the semantic layer.
For percentage calculations using t.all(), use .mutate() after .aggregate().

Structure:
1. Define dataset
2. Compute Semantic Layer with base measures
3. Query SL and calculate percentages with t.all() in mutate
"""

import pandas as pd
import ibis
from ibis import _
from boring_semantic_layer import to_semantic_table


def main():
    print("=" * 80)
    print("  Example 2: Percent of Total / Market Share")
    print("=" * 80)

    # ============================================================================
    # STEP 1: Define Dataset
    # ============================================================================
    print("\n" + "-" * 80)
    print("STEP 1: Define Dataset")
    print("-" * 80)

    con = ibis.duckdb.connect(":memory:")

    # Flights data with carriers
    flights_df = pd.DataFrame(
        {
            "origin": ["JFK"] * 4 + ["LAX"] * 4 + ["ORD"] * 4,
            "destination": ["LAX", "ORD", "MIA", "DFW"] * 3,
            "distance": [2475, 740, 1090, 1391] * 3,
            "carrier": ["AA", "UA", "DL", "AA"] * 3,
        }
    )

    flights_tbl = con.create_table("flights", flights_df)

    print("\n📊 Sample data (12 flights across 3 origins, 4 carriers):")
    print(flights_df.head(8))
    print("...")

    # ============================================================================
    # STEP 2: Compute Semantic Layer - INCLUDING percentage measures!
    # ============================================================================
    print("\n" + "-" * 80)
    print("STEP 2: Compute Semantic Layer - Define ALL measures including percentages")
    print("-" * 80)
    print("\n💡 KEY INSIGHT: You CAN define percentage measures in the semantic layer!")
    print("   Use t.all() to reference the grand total of another measure.")

    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            origin=lambda t: t.origin,
            carrier=lambda t: t.carrier,
        )
        .with_measures(
            # Base aggregation measures
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
            avg_distance=lambda t: t.distance.mean(),
        )
        # Add percentage measures that reference base measures
        .with_measures(
            # Percentage measures using t.all()
            market_share=lambda t: t.flight_count / t.all(t.flight_count),
            market_share_pct=lambda t: t.flight_count / t.all(t.flight_count) * 100,
            distance_share=lambda t: t.total_distance / t.all(t.total_distance),
            distance_share_pct=lambda t: t.total_distance
            / t.all(t.total_distance)
            * 100,
        )
    )

    print("\n✓ Semantic table created with:")
    print(f"  Dimensions: {list(flights.dimensions.keys())}")
    print(f"  Measures: {flights.measures}")
    print("\n🎯 Percentage measures are now part of the semantic layer!")

    # ============================================================================
    # STEP 3: Query the Semantic Layer
    # ============================================================================
    print("\n" + "=" * 80)
    print("STEP 3: Query the Semantic Layer")
    print("=" * 80)

    # Query 1: Basic market share by carrier
    print("\n" + "-" * 80)
    print("Query 1: Carrier Market Share (using semantic measure!)")
    print("-" * 80)

    result = (
        flights.group_by("carrier")
        .aggregate("flight_count", "market_share")  # Both from semantic layer!
        .order_by(_.market_share.desc())
        .execute()
    )

    print(result)
    print(f"\n✓ Market shares sum to: {result['market_share'].sum():.2f}")
    print("✓ Both measures come directly from semantic layer - no .mutate() needed!")

    # Query 2: Market share with percentage formatting
    print("\n" + "-" * 80)
    print("Query 2: Market Share as Percentage (from semantic layer!)")
    print("-" * 80)

    result = (
        flights.group_by("carrier")
        .aggregate("flight_count", "market_share_pct")  # Both from SL!
        .order_by(_.market_share_pct.desc())
        .execute()
    )

    print(result)
    print("\n✓ Percentage already calculated - no .mutate() needed!")

    # Query 3: Contribution to total distance
    print("\n" + "-" * 80)
    print("Query 3: Contribution to Total Distance (ALL from semantic layer!)")
    print("-" * 80)

    result = (
        flights.group_by("carrier")
        .aggregate(
            "flight_count",
            "total_distance",
            "distance_share",  # From SL!
            "avg_distance",  # From SL!
        )
        .order_by(_.distance_share.desc())
        .execute()
    )

    print(result)
    print("\n✓ ALL measures from semantic layer - zero .mutate() calls!")

    # Query 4: Origin-level analysis with percent of total
    print("\n" + "-" * 80)
    print("Query 4: Flight Distribution by Origin (from semantic layer!)")
    print("-" * 80)

    result = (
        flights.group_by("origin")
        .aggregate("flight_count", "market_share_pct")  # Both from SL!
        .order_by(_.flight_count.desc())
        .execute()
    )

    print(result)
    print("\n✓ Percentage measure works for ANY grouping!")

    # Query 5: Comparing to group total vs grand total
    print("\n" + "-" * 80)
    print("Query 5: Carrier Share Within Each Origin")
    print("-" * 80)
    print("(Note: Grand total % from SL, within-group % needs .mutate())")

    result = (
        flights.group_by("origin", "carrier")
        .aggregate("flight_count", "market_share_pct")  # Grand total from SL!
        .mutate(
            # Percent of origin's flights (within-group calc needs window function)
            pct_of_origin=lambda t: t["flight_count"]
            / t["flight_count"].sum().over(ibis.window(group_by="origin"))
            * 100,
        )
        .order_by("origin", _.pct_of_origin.desc())
        .execute()
    )

    print(result)
    print("\n✓ Grand total % from SL, within-group % needs .mutate() + window function")

    # Query 6: Comprehensive View - ALL measures from semantic layer!
    print("\n" + "-" * 80)
    print("Query 6: Comprehensive View - Everything from Semantic Layer")
    print("-" * 80)

    result = (
        flights.group_by("carrier")
        .aggregate(
            "flight_count",
            "total_distance",
            "avg_distance",
            "market_share_pct",  # From SL!
            "distance_share_pct",  # From SL!
        )
        .order_by(_.market_share_pct.desc())
        .execute()
    )

    print(result)
    print("\n✓ 5 measures, ZERO .mutate() calls - all from semantic layer!")

    print("\n" + "=" * 80)
    print("✅ Example completed successfully!")
    print("=" * 80)
    print("\n🎯 KEY TAKEAWAYS:")
    print("  ✓ Structure: 1) Define dataset, 2) Compute SL, 3) Query SL")
    print("  ✓ You CAN define percentage measures in the semantic layer!")
    print("  ✓ Use t.all(t.measure) to reference grand totals in measure definitions")
    print("  ✓ Percentage measures work across ANY grouping (carrier, origin, etc.)")
    print(
        "  ✓ Grand total %: Define in SL | Within-group %: Use .mutate() + window function"
    )
    print("  ✓ Result: Reusable percentage metrics, no .mutate() for common cases")
    print("\n📚 Next: See window_functions.py for rolling averages and rankings")
    print()


if __name__ == "__main__":
    main()
