#!/usr/bin/env python3
"""
Example 2: Percent of Total (Market Share Analysis)

This example demonstrates the t.all() functionality for calculating percentages
of grand totals - commonly used for market share, contribution analysis, and
relative performance metrics.

Key Concept:
- t.all(t["measure"]) accesses the grand total across all groups
- Enables "percent of total" calculations in a single query
- Works seamlessly with grouped aggregations
"""

import pandas as pd
import ibis
from ibis import _
from boring_semantic_layer.api import to_semantic_table


def main():
    print("=" * 80)
    print("  Example 2: Percent of Total / Market Share")
    print("=" * 80)

    # Set up connection and data
    con = ibis.duckdb.connect(":memory:")

    # Flights data with carriers
    flights_df = pd.DataFrame({
        "origin": ["JFK"] * 4 + ["LAX"] * 4 + ["ORD"] * 4,
        "destination": ["LAX", "ORD", "MIA", "DFW"] * 3,
        "distance": [2475, 740, 1090, 1391] * 3,
        "carrier": ["AA", "UA", "DL", "AA"] * 3,
    })

    flights_tbl = con.create_table("flights", flights_df)

    print("\n📊 Sample data (12 flights across 3 origins, 4 carriers):")
    print(flights_df.head(8))
    print("...")

    # Create semantic table
    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            origin=lambda t: t.origin,
            carrier=lambda t: t.carrier,
        )
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
        )
    )

    # Example 1: Basic market share by carrier
    print("\n" + "-" * 80)
    print("Query 1: Carrier Market Share")
    print("-" * 80)

    result = (
        flights
        .group_by("carrier")
        .aggregate("flight_count")
        .mutate(
            market_share=lambda t: t["flight_count"] / t.all(t["flight_count"])
        )
        .order_by(_.market_share.desc())
        .execute()
    )

    print(result)
    print(f"\n✓ Market shares sum to: {result['market_share'].sum():.2f}")

    # Example 2: Market share with percentage formatting
    print("\n" + "-" * 80)
    print("Query 2: Market Share as Percentage")
    print("-" * 80)

    result = (
        flights
        .group_by("carrier")
        .aggregate("flight_count")
        .mutate(
            market_share_pct=lambda t: (
                t["flight_count"] / t.all(t["flight_count"]) * 100
            )
        )
        .order_by(_.market_share_pct.desc())
        .execute()
    )

    print(result)

    # Example 3: Contribution to total distance
    print("\n" + "-" * 80)
    print("Query 3: Contribution to Total Distance Flown")
    print("-" * 80)

    result = (
        flights
        .group_by("carrier")
        .aggregate("flight_count", "total_distance")
        .mutate(
            distance_share=lambda t: t["total_distance"] / t.all(t["total_distance"]),
            avg_distance=lambda t: t["total_distance"] / t["flight_count"],
        )
        .order_by(_.distance_share.desc())
        .execute()
    )

    print(result)

    # Example 4: Origin-level analysis with percent of total
    print("\n" + "-" * 80)
    print("Query 4: Flight Distribution by Origin")
    print("-" * 80)

    result = (
        flights
        .group_by("origin")
        .aggregate("flight_count")
        .mutate(
            pct_of_flights=lambda t: (
                t["flight_count"] / t.all(t["flight_count"]) * 100
            )
        )
        .order_by(_.flight_count.desc())
        .execute()
    )

    print(result)

    # Example 5: Comparing to group total vs grand total
    print("\n" + "-" * 80)
    print("Query 5: Carrier Share Within Each Origin")
    print("-" * 80)
    print("(Note: This shows percent within origin, not overall market share)")

    result = (
        flights
        .group_by("origin", "carrier")
        .aggregate("flight_count")
        .mutate(
            # Percent of origin's flights (using window function)
            pct_of_origin=lambda t: t["flight_count"] / t["flight_count"].sum().over(
                ibis.window(group_by="origin")
            ) * 100,
            # Percent of all flights (using t.all)
            pct_of_total=lambda t: t["flight_count"] / t.all(t["flight_count"]) * 100,
        )
        .order_by("origin", _.pct_of_origin.desc())
        .execute()
    )

    print(result)

    # Example 6: Using deferred syntax
    print("\n" + "-" * 80)
    print("Query 6: Same Query Using Deferred Syntax (_.col)")
    print("-" * 80)

    result = (
        flights
        .group_by("carrier")
        .aggregate("flight_count")
        .mutate(
            market_share=_.flight_count / _.all(_.flight_count)
        )
        .order_by(_.market_share.desc())
        .execute()
    )

    print(result)

    print("\n" + "=" * 80)
    print("✅ Example completed successfully!")
    print("=" * 80)
    print("\nKey Takeaways:")
    print("  • Use t.all(t['measure']) to access grand total")
    print("  • Calculate market share: measure / t.all(measure)")
    print("  • Multiply by 100 for percentage values")
    print("  • Compare to window functions for group-level percentages")
    print("  • Works with both lambda and deferred syntax")
    print("\nNext: See 03_window_functions.py for rolling averages and rankings")
    print()


if __name__ == "__main__":
    main()
