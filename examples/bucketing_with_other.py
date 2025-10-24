#!/usr/bin/env python3
"""
Example 5: Bucketing with 'Other' (Top N with Rollup)

This example demonstrates a common reporting pattern: showing the top N items
individually and grouping everything else into an 'OTHER' bucket using
window functions and case expressions.

Pattern:
1. Use semantic layer to aggregate data (group_by + aggregate)
2. Add ranking with window functions in mutate
3. Add bucketing logic with case expressions
4. Convert to Ibis and do second level of aggregation for bucketing

This is equivalent to Malloy's "Bucketing with 'Other'" pattern.
"""

import pandas as pd
import ibis
from boring_semantic_layer.api import to_semantic_table


def main():
    print("=" * 80)
    print("  Example 5: Bucketing with 'Other' (Top N Analysis)")
    print("=" * 80)

    con = ibis.duckdb.connect(":memory:")

    # Airport data
    airports_df = pd.DataFrame(
        {
            "code": [f"K{i:03d}" for i in range(100)],
            "state": (
                ["CO"] * 20
                + ["CA"] * 18
                + ["TX"] * 15
                + ["FL"] * 12
                + ["AK"] * 10
                + ["NY"] * 8
                + ["WA"] * 5
                + ["OR"] * 4
                + ["NV"] * 3
                + ["ID"] * 2
                + ["MT"] * 1
                + ["WY"] * 1
                + ["UT"] * 1
            ),
            "elevation": (
                [6500] * 20
                + [1200] * 18
                + [800] * 15
                + [50] * 12
                + [1500] * 10
                + [500] * 8
                + [1000] * 5
                + [1100] * 4
                + [4500] * 3
                + [4200] * 2
                + [3800] * 1
                + [6000] * 1
                + [5200] * 1
            ),
            "fac_type": (
                ["AIRPORT"] * 70
                + ["HELIPORT"] * 15
                + ["SEAPLANE BASE"] * 10
                + ["GLIDERPORT"] * 5
            ),
        }
    )

    airports_tbl = con.create_table("airports", airports_df)

    print("\n📊 Airport Data (100 airports across 13 states):")
    print(airports_df.groupby("state").size().sort_values(ascending=False))

    # Create semantic table
    airports = (
        to_semantic_table(airports_tbl, name="airports")
        .with_dimensions(
            state=lambda t: t.state,
            code=lambda t: t.code,
            fac_type=lambda t: t.fac_type,
        )
        .with_measures(
            airport_count=lambda t: t.count(),
            avg_elevation=lambda t: t.elevation.mean(),
        )
    )

    # Example 1: Top 5 States with OTHER bucket - using mutate only
    print("\n" + "-" * 80)
    print("Query 1: Top 5 States by Airport Count (with OTHER)")
    print("-" * 80)
    print("Using single query with mutate and case expression")

    # Calculate everything in one query using window functions and case
    result = (
        airports.group_by("state")
        .aggregate("airport_count", "avg_elevation")
        .mutate(
            # Add ranking
            rank=lambda t: ibis.rank().over(
                ibis.window(order_by=ibis.desc(t["airport_count"]))
            ),
            # Add bucket based on rank
            state_bucket=lambda t: ibis.case()
            .when(
                ibis.rank().over(ibis.window(order_by=ibis.desc(t["airport_count"])))
                <= 5,
                t.state,
            )
            .else_("OTHER")
            .end(),
        )
        .to_ibis()
        # Now do SQL-level aggregation for the bucketing
        .group_by("state_bucket")
        .aggregate(
            airports=lambda t: t.airport_count.sum(),
            weighted_elevation=lambda t: (t.avg_elevation * t.airport_count).sum(),
        )
        .mutate(avg_elevation=lambda t: t.weighted_elevation / t.airports)
        .select("state_bucket", "airports", "avg_elevation")
        .order_by(ibis.desc("airports"))
        .execute()
    )

    print("\nTop 5 states + OTHER bucket:")
    print(result)

    other_count = (
        result[result["state_bucket"] == "OTHER"]["airports"].iloc[0]
        if "OTHER" in result["state_bucket"].values
        else 0
    )
    print(f"\n✓ Top 5 states shown individually, {other_count} airports in OTHER")

    # Example 2: Top 3 States per Facility Type
    print("\n" + "-" * 80)
    print("Query 2: Top 3 States per Facility Type (with OTHER)")
    print("-" * 80)

    # Calculate everything in one query
    result = (
        airports.group_by("fac_type", "state")
        .aggregate("airport_count")
        .mutate(
            rank_in_type=lambda t: ibis.rank().over(
                ibis.window(group_by="fac_type", order_by=ibis.desc(t["airport_count"]))
            ),
            state_bucket=lambda t: ibis.case()
            .when(
                ibis.rank().over(
                    ibis.window(
                        group_by="fac_type", order_by=ibis.desc(t["airport_count"])
                    )
                )
                <= 3,
                t.state,
            )
            .else_("OTHER")
            .end(),
        )
        .to_ibis()
        .group_by("fac_type", "state_bucket")
        .aggregate(airports=lambda t: t.airport_count.sum())
        .order_by("fac_type", ibis.desc("airports"))
        .execute()
    )

    print("\nTop 3 states per facility type:")
    for fac_type in result["fac_type"].unique():
        print(f"\n{fac_type}:")
        subset = result[result["fac_type"] == fac_type]
        print(subset[["state_bucket", "airports"]].to_string(index=False))

    # Example 3: Dynamic threshold (80% of airports)
    print("\n" + "-" * 80)
    print("Query 3: States Covering 80% of Airports")
    print("-" * 80)

    # Calculate cumulative percentage and bucket in one query
    temp_result = (
        airports.group_by("state")
        .aggregate("airport_count")
        .mutate(
            cumsum=lambda t: t["airport_count"]
            .sum()
            .over(
                ibis.window(
                    order_by=ibis.desc(t["airport_count"]), preceding=None, following=0
                )
            ),
            total=lambda t: t.all(t["airport_count"]),
            cum_pct=lambda t: (
                t["airport_count"]
                .sum()
                .over(
                    ibis.window(
                        order_by=ibis.desc(t["airport_count"]),
                        preceding=None,
                        following=0,
                    )
                )
                / t.all(t["airport_count"])
            ),
        )
        .to_ibis()
    )

    print("\nCumulative coverage:")
    print(temp_result.select("state", "airport_count", "cum_pct").head(8).execute())

    # Bucket by 80% threshold
    result = (
        temp_result.mutate(
            state_bucket=lambda t: ibis.case()
            .when(t.cum_pct <= 0.80, t.state)
            .else_("OTHER")
            .end()
        )
        .group_by("state_bucket")
        .aggregate(airports=lambda t: t.airport_count.sum())
        .mutate(pct=lambda t: t.airports / t.airports.sum().over(ibis.window()) * 100)
        .order_by(ibis.desc("airports"))
        .execute()
    )

    print("\nStates covering 80% + OTHER:")
    print(result)

    top_count = len(result) - (1 if "OTHER" in result["state_bucket"].values else 0)
    print(f"\n✓ {top_count} states cover 80% of airports")

    # Example 4: Pie chart ready
    print("\n" + "-" * 80)
    print("Query 4: Pie Chart Ready - Top 6 + OTHER")
    print("-" * 80)

    result = (
        airports.group_by("state")
        .aggregate("airport_count")
        .mutate(
            rank=lambda t: ibis.rank().over(
                ibis.window(order_by=ibis.desc(t["airport_count"]))
            ),
            label=lambda t: ibis.case()
            .when(
                ibis.rank().over(ibis.window(order_by=ibis.desc(t["airport_count"])))
                <= 6,
                t.state,
            )
            .else_("OTHER")
            .end(),
        )
        .to_ibis()
        .group_by("label")
        .aggregate(value=lambda t: t.airport_count.sum())
        .mutate(
            percentage=lambda t: (
                t.value / t.value.sum().over(ibis.window()) * 100
            ).round(1)
        )
        .order_by(ibis.desc("value"))
        .execute()
    )

    print(result)
    print("\n📊 Perfect for pie charts:")
    print("   - Limited slices (7 total)")
    print("   - Focused on top contributors")
    print("   - Small values grouped as OTHER")

    print("\n" + "=" * 80)
    print("✅ Example completed successfully!")
    print("=" * 80)
    print("\nKey Takeaways:")
    print("  • Use semantic layer for first-level aggregation (group_by + aggregate)")
    print("  • Use ibis.rank() with window functions in mutate for rankings")
    print("  • Use ibis.case() for bucketing logic (top N vs OTHER)")
    print("  • Call .to_ibis() to drop to SQL level for second aggregation")
    print("  • Window functions work for ranking within groups (group_by parameter)")
    print("  • Cumulative sums with window functions for percentage thresholds")
    print("  • Essential pattern for clean reports and visualizations")
    print("\nNext: See examples/README.md for all patterns")
    print()


if __name__ == "__main__":
    main()
