#!/usr/bin/env python3
"""
Example 3: Window Functions (Rolling Averages, Rankings, Running Totals)

This example demonstrates advanced analytical patterns using window functions:
- Rolling/moving averages for trend analysis
- Rankings within groups
- Running totals (cumulative sums)
- Percentiles and statistical functions

Window functions are powerful for time-series analysis, leaderboards,
and comparative analytics.
"""

import pandas as pd
import ibis
from ibis import _
from boring_semantic_layer import to_semantic_table


def main():
    print("=" * 80)
    print("  Example 3: Window Functions")
    print("=" * 80)

    con = ibis.duckdb.connect(":memory:")

    # Time series data: daily flights over 2 months
    dates = pd.date_range("2024-01-01", periods=60, freq="D")
    flights_df = pd.DataFrame({
        "date": dates,
        "origin": (["JFK"] * 20 + ["LAX"] * 20 + ["ORD"] * 20),
        "flight_count": [
            # JFK - gradual increase
            50, 52, 51, 53, 54, 56, 55, 57, 58, 60, 59, 61, 62, 64, 63, 65, 66, 68, 67, 69,
            # LAX - stable with spike
            80, 81, 82, 80, 81, 85, 90, 95, 88, 82, 81, 80, 82, 81, 83, 82, 81, 80, 82, 81,
            # ORD - declining
            100, 98, 96, 94, 92, 90, 88, 86, 84, 82, 80, 78, 76, 74, 72, 70, 68, 66, 64, 62,
        ],
        "avg_delay": [
            # JFK
            15, 18, 12, 20, 16, 14, 22, 19, 17, 15, 18, 21, 16, 14, 19, 17, 15, 18, 20, 16,
            # LAX
            10, 12, 11, 13, 10, 15, 20, 25, 18, 12, 11, 10, 12, 11, 13, 12, 11, 10, 12, 11,
            # ORD
            25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6,
        ],
    })

    flights_tbl = con.create_table("daily_flights", flights_df)

    print("\n📊 Daily flight data (60 days across 3 airports):")
    print(flights_df.head())
    print("...")

    # Create semantic table with window function measures
    flights = (
        to_semantic_table(flights_tbl, name="daily_flights")
        .with_dimensions(
            date=lambda t: t.date,
            origin=lambda t: t.origin,
        )
        .with_measures(
            # Base measures
            daily_flights=lambda t: t.flight_count.sum(),
            mean_delay=lambda t: t.avg_delay.mean(),
        )
    )
    # Example 1: 7-day rolling average
    print("\n" + "-" * 80)
    print("Query 1: 7-Day Rolling Average of Flights")
    print("-" * 80)

    rolling_7d = ibis.window(order_by="date", preceding=6, following=0)

    result = (
        flights
        .group_by("date")
        .aggregate("daily_flights")
        .mutate(
            rolling_7d_avg=lambda t: t["daily_flights"].mean().over(rolling_7d)
        )
        .order_by("date")
        .limit(15)
        .execute()
    )

    print("First 15 days:")
    print(result)

    # Example 2: Rolling average by airport
    print("\n" + "-" * 80)
    print("Query 2: 7-Day Rolling Average by Airport")
    print("-" * 80)

    rolling_by_airport = ibis.window(
        group_by="origin", order_by="date", preceding=6, following=0
    )

    result = (
        flights
        .group_by("origin", "date")
        .aggregate("daily_flights")
        .mutate(
            rolling_avg=lambda t: t["daily_flights"].mean().over(rolling_by_airport)
        )
        .order_by("origin", "date")
        .execute()
    )

    print(f"Showing JFK data (first 10 days):")
    print(result[result["origin"] == "JFK"].head(10))

    # Example 3: Running total (cumulative sum)
    print("\n" + "-" * 80)
    print("Query 3: Running Total of Flights")
    print("-" * 80)

    cumulative_window = ibis.window(order_by="date", preceding=None, following=0)

    result = (
        flights
        .group_by("date")
        .aggregate("daily_flights")
        .mutate(
            cumulative_flights=lambda t: t["daily_flights"].sum().over(cumulative_window)
        )
        .order_by("date")
        .limit(10)
        .execute()
    )

    print("First 10 days:")
    print(result)

    # Example 4: Rankings
    print("\n" + "-" * 80)
    print("Query 4: Daily Airport Rankings by Flight Count")
    print("-" * 80)

    result = (
        flights
        .group_by("origin", "date")
        .aggregate("daily_flights")
        .mutate(
            daily_rank=lambda t: t["daily_flights"].rank().over(
                ibis.window(group_by="date", order_by=ibis.desc(t["daily_flights"]))
            )
        )
        .order_by("date", "daily_rank")
        .execute()
    )

    print("First 9 days (3 airports per day):")
    print(result.head(9))

    # Example 5: Lead and Lag
    print("\n" + "-" * 80)
    print("Query 5: Day-over-Day Change")
    print("-" * 80)

    lag_window = ibis.window(group_by="origin", order_by="date")

    result = (
        flights
        .group_by("origin", "date")
        .aggregate("daily_flights")
        .mutate(
            previous_day=lambda t: t["daily_flights"].lag().over(lag_window),
            day_over_day_change=lambda t: (
                t["daily_flights"] - t["daily_flights"].lag().over(lag_window)
            ),
        )
        .order_by("origin", "date")
        .execute()
    )

    print("JFK data (showing day-over-day changes):")
    print(result[result["origin"] == "JFK"].head(10))

    # Example 6: Statistical measures with windows
    print("\n" + "-" * 80)
    print("Query 6: Moving Statistics (Min, Max, StdDev)")
    print("-" * 80)

    stats_window = ibis.window(order_by="date", preceding=6, following=0)

    result = (
        flights
        .group_by("date")
        .aggregate("daily_flights")
        .mutate(
            rolling_min=lambda t: t["daily_flights"].min().over(stats_window),
            rolling_max=lambda t: t["daily_flights"].max().over(stats_window),
            rolling_std=lambda t: t["daily_flights"].std().over(stats_window),
        )
        .order_by("date")
        .limit(15)
        .execute()
    )

    print("First 15 days (7-day window statistics):")
    print(result)

    # Example 7: Combining with percent of total
    print("\n" + "-" * 80)
    print("Query 7: Airport Share with Rank")
    print("-" * 80)

    total_by_airport = (
        flights
        .group_by("origin")
        .aggregate("daily_flights")
        .mutate(
            total_share=lambda t: t["daily_flights"] / t.all(t["daily_flights"]) * 100,
        )
        .order_by(_.daily_flights.desc())
        .execute()
    )

    print(total_by_airport)

    print("\n" + "=" * 80)
    print("✅ Example completed successfully!")
    print("=" * 80)
    print("\nKey Takeaways:")
    print("  • Use ibis.window() to define window frames")
    print("  • Rolling average: .mean().over(window(preceding=N, following=0))")
    print("  • Running total: .sum().over(window(preceding=None, following=0))")
    print("  • Rankings: .rank().over(window(order_by=...))")
    print("  • Group windows with group_by parameter")
    print("  • Combine with t.all() for comprehensive analytics")
    print()


if __name__ == "__main__":
    main()
