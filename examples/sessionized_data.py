#!/usr/bin/env python3
"""
Example 7: Sessionized Data - Map/Reduce Pattern

This example demonstrates how to group event data into sessions and create
nested views of the details within each session. This is similar to Malloy's
map-reduce pattern for sessionization.

Pattern:
1. Group events into sessions (e.g., all flights by a plane on a given day)
2. Aggregate session-level metrics (total distance, max delay, flight count)
3. Show detailed legs/events within each session
4. Number the legs within each session

Use cases:
- Flight legs by aircraft per day
- User actions per session on a website
- Orders per customer per day
- Device events per time window

This is equivalent to Malloy's nested queries with row_number() for session IDs
and flight legs.
"""

import pandas as pd
import ibis
from datetime import datetime, timedelta
from boring_semantic_layer.semantic_api import to_semantic_table


def main():
    print("=" * 80)
    print("  Example 7: Sessionized Data (Map/Reduce Pattern)")
    print("=" * 80)

    con = ibis.duckdb.connect(":memory:")

    # Create flight events data - each row is a flight
    # We'll create multiple flights per tail_num per day to show sessionization
    np_random = __import__("numpy").random
    np_random.seed(42)

    # Generate flight events
    carriers = ["WN", "AA", "DL", "UA"]
    tail_nums = [f"N{i:03d}" for i in range(1, 21)]  # 20 aircraft
    origins = ["LAX", "SFO", "SEA", "PDX", "DEN"]
    destinations = ["PHX", "LAS", "SLC", "ABQ", "BUR"]

    n_flights = 500
    base_date = datetime(2002, 3, 1)

    flights_data = []
    for i in range(n_flights):
        # Create clusters of flights by the same aircraft on the same day
        tail = np_random.choice(tail_nums)
        day_offset = np_random.randint(0, 7)  # 7 days
        flight_date = base_date + timedelta(days=day_offset)

        # Time of day for this flight (flights throughout the day)
        hour = np_random.randint(6, 22)
        minute = np_random.randint(0, 60)
        dep_time = flight_date + timedelta(hours=hour, minutes=minute)

        flights_data.append(
            {
                "id": i + 1,
                "carrier": np_random.choice(carriers),
                "tail_num": tail,
                "dep_time": dep_time,
                "origin": np_random.choice(origins),
                "destination": np_random.choice(destinations),
                "dep_delay": int(np_random.normal(15, 30)),  # minutes
                "arr_delay": int(np_random.normal(10, 25)),  # minutes
                "distance": int(np_random.uniform(200, 1200)),  # miles
            }
        )

    flights_df = pd.DataFrame(flights_data)
    flights_df["flight_date"] = flights_df["dep_time"].dt.date
    flights_df["dep_minute"] = (
        flights_df["dep_time"].dt.hour * 60 + flights_df["dep_time"].dt.minute
    )

    flights_tbl = con.create_table("flights", flights_df)

    print("\n📊 Sample Flight Events Data:")
    print(
        flights_df.head(10)[
            [
                "id",
                "carrier",
                "tail_num",
                "dep_time",
                "origin",
                "destination",
                "dep_delay",
            ]
        ]
    )
    print(f"\nTotal flight events: {len(flights_df)}")
    print(
        f"Date range: {flights_df['flight_date'].min()} to {flights_df['flight_date'].max()}"
    )
    print(f"Unique aircraft: {flights_df['tail_num'].nunique()}")
    print(f"Unique carriers: {flights_df['carrier'].nunique()}")

    # Create semantic table
    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            carrier=lambda t: t.carrier,
            tail_num=lambda t: t.tail_num,
            flight_date=lambda t: t.flight_date,
            origin=lambda t: t.origin,
            destination=lambda t: t.destination,
            dep_minute=lambda t: t.dep_minute,
        )
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
            max_delay=lambda t: t.dep_delay.max(),
            avg_delay=lambda t: t.dep_delay.mean(),
        )
    )

    # Example 1: Basic Sessionization - Group flights by aircraft and day
    print("\n" + "-" * 80)
    print("Example 1: Basic Sessionization - Flights per Aircraft per Day")
    print("-" * 80)

    # Filter to specific carrier and date range for focused example
    # ✅ GOOD: Use .filter() directly on semantic table - preserves measures!
    filtered_flights = flights.filter(
        lambda t: (t.carrier == "WN")
        & (t.flight_date == pd.to_datetime("2002-03-03").date())
    )

    # Create sessions: group by date, carrier, tail_num
    # Use semantic layer measures for aggregation!
    sessions = (
        filtered_flights.group_by("flight_date", "carrier", "tail_num")
        .aggregate(
            "flight_count", "total_distance", "max_delay"
        )  # From semantic layer!
        .mutate(session_id=lambda t: ibis.row_number().over(ibis.window()))
        .order_by("session_id")
        .execute()
    )

    print("\nSessions (WN flights on 2002-03-03):")
    print(
        sessions[
            ["session_id", "tail_num", "flight_count", "total_distance", "max_delay"]
        ].head(10)
    )
    print(f"\nTotal sessions: {len(sessions)}")

    # Example 2: Sessionized with Flight Legs (Nested Detail)
    print("\n" + "-" * 80)
    print("Example 2: Sessions with Flight Legs (Nested Detail)")
    print("-" * 80)

    # Get the detailed flight legs with session info
    flight_legs = (
        filtered_flights.to_ibis()
        .mutate(
            flight_date=lambda t: t.dep_time.date(),
        )
        .mutate(
            # Add session identifier
            session_key=lambda t: (
                t.flight_date.cast(str) + "_" + t.carrier + "_" + t.tail_num
            )
        )
        .mutate(
            # Add flight leg number within session
            flight_leg=lambda t: ibis.row_number().over(
                ibis.window(group_by="session_key", order_by="dep_time")
            )
        )
        .order_by("session_key", "flight_leg")
        .execute()
    )

    print("\nFlight legs with session and leg numbers:")
    display_cols = [
        "tail_num",
        "flight_leg",
        "dep_minute",
        "origin",
        "destination",
        "dep_delay",
        "arr_delay",
        "distance",
    ]

    # Show a few sessions
    sample_sessions = flight_legs["session_key"].unique()[:3]
    for session_key in sample_sessions:
        session_legs = flight_legs[flight_legs["session_key"] == session_key]
        tail = session_legs.iloc[0]["tail_num"]
        total_dist = session_legs["distance"].sum()
        max_del = session_legs["dep_delay"].max()

        print(f"\nSession: {tail} on 2002-03-03")
        print(
            f"  Metrics: {len(session_legs)} flights, {total_dist} miles, max delay {max_del} min"
        )
        print("  Flight legs:")
        for _, leg in session_legs.iterrows():
            print(
                f"    Leg {leg['flight_leg']}: {leg['origin']:>3} → {leg['destination']:<3} "
                f"@ {leg['dep_minute']:>4} min | "
                f"Delay: {leg['dep_delay']:>3} min | "
                f"Distance: {leg['distance']:>4} mi"
            )

    # Example 3: Multi-day Sessions
    print("\n" + "-" * 80)
    print("Example 3: Multi-day Sessions (All WN Flights)")
    print("-" * 80)

    # All WN flights across all dates
    # ✅ GOOD: Use .filter() directly - preserves semantic layer measures!
    wn_flights = flights.filter(lambda t: t.carrier == "WN")

    # Create sessions across multiple days
    # Use semantic layer measures for aggregation!
    wn_sessions = (
        wn_flights.group_by("flight_date", "tail_num")
        .aggregate(
            "flight_count", "total_distance", "max_delay", "avg_delay"
        )  # From SL!
        .order_by("flight_date", "tail_num")
        .execute()
    )

    print("\nWN sessions by aircraft and date:")
    print(wn_sessions.head(15))

    print("\nSummary:")
    print(f"  Total sessions: {len(wn_sessions)}")
    print(f"  Unique dates: {wn_sessions['flight_date'].nunique()}")
    print(f"  Unique aircraft: {wn_sessions['tail_num'].nunique()}")
    print(f"  Avg flights per session: {wn_sessions['flight_count'].mean():.1f}")
    print(
        f"  Avg distance per session: {wn_sessions['total_distance'].mean():.0f} miles"
    )

    # Example 4: Identify High-Activity Sessions
    print("\n" + "-" * 80)
    print("Example 4: High-Activity Sessions (Most flights per session)")
    print("-" * 80)

    # Sessions with most flights
    top_sessions = wn_sessions.nlargest(10, "flight_count")

    print("\nTop 10 sessions by flight count:")
    for _, session in top_sessions.iterrows():
        print(
            f"  {session['flight_date']} | {session['tail_num']} | "
            f"{session['flight_count']} flights | "
            f"{session['total_distance']} mi | "
            f"Max delay: {session['max_delay']} min"
        )

    # Example 5: Session-level Analysis with Filtering
    print("\n" + "-" * 80)
    print("Example 5: Sessions with Delays > 30 minutes")
    print("-" * 80)

    delayed_sessions = wn_sessions[wn_sessions["max_delay"] > 30]

    print(f"\nSessions with significant delays (max > 30 min): {len(delayed_sessions)}")
    print("\nSample delayed sessions:")
    for _, session in delayed_sessions.head(8).iterrows():
        print(
            f"  {session['flight_date']} | {session['tail_num']} | "
            f"{session['flight_count']} flights | "
            f"Max delay: {session['max_delay']} min | "
            f"Avg delay: {session['avg_delay']:.0f} min"
        )

    print("\n" + "=" * 80)
    print("✅ Example completed successfully!")
    print("=" * 80)
    print("\nKey Takeaways:")
    print("  • Sessionization groups events by entity (aircraft) and time (day)")
    print("  • Use group_by with multiple dimensions to define sessions")
    print("  • Add session_id with ibis.row_number() for unique identifiers")
    print("  • Add flight_leg with windowed row_number() for ordering within sessions")
    print("  • Aggregate session-level metrics (count, sum, max, avg)")
    print("  • Display nested flight legs within each session")
    print("  • Filter and analyze sessions based on aggregated metrics")
    print("  • Essential for event analysis, user sessions, device logs")
    print("\nNext: See examples/README.md for all patterns")
    print()


if __name__ == "__main__":
    main()
