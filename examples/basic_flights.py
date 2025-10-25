#!/usr/bin/env python3
"""Basic Semantic Table Usage with Flights."""
import pandas as pd
import ibis
from ibis import _
from boring_semantic_layer import to_semantic_table


def main():
    con = ibis.duckdb.connect(":memory:")

    flights_df = pd.DataFrame({
        "origin": ["JFK", "LAX", "JFK", "ORD", "LAX", "JFK", "ORD", "LAX"],
        "destination": ["LAX", "JFK", "ORD", "JFK", "ORD", "LAX", "LAX", "ORD"],
        "distance": [2475, 2475, 740, 740, 1744, 2475, 987, 1744],
        "carrier": ["AA", "UA", "AA", "UA", "AA", "UA", "AA", "UA"],
    })

    flights_tbl = con.create_table("flights", flights_df)

    flights = to_semantic_table(flights_tbl, name="flights").with_measures(
        flight_count=lambda t: t.count(),
        total_distance=lambda t: t.distance.sum(),
        avg_distance=lambda t: t.distance.mean(),
        max_distance=lambda t: t.distance.max(),
        min_distance=lambda t: t.distance.min(),
    )

    result = flights.group_by("origin").aggregate("flight_count").execute()
    print("\nFlight counts by origin:")
    print(result)

    result = (
        flights.group_by("origin", "carrier")
        .aggregate("flight_count", "avg_distance")
        .order_by(_.flight_count.desc())
        .execute()
    )
    print("\nFlights by origin and carrier:")
    print(result)

    flights_enhanced = flights.with_measures(
        distance_per_flight=lambda t: t.distance.sum() / t.count(),
    )

    result = (
        flights_enhanced.group_by("carrier")
        .aggregate("flight_count", "total_distance", "distance_per_flight")
        .order_by(_.distance_per_flight.desc())
        .execute()
    )
    print("\nDistance per flight by carrier:")
    print(result)

    long_haul_flights = flights_enhanced.filter(lambda t: t.distance > 1000)
    result = (
        long_haul_flights.group_by("carrier")
        .aggregate("flight_count", "avg_distance")
        .execute()
    )
    print("\nLong-haul flights (>1000 miles):")
    print(result)


if __name__ == "__main__":
    main()
