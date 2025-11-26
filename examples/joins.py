#!/usr/bin/env python3
"""Joining Semantic Tables - Foreign Sums and Averages.
https://docs.malloydata.dev/documentation/patterns/foreign_sums

This example demonstrates:
1. Basic joins between semantic tables
2. Handling name conflicts when joining tables with overlapping column names
"""

import ibis

from boring_semantic_layer import to_semantic_table

# this is a public R2 bucket with sample data hosted by Malloy
BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def main():
    con = ibis.duckdb.connect(":memory:")
    flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")
    aircraft_tbl = con.read_parquet(f"{BASE_URL}/aircraft.parquet")
    aircraft_models_tbl = con.read_parquet(f"{BASE_URL}/aircraft_models.parquet")
    airports_tbl = con.read_parquet(f"{BASE_URL}/airports.parquet")
    carriers_tbl = con.read_parquet(f"{BASE_URL}/carriers.parquet")

    # ========================================================================
    # Example 1: Basic joins (multi-level)
    # ========================================================================
    print("\n" + "=" * 70)
    print("Example 1: Multi-level joins (flights -> aircraft -> models)")
    print("=" * 70)

    models = to_semantic_table(aircraft_models_tbl, name="models").with_measures(
        model_count=lambda t: t.count(),
        avg_seats=lambda t: t.seats.mean(),
    )

    aircraft = (
        to_semantic_table(aircraft_tbl, name="aircraft")
        .join_one(
            models,
            lambda a, m: a.aircraft_model_code == m.aircraft_model_code,
            how="left",
        )
        .with_measures(
            aircraft_count=lambda t: t.count(),
        )
    )

    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .join_many(aircraft, lambda f, a: f.tail_num == a.tail_num)
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
        )
    )

    flights_by_origin = flights.group_by("origin").aggregate("flight_count").limit(10).execute()
    print("\nFlights by origin:")
    print(flights_by_origin)

    aircraft_by_type = (
        aircraft.group_by("aircraft_type_id").aggregate("aircraft_count").limit(10).execute()
    )
    print("\nAircraft by type:")
    print(aircraft_by_type)

    # ========================================================================
    # Example 2: Handling name conflicts with dot notation
    # ========================================================================
    print("\n" + "=" * 70)
    print("Example 2: Name conflicts - both tables have 'code' column")
    print("=" * 70)

    # Both carriers and airports have a 'code' column
    # Solution: Use dot notation (table.column) when defining dimensions

    # Join and rename to avoid Ibis-level conflicts
    joined = flights_tbl.left_join(
        carriers_tbl.rename(carrier_code="code"),
        flights_tbl.carrier == carriers_tbl.code,
    ).left_join(
        airports_tbl.rename(airport_code="code"),
        flights_tbl.origin == airports_tbl.code,
    )

    # Use dot notation for dimension names to show source table
    flights_enriched = (
        to_semantic_table(joined, name="flights")
        .with_dimensions(
            **{
                "carriers.code": lambda t: t.carrier_code,
                "carriers.nickname": lambda t: t.nickname,
                "airports.code": lambda t: t.airport_code,
                "airports.city": lambda t: t.city,
            }
        )
        .with_measures(flight_count=lambda t: t.count())
    )

    # Query using dot notation - clearly shows which 'code' column
    result = (
        flights_enriched.group_by("carriers.code", "airports.code")
        .aggregate("flight_count")
        .order_by(ibis._.flight_count.desc())
        .limit(10)
        .execute()
    )

    print("\nTop routes (carriers.code + airports.code):")
    print(result)


if __name__ == "__main__":
    main()
