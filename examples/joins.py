#!/usr/bin/env python3
"""Joining Semantic Tables - Foreign Sums and Averages.
https://docs.malloydata.dev/documentation/patterns/foreign_sums
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

    models = to_semantic_table(aircraft_models_tbl, name="models").with_measures(
        model_count=lambda t: t.count(),
        avg_seats=lambda t: t.seats.mean(),
    )

    aircraft = (
        to_semantic_table(aircraft_tbl, name="aircraft")
        .join(
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
        .join(aircraft, lambda f, a: f.tail_num == a.tail_num, how="left")
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
        )
    )

    flights_by_origin = (
        flights.group_by("origin").aggregate("flight_count").limit(10).execute()
    )
    print("\nFlights by origin:")
    print(flights_by_origin)

    aircraft_by_type = (
        aircraft.group_by("aircraft_type_id")
        .aggregate("aircraft_count")
        .limit(10)
        .execute()
    )
    print("\nAircraft by type:")
    print(aircraft_by_type)


if __name__ == "__main__":
    main()
