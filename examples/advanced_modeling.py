#!/usr/bin/env python3
"""Advanced Modeling — Multi-level join-tree aggregates, pipelines, and composition.

Inspired by: https://docs.malloydata.dev/documentation/language/advanced_modeling

This example demonstrates:
  1. Multi-level joins with string/Deferred shorthand
  2. Aggregates across the join tree (measures from 3 levels deep)
  3. Extending sources (adding measures / dimensions after a join)
  4. Pipelines — chaining aggregate -> mutate for derived analytics
  5. Composing a snowflake schema programmatically
"""

import ibis
from ibis import _

from boring_semantic_layer import to_semantic_table

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def main():
    con = ibis.duckdb.connect(":memory:")

    # ------------------------------------------------------------------
    # Load raw tables
    # ------------------------------------------------------------------
    airports_tbl = con.read_parquet(f"{BASE_URL}/airports.parquet")
    flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")
    carriers_tbl = con.read_parquet(f"{BASE_URL}/carriers.parquet")
    aircraft_tbl = con.read_parquet(f"{BASE_URL}/aircraft.parquet")
    aircraft_models_tbl = con.read_parquet(f"{BASE_URL}/aircraft_models.parquet")

    # ------------------------------------------------------------------
    # 1. Define semantic models (sources)
    # ------------------------------------------------------------------
    carriers = (
        to_semantic_table(carriers_tbl, name="carriers")
        .with_dimensions(
            code=_.code,
            name=_.name,
            nickname=_.nickname,
        )
        .with_measures(carrier_count=_.count())
    )

    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            origin=_.origin,
            destination=_.destination,
            carrier=_.carrier,
            tail_num=_.tail_num,
        )
        .with_measures(
            flight_count=_.count(),
            total_distance=_.distance.sum(),
            avg_distance=_.distance.mean(),
        )
    )

    airports = (
        to_semantic_table(airports_tbl, name="airports")
        .with_dimensions(
            code=_.code,
            state=_.state,
            city=_.city,
            fac_type=_.fac_type,
            elevation=_.elevation,
        )
        .with_measures(
            airport_count=_.count(),
            avg_elevation=_.elevation.mean(),
        )
    )

    aircraft = (
        to_semantic_table(aircraft_tbl, name="aircraft")
        .with_dimensions(
            tail_num=_.tail_num,
            aircraft_model_code=_.aircraft_model_code,
        )
        .with_measures(aircraft_count=_.count())
    )

    aircraft_models = (
        to_semantic_table(aircraft_models_tbl, name="models")
        .with_dimensions(
            aircraft_model_code=_.aircraft_model_code,
            manufacturer=_.manufacturer,
            model=_.model,
            seats=_.seats,
        )
        .with_measures(
            model_count=_.count(),
            avg_seats=_.seats.mean(),
        )
    )

    # ==================================================================
    # 2. Multi-level joins — string / Deferred shorthand
    # ==================================================================
    #
    # Malloy equivalent:
    #   source: flights2 is ... extend {
    #     join_one: carriers2 with carrier
    #   }
    #   source: airports2 is ... extend {
    #     join_many: flights2 on code = flights2.origin
    #   }
    #
    # Build the join tree bottom-up:
    #   airports -> flights -> carriers          (3 levels)
    #              flights -> aircraft -> models  (also 3 levels via another arm)
    # ------------------------------------------------------------------

    # flights joined to carriers (equi-join on same-named col via string)
    flights_with_carriers = flights.join_one(carriers, on=lambda f, c: f.carrier == c.code)

    # airports joined to flights (different column names need lambda)
    airports_with_flights = airports.join_many(
        flights_with_carriers,
        on=lambda a, f: a.code == f.origin,
    )

    print("=" * 80)
    print("2. Aggregates Across the Join Tree (3 levels)")
    print("=" * 80)
    print()
    print("Schema: airports -< flights -- carriers")
    print()
    print("Malloy equivalent:")
    print("  run: airports2 -> {")
    print("    group_by: state")
    print("    aggregate:")
    print("      flights2.carriers2.carrier_count  -- 3 levels deep")
    print("      flights2.flight_count")
    print("      flights2.total_distance")
    print("      airport_count")
    print("      avg_elevation")
    print("  }")
    print()

    result = (
        airports_with_flights.group_by("airports.state")
        .aggregate(
            "airports.airport_count",
            "airports.avg_elevation",
            "flights.flight_count",
            "flights.total_distance",
            "carriers.carrier_count",  # 3 levels deep!
        )
        .order_by(lambda t: t.flight_count.desc())
        .limit(10)
        .execute()
    )
    print(result)

    # ==================================================================
    # 3. Snowflake arm: flights -> aircraft -> models
    # ==================================================================
    print()
    print("=" * 80)
    print("3. Second Snowflake Arm: flights -> aircraft -> models")
    print("=" * 80)
    print()

    aircraft_with_models = aircraft.join_one(aircraft_models, on="aircraft_model_code")

    flights_full = flights.join_one(
        carriers, on=lambda f, c: f.carrier == c.code
    ).join_one(
        aircraft_with_models, on="tail_num"
    )

    result = (
        flights_full.group_by("carriers.nickname", "models.manufacturer")
        .aggregate("flights.flight_count")
        .order_by(lambda t: t.flight_count.desc())
        .limit(15)
        .execute()
    )
    print("Flight count by carrier and aircraft manufacturer:")
    print(result)

    # ==================================================================
    # 4. Extending sources — add measures after the join
    # ==================================================================
    #
    # Malloy equivalent:
    #   source: california_airports is airports extend {
    #     where: state = 'CA'
    #     measure: ca_airport_count is count()
    #   }
    # ------------------------------------------------------------------
    print()
    print("=" * 80)
    print("4. Extending Sources After Joins")
    print("=" * 80)
    print()

    texas_flights = (
        airports_with_flights.filter(lambda t: t.airports.state == "TX")
    )

    result = (
        texas_flights.group_by("airports.city")
        .aggregate("flights.flight_count", "flights.total_distance")
        .order_by(lambda t: t.flight_count.desc())
        .limit(10)
        .execute()
    )
    print("Texas airports ranked by flight volume:")
    print(result)

    # ==================================================================
    # 5. Pipelines — aggregate then mutate
    # ==================================================================
    #
    # Malloy equivalent:
    #   run: airports2 -> {
    #     group_by: state
    #     aggregate: flights2.flight_count, airport_count
    #   } -> {
    #     select: state, flight_count,
    #             flights_per_airport is flight_count / airport_count
    #   }
    # ------------------------------------------------------------------
    print()
    print("=" * 80)
    print("5. Pipelines — aggregate then derive new columns")
    print("=" * 80)
    print()

    result = (
        airports_with_flights.group_by("airports.state")
        .aggregate(
            "airports.airport_count",
            "flights.flight_count",
            "flights.total_distance",
        )
        .mutate(
            flights_per_airport=lambda t: t.flight_count / t.airport_count,
            avg_distance_per_flight=lambda t: t.total_distance / t.flight_count,
        )
        .order_by(lambda t: t.flights_per_airport.desc())
        .limit(10)
        .execute()
    )
    print("States ranked by flights per airport (pipeline: aggregate -> mutate):")
    print(result)

    # ==================================================================
    # 6. Percent-of-total across join tree
    # ==================================================================
    print()
    print("=" * 80)
    print("6. Percent-of-Total via Pipeline")
    print("=" * 80)
    print()

    result = (
        airports_with_flights.group_by("airports.state")
        .aggregate("flights.flight_count")
        .mutate(
            pct_of_flights=lambda t: (
                t.flight_count / t.flight_count.sum() * 100
            ),
        )
        .order_by(lambda t: t.pct_of_flights.desc())
        .limit(10)
        .execute()
    )
    print("Top 10 states by share of total flights:")
    print(result)


if __name__ == "__main__":
    main()
