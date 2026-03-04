#!/usr/bin/env python3
"""FAA Comprehensive Semantic Model — Full coverage of measure types, calculated
measures, time/date dimensions, and multi-level joins.

Exercises:
  1. All aggregation types: count, sum, mean, min, max
  2. Calculated measures: percent_of_total, ratios, on-time rate
  3. Time dimensions: dep_time truncated to month/day/hour
  4. Date dimensions: aircraft cert dates
  5. Multi-level snowflake joins (airports -< flights -- carriers -- aircraft -- models)
  6. Filters, pipelines (aggregate -> mutate), unmatched rows
  7. Window functions via post-agg mutate

Schema:
    airports -< flights -- carriers
                flights -- aircraft -- models

Usage:
    python examples/faa_comprehensive_model.py
"""

import ibis
from ibis import _

from boring_semantic_layer import to_semantic_table

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def main():
    con = ibis.duckdb.connect(":memory:")

    # ------------------------------------------------------------------
    # Raw tables
    # ------------------------------------------------------------------
    airports_tbl = con.read_parquet(f"{BASE_URL}/airports.parquet")
    flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")
    carriers_tbl = con.read_parquet(f"{BASE_URL}/carriers.parquet")
    aircraft_tbl = con.read_parquet(f"{BASE_URL}/aircraft.parquet")
    aircraft_models_tbl = con.read_parquet(f"{BASE_URL}/aircraft_models.parquet")

    # ------------------------------------------------------------------
    # 1. Semantic models with rich measure/dimension sets
    # ------------------------------------------------------------------

    # Carriers — reference table
    carriers = (
        to_semantic_table(carriers_tbl, name="carriers")
        .with_dimensions(
            code=_.code,
            name=_.name,
            nickname=_.nickname,
        )
        .with_measures(carrier_count=_.count())
    )

    # Flights — fact table with time dimensions and all measure types
    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            origin=_.origin,
            destination=_.destination,
            carrier=_.carrier,
            tail_num=_.tail_num,
            dep_month=lambda t: t.dep_time.truncate("M"),
            dep_date=lambda t: t.dep_time.truncate("D"),
            dep_hour=lambda t: t.dep_time.hour(),
            is_cancelled=lambda t: t.cancelled == "Y",
        )
        .with_measures(
            # count
            flight_count=_.count(),
            # sum
            total_distance=_.distance.sum(),
            total_dep_delay=_.dep_delay.sum(),
            total_arr_delay=_.arr_delay.sum(),
            total_taxi_out=_.taxi_out.sum(),
            # mean
            avg_distance=_.distance.mean(),
            avg_dep_delay=_.dep_delay.mean(),
            avg_arr_delay=_.arr_delay.mean(),
            avg_taxi_out=_.taxi_out.mean(),
            # min / max
            min_dep_delay=_.dep_delay.min(),
            max_dep_delay=_.dep_delay.max(),
            min_distance=_.distance.min(),
            max_distance=_.distance.max(),
            # calculated: percent of total (references flight_count via t.all)
            pct_of_total_flights=lambda t: (
                t.flight_count / t.all(t.flight_count) * 100
            ),
            # calculated: distance share (references total_distance via t.all)
            distance_share=lambda t: (
                t.total_distance / t.all(t.total_distance) * 100
            ),
            # calculated: ratio of two base measures (no t.all)
            avg_miles_per_flight=lambda t: t.total_distance / t.flight_count,
            # calculated: delay ratio
            delay_per_mile=lambda t: t.total_dep_delay / t.total_distance,
        )
    )

    # Airports — dimension table
    airports = (
        to_semantic_table(airports_tbl, name="airports")
        .with_dimensions(
            code=_.code,
            state=_.state,
            city=_.city,
            fac_type=_.fac_type,
            elevation=_.elevation,
            cntl_twr=_.cntl_twr,
        )
        .with_measures(
            airport_count=_.count(),
            avg_elevation=_.elevation.mean(),
            max_elevation=_.elevation.max(),
            min_elevation=_.elevation.min(),
        )
    )

    # Aircraft — dimension table with date dimensions
    aircraft = (
        to_semantic_table(aircraft_tbl, name="aircraft")
        .with_dimensions(
            tail_num=_.tail_num,
            aircraft_model_code=_.aircraft_model_code,
            year_built=lambda t: t.year_built,
        )
        .with_measures(aircraft_count=_.count())
    )

    # Aircraft models — dimension table
    aircraft_models = (
        to_semantic_table(aircraft_models_tbl, name="models")
        .with_dimensions(
            aircraft_model_code=_.aircraft_model_code,
            manufacturer=_.manufacturer,
            model=_.model,
            seats=_.seats,
            engines=_.engines,
        )
        .with_measures(
            model_count=_.count(),
            avg_seats=_.seats.mean(),
            max_seats=_.seats.max(),
            total_seats=_.seats.sum(),
        )
    )

    # ------------------------------------------------------------------
    # 2. Join tree (snowflake)
    # ------------------------------------------------------------------
    aircraft_with_models = aircraft.join_one(aircraft_models, on="aircraft_model_code")

    flights_full = (
        flights
        .join_one(carriers, on=lambda f, c: f.carrier == c.code)
        .join_one(aircraft_with_models, on="tail_num")
    )

    faa = airports.join_many(
        flights_full,
        on=lambda a, f: a.code == f.origin,
    )

    # ==================================================================
    # QUERIES
    # ==================================================================

    # -- Q1: All scalar aggregates across full schema -------------------
    print("=" * 80)
    print("Q1: Scalar aggregates (all measure types)")
    print("=" * 80)
    df = faa.aggregate(
        "airports.airport_count",
        "flights.flight_count",
        "flights.total_distance",
        "flights.avg_distance",
        "flights.avg_dep_delay",
        "flights.min_dep_delay",
        "flights.max_dep_delay",
        "flights.total_dep_delay",
    ).execute()
    print(df)
    print()

    # -- Q2: Group by time dimension (month) ----------------------------
    print("=" * 80)
    print("Q2: Flights by departure month")
    print("=" * 80)
    df = (
        flights.group_by("dep_month")
        .aggregate("flight_count", "avg_dep_delay", "avg_distance")
        .order_by(lambda t: t.dep_month)
        .execute()
    )
    print(df)
    print()

    # -- Q3: Group by hour-of-day (time grain) --------------------------
    print("=" * 80)
    print("Q3: Flights by departure hour (busiest times)")
    print("=" * 80)
    df = (
        flights.group_by("dep_hour")
        .aggregate("flight_count", "avg_dep_delay")
        .order_by(lambda t: t.flight_count.desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    # -- Q4: Calculated measure — percent of total ----------------------
    print("=" * 80)
    print("Q4: Calculated measure — percent of total flights by carrier")
    print("=" * 80)
    df = (
        flights.group_by("carrier")
        .aggregate("flight_count", "pct_of_total_flights")
        .order_by(lambda t: t.pct_of_total_flights.desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    # -- Q5: Calculated measure — distance share ------------------------
    print("=" * 80)
    print("Q5: Calculated measure — distance share by carrier")
    print("=" * 80)
    df = (
        flights.group_by("carrier")
        .aggregate("flight_count", "distance_share", "avg_distance")
        .order_by(lambda t: t.distance_share.desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    # -- Q6: Star schema group-by across 3 levels -----------------------
    print("=" * 80)
    print("Q6: Star schema — state x carrier nickname (3 levels)")
    print("=" * 80)
    df = (
        faa.group_by("airports.state", "carriers.nickname")
        .aggregate("flights.flight_count", "flights.avg_dep_delay")
        .order_by(lambda t: t["flights.flight_count"].desc())
        .limit(15)
        .execute()
    )
    print(df)
    print()

    # -- Q7: Snowflake — 4 levels deep (state x manufacturer) ----------
    print("=" * 80)
    print("Q7: Snowflake — state x manufacturer (4 levels deep)")
    print("=" * 80)
    df = (
        faa.group_by("airports.state", "models.manufacturer")
        .aggregate("flights.flight_count", "models.avg_seats")
        .order_by(lambda t: t["flights.flight_count"].desc())
        .limit(15)
        .execute()
    )
    print(df)
    print()

    # -- Q8: Pipeline — aggregate then mutate (flights per airport) -----
    print("=" * 80)
    print("Q8: Pipeline — flights per airport by state")
    print("=" * 80)
    df = (
        faa.group_by("airports.state")
        .aggregate(
            "airports.airport_count",
            "flights.flight_count",
            "flights.total_distance",
        )
        .mutate(
            flights_per_airport=lambda t: (
                t["flights.flight_count"] / t["airports.airport_count"]
            ),
            avg_dist_per_flight=lambda t: (
                t["flights.total_distance"] / t["flights.flight_count"]
            ),
        )
        .order_by(lambda t: t.flights_per_airport.desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    # -- Q9: Pipeline — percent of total via post-agg mutate ------------
    print("=" * 80)
    print("Q9: Pipeline — percent of total flights by state (post-agg)")
    print("=" * 80)
    df = (
        faa.group_by("airports.state")
        .aggregate("flights.flight_count")
        .mutate(
            pct_of_flights=lambda t: (
                t["flights.flight_count"]
                / t["flights.flight_count"].sum()
                * 100
            ),
        )
        .order_by(lambda t: t.pct_of_flights.desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    # -- Q10: Min/Max across the full star schema -----------------------
    print("=" * 80)
    print("Q10: Min/Max dep delay and distance by state")
    print("=" * 80)
    df = (
        faa.group_by("airports.state")
        .aggregate(
            "flights.min_dep_delay",
            "flights.max_dep_delay",
            "flights.min_distance",
            "flights.max_distance",
        )
        .order_by(lambda t: t["flights.max_dep_delay"].desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    # -- Q11: Filter pushdown — California long-haul --------------------
    print("=" * 80)
    print("Q11: Filter — long-haul flights from California")
    print("=" * 80)
    df = (
        faa.filter(lambda t: t.state == "CA")
        .filter(lambda t: t.distance > 1000)
        .group_by("airports.city")
        .aggregate(
            "flights.flight_count",
            "flights.avg_distance",
            "flights.avg_dep_delay",
        )
        .order_by(lambda t: t["flights.flight_count"].desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    # -- Q12: Mean across join tree (verifies Mean decomposition) -------
    print("=" * 80)
    print("Q12: Mean measures across join tree (decomposition check)")
    print("=" * 80)
    df = (
        faa.group_by("airports.state")
        .aggregate(
            "flights.avg_dep_delay",
            "flights.avg_arr_delay",
            "flights.avg_taxi_out",
            "flights.avg_distance",
            "airports.avg_elevation",
        )
        .order_by(lambda t: t["flights.avg_dep_delay"].desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    # -- Q13: Aircraft year dimension (date-derived) --------------------
    print("=" * 80)
    print("Q13: Flights by aircraft year built")
    print("=" * 80)
    df = (
        faa.group_by("aircraft.year_built")
        .aggregate("flights.flight_count", "flights.avg_distance")
        .filter(lambda t: t["aircraft.year_built"].notnull())
        .order_by(lambda t: t["flights.flight_count"].desc())
        .limit(15)
        .execute()
    )
    print(df)
    print()

    # -- Q14: Unmatched rows — airports with no flights -----------------
    print("=" * 80)
    print("Q14: Unmatched rows — states with airports but no flights")
    print("=" * 80)
    df = (
        faa.group_by("airports.state")
        .aggregate("airports.airport_count", "flights.flight_count")
        .filter(lambda t: t["flights.flight_count"].isnull())
        .order_by(lambda t: t["airports.airport_count"].desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    # -- Q15: Mixed agg types in single query ---------------------------
    print("=" * 80)
    print("Q15: Mixed aggregation types (count, sum, mean, min, max)")
    print("=" * 80)
    df = (
        faa.group_by("carriers.nickname")
        .aggregate(
            "flights.flight_count",
            "flights.total_distance",
            "flights.avg_distance",
            "flights.min_distance",
            "flights.max_distance",
            "flights.avg_dep_delay",
            "flights.min_dep_delay",
            "flights.max_dep_delay",
        )
        .order_by(lambda t: t["flights.flight_count"].desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    # -- Q16: Elevation-based analysis ----------------------------------
    print("=" * 80)
    print("Q16: Airport elevation vs. flight volume")
    print("=" * 80)
    df = (
        faa.group_by("airports.state")
        .aggregate(
            "airports.airport_count",
            "airports.avg_elevation",
            "airports.max_elevation",
            "flights.flight_count",
        )
        .mutate(
            flights_per_airport=lambda t: (
                t["flights.flight_count"] / t["airports.airport_count"]
            ),
        )
        .order_by(lambda t: t["airports.avg_elevation"].desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    # -- Q17: Carrier + models (cross-arm aggregation) ------------------
    print("=" * 80)
    print("Q17: Cross-arm — carrier x model manufacturer with seats")
    print("=" * 80)
    df = (
        faa.group_by("carriers.nickname", "models.manufacturer")
        .aggregate(
            "flights.flight_count",
            "models.avg_seats",
        )
        .order_by(lambda t: t["flights.flight_count"].desc())
        .limit(15)
        .execute()
    )
    print(df)
    print()

    # -- Q18: Calc measure through join tree (ratio) ----------------------
    print("=" * 80)
    print("Q18: Calc measure — avg_miles_per_flight through join_many")
    print("=" * 80)
    df = (
        faa.group_by("airports.state")
        .aggregate("flights.avg_miles_per_flight", "flights.flight_count")
        .order_by(lambda t: t["flights.avg_miles_per_flight"].desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    # -- Q19: Calc measure through join tree (t.all pct_of_total) --------
    print("=" * 80)
    print("Q19: Calc measure — pct_of_total_flights through join_many")
    print("=" * 80)
    df = (
        faa.group_by("airports.state")
        .aggregate("flights.pct_of_total_flights")
        .order_by(lambda t: t["flights.pct_of_total_flights"].desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    # -- Q20: Calc measure through join tree (delay_per_mile) ------------
    print("=" * 80)
    print("Q20: Calc measure — delay_per_mile through join_many")
    print("=" * 80)
    df = (
        faa.group_by("carriers.nickname")
        .aggregate("flights.delay_per_mile", "flights.flight_count")
        .order_by(lambda t: t["flights.delay_per_mile"].desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    # -- Q21: Multiple calc measures in one query through join tree ------
    print("=" * 80)
    print("Q21: Multiple calc measures through join tree")
    print("=" * 80)
    df = (
        faa.group_by("airports.state")
        .aggregate(
            "flights.flight_count",
            "flights.avg_miles_per_flight",
            "flights.pct_of_total_flights",
            "flights.delay_per_mile",
        )
        .order_by(lambda t: t["flights.flight_count"].desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    # -- Q22: Calc measure grouped by snowflake dimension ----------------
    print("=" * 80)
    print("Q22: Calc measure grouped by 4-level deep dimension")
    print("=" * 80)
    df = (
        faa.group_by("models.manufacturer")
        .aggregate(
            "flights.flight_count",
            "flights.avg_miles_per_flight",
            "flights.distance_share",
        )
        .order_by(lambda t: t["flights.flight_count"].desc())
        .limit(10)
        .execute()
    )
    print(df)
    print()

    print("=" * 80)
    print("All 22 queries completed successfully!")
    print("=" * 80)


if __name__ == "__main__":
    main()
