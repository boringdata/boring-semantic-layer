#!/usr/bin/env python3
"""FAA Semantic Model — Star/Snowflake schema over xorq deferred parquet sources.

Uses xorq.deferred_read_parquet for lazy, cacheable data access.

Schema:
    airports -< flights -- carriers
                flights -- aircraft -- models

Usage:
    python examples/faa_semantic_model.py
"""

import xorq.api as xo
from ibis import _

from boring_semantic_layer import to_semantic_table

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"
con = xo.connect()

# ------------------------------------------------------------------
# Deferred parquet sources
# ------------------------------------------------------------------
airports_tbl = xo.deferred_read_parquet(
    f"{BASE_URL}/airports.parquet", con, table_name="airports"
)

flights_tbl = xo.deferred_read_parquet(
    f"{BASE_URL}/flights.parquet", con, table_name="flights"
)

carriers_tbl = xo.deferred_read_parquet(
    f"{BASE_URL}/carriers.parquet", con, table_name="carriers"
)

aircraft_tbl = xo.deferred_read_parquet(
    f"{BASE_URL}/aircraft.parquet", con, table_name="aircraft"
)

aircraft_models_tbl = xo.deferred_read_parquet(
    f"{BASE_URL}/aircraft_models.parquet", con, table_name="aircraft_models"
)

# ------------------------------------------------------------------
# Semantic models (sources)
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
        avg_dep_delay=_.dep_delay.mean(),
        total_dep_delay=_.dep_delay.sum(),
        min_dep_delay=_.dep_delay.min(),
        max_dep_delay=_.dep_delay.max(),
    )
)

airports = (
    to_semantic_table(airports_tbl, name="airports")
    .with_dimensions(
        code=_.code,
        state=_.state,
        city=_.city,
        fac_type=_.fac_type,
    )
    .with_measures(airport_count=_.count())
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

# ------------------------------------------------------------------
# Join tree
# ------------------------------------------------------------------
# Snowflake arm: aircraft -- models
aircraft_with_models = aircraft.join_one(aircraft_models, on="aircraft_model_code")

# Star: flights -- carriers, flights -- aircraft -- models
flights_full = (
    flights
    .join_one(carriers, on=lambda f, c: f.carrier == c.code)
    .join_one(aircraft_with_models, on="tail_num")
)

# Root: airports -< flights (with all arms)
faa = airports.join_many(
    flights_full,
    on=lambda a, f: a.code == f.origin,
)


def main():
    print("=" * 80)
    print("FAA Semantic Model — xorq deferred sources")
    print("=" * 80)

    # -- Scalar aggregates across the full schema ----------------------
    print("\n1. Scalar aggregates (full schema)")
    df = faa.aggregate(
        "airports.airport_count",
        "flights.flight_count",
        "flights.total_distance",
        "flights.avg_distance",
    ).execute()
    print(df)

    # -- Group by state ------------------------------------------------
    print("\n2. By origin state (top 10)")
    df = (
        faa.group_by("airports.state")
        .aggregate(
            "airports.airport_count",
            "flights.flight_count",
            "flights.avg_dep_delay",
        )
        .order_by(lambda t: t["flights.flight_count"].desc())
        .limit(10)
        .execute()
    )
    print(df)

    # -- Group by carrier nickname -------------------------------------
    print("\n3. By carrier (top 10)")
    df = (
        faa.group_by("carriers.nickname")
        .aggregate("flights.flight_count", "flights.avg_distance")
        .order_by(lambda t: t["flights.flight_count"].desc())
        .limit(10)
        .execute()
    )
    print(df)

    # -- Group by manufacturer (4 levels deep) -------------------------
    print("\n4. By aircraft manufacturer (top 10)")
    df = (
        faa.group_by("models.manufacturer")
        .aggregate("flights.flight_count", "models.avg_seats")
        .order_by(lambda t: t["flights.flight_count"].desc())
        .limit(10)
        .execute()
    )
    print(df)

    # -- Cross-arm group by: carrier x manufacturer --------------------
    print("\n5. Carrier x Manufacturer (top 15)")
    df = (
        faa.group_by("carriers.nickname", "models.manufacturer")
        .aggregate("flights.flight_count")
        .order_by(lambda t: t["flights.flight_count"].desc())
        .limit(15)
        .execute()
    )
    print(df)

    # -- Filter: long-haul flights from California ---------------------
    print("\n6. Long-haul flights (>1000 mi) from California")
    df = (
        faa.filter(lambda t: t.state == "CA")
        .filter(lambda t: t.distance > 1000)
        .group_by("airports.city")
        .aggregate("flights.flight_count", "flights.avg_distance")
        .order_by(lambda t: t["flights.flight_count"].desc())
        .limit(10)
        .execute()
    )
    print(df)

    # -- Min/Max across the star schema --------------------------------
    print("\n7. Min/Max dep delay by state (top 10 by max)")
    df = (
        faa.group_by("airports.state")
        .aggregate("flights.min_dep_delay", "flights.max_dep_delay")
        .order_by(lambda t: t["flights.max_dep_delay"].desc())
        .limit(10)
        .execute()
    )
    print(df)

    # -- Pipeline: aggregate then mutate (percent of total) ------------
    print("\n8. Percent of total flights by state (top 10)")
    df = (
        faa.group_by("airports.state")
        .aggregate("flights.flight_count")
        .mutate(
            pct_of_flights=lambda t: (
                t["flights.flight_count"] / t["flights.flight_count"].sum() * 100
            ),
        )
        .order_by(lambda t: t.pct_of_flights.desc())
        .limit(10)
        .execute()
    )
    print(df)

    # -- Unmatched rows: states with airports but no flights -----------
    print("\n9. States with airports but no flights (unmatched rows)")
    df = (
        faa.group_by("airports.state")
        .aggregate("airports.airport_count", "flights.flight_count")
        .filter(lambda t: t["flights.flight_count"].isnull())
        .order_by(lambda t: t["airports.airport_count"].desc())
        .limit(10)
        .execute()
    )
    print(df)


if __name__ == "__main__":
    main()
