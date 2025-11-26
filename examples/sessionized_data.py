#!/usr/bin/env python3
"""Sessionized Data - Map/Reduce Pattern.
https://docs.malloydata.dev/documentation/patterns/sessionize

Flight event data contains dep_time, carrier, origin, destination and tail_num
(the plane that made the flight). The query below takes the flight event data
and maps it into sessions of flight_date, carrier, and tail_num. For each session,
a nested list of flight_legs by the aircraft on that day. The flight legs are numbered.
"""

from pathlib import Path

import ibis
import pandas as pd

from boring_semantic_layer import from_yaml, to_untagged

# Show all columns in output
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)


def main():
    # Load semantic models from YAML with profile
    yaml_path = Path(__file__).parent / "flights.yml"
    profile_file = Path(__file__).parent / "profiles.yml"
    models = from_yaml(str(yaml_path), profile="example_db", profile_path=str(profile_file))

    # Use flights model from YAML (already has all measures including max_delay)
    flights = models["flights"]

    # Filter for carrier WN on 2002-03-03 and add flight_date column
    filtered_flights = flights.filter(
        lambda t: (t.carrier == "WN") & (t.dep_time.date() == ibis.date(2002, 3, 3)),
    ).mutate(flight_date=lambda t: t.dep_time.date())

    # Create sessions with nested flight legs
    sessions = (
        filtered_flights.group_by("flight_date", "carrier", "tail_num")
        .aggregate(
            "flight_count",
            "max_delay",
            "total_distance",
            nest={
                "flight_legs": lambda t: t.group_by([
                    "tail_num",
                    "dep_time",
                    "origin",
                    "destination",
                    "dep_delay",
                    "arr_delay",
                ]),
            },
        )
        .mutate(session_id=ibis.row_number().over(ibis.window()))
        .order_by("session_id")
    )

    print("Sessions with nested flight legs:")
    sessions_result = sessions.execute()
    print(sessions_result)
    print()

    # Normalize by unnesting flight_legs - each leg becomes its own row
    # Convert semantic expression to ibis, unnest the array column, then execute
    sessions_ibis = to_untagged(sessions)
    unnested = sessions_ibis.unnest("flight_legs")

    # Unpack the struct fields into individual columns
    struct_col = unnested.flight_legs
    normalized = unnested.select(
        "flight_date",
        "carrier",
        "tail_num",
        "flight_count",
        "max_delay",
        "total_distance",
        "session_id",
        leg_tail_num=struct_col.tail_num,
        dep_time=struct_col.dep_time,
        origin=struct_col.origin,
        destination=struct_col.destination,
        dep_delay=struct_col.dep_delay,
        arr_delay=struct_col.arr_delay,
    ).execute()
    print("Normalized (one row per flight leg):")
    print(normalized)


if __name__ == "__main__":
    main()
