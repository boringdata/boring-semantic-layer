#!/usr/bin/env python3
"""Window Functions - Rolling Averages, Rankings, Running Totals, and t.all()."""

from pathlib import Path

import ibis
from ibis import _

from boring_semantic_layer import loader, to_semantic_table


def main():
    # Load database connection from profile
    profile_file = Path(__file__).parent / "profiles.yml"
    con = loader.get_connection("example_db", profile_file=profile_file)

    flights_tbl = con.table("flights_tbl")

    flights_with_date = flights_tbl.mutate(
        flight_date=flights_tbl.dep_time.date(),
    )

    flights = to_semantic_table(flights_with_date, name="flights").with_measures(
        flight_count=lambda t: t.count(),
        avg_delay=lambda t: t.dep_delay.mean(),
    )

    daily_stats = (
        flights.group_by("flight_date", "carrier")
        .aggregate("flight_count", "avg_delay")
        .filter(lambda t: t.carrier == "WN")
    )

    result = (
        daily_stats.mutate(
            rolling_avg=lambda t: t.flight_count.mean().over(
                ibis.window(order_by=t.flight_date, preceding=6, following=0),
            ),
            rank=lambda t: ibis.dense_rank().over(
                ibis.window(order_by=ibis.desc(t.flight_count)),
            ),
            running_total=lambda t: t.flight_count.sum().over(
                ibis.window(order_by=t.flight_date),
            ),
        )
        .order_by("flight_date")
        .limit(20)
        .execute()
    )

    flights_with_share = flights.with_measures(
        percent_of_total=lambda t: (t.flight_count / t.all(t.flight_count)) * 100,
    )

    result = (
        flights_with_share.group_by("carrier")
        .aggregate("flight_count", "percent_of_total")
        .order_by(_.percent_of_total.desc())
        .limit(10)
        .execute()
    )

    print("\n using t.all():")
    print(result)

    carrier_stats = flights.group_by("carrier").aggregate("flight_count")

    result = (
        carrier_stats.mutate(
            total_flights=lambda t: t.flight_count.sum().over(ibis.window()),
            percent_manual=lambda t: (t.flight_count / t.flight_count.sum().over(ibis.window()))
            * 100,
        )
        .order_by(_.percent_manual.desc())
        .limit(10)
        .execute()
    )

    print("\n window functions:")
    print(result)


if __name__ == "__main__":
    main()
