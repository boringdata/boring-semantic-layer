#!/usr/bin/env python3
"""Window Functions - Rolling Averages, Rankings, Running Totals.
https://docs.malloydata.dev/documentation/patterns/moving_avg
"""

import ibis
from boring_semantic_layer import to_semantic_table, to_ibis

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def main():
    con = ibis.duckdb.connect(":memory:")
    flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")

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
        to_ibis(daily_stats)
        .mutate(
            rolling_avg=lambda t: t.flight_count.mean().over(
                ibis.window(order_by=t.flight_date, preceding=6, following=0)
            ),
            rank=lambda t: ibis.dense_rank().over(
                ibis.window(order_by=ibis.desc(t.flight_count))
            ),
            running_total=lambda t: t.flight_count.sum().over(
                ibis.window(order_by=t.flight_date)
            ),
        )
        .order_by("flight_date")
        .limit(20)
        .execute()
    )

    print("\nDaily WN flights with window functions:")
    print(
        result[["flight_date", "flight_count", "rolling_avg", "rank", "running_total"]]
    )


if __name__ == "__main__":
    main()
