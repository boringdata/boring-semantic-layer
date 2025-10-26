#!/usr/bin/env python3
"""Percent of Total - Market Share Analysis.

Malloy: https://docs.malloydata.dev/documentation/patterns/percent_of_total
"""

import ibis
from ibis import _

from boring_semantic_layer import to_semantic_table

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def main():
    con = ibis.duckdb.connect(":memory:")
    flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")
    carriers_tbl = con.read_parquet(f"{BASE_URL}/carriers.parquet")

    flights_with_carriers = flights_tbl.join(
        carriers_tbl,
        flights_tbl.carrier == carriers_tbl.code,
        how="inner",
    )

    flights = (
        to_semantic_table(flights_with_carriers, name="flights")
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
        )
        .with_measures(
            market_share=lambda t: t.flight_count / t.all(t.flight_count) * 100,
            distance_share=lambda t: t.total_distance / t.all(t.total_distance) * 100,
        )
    )

    result = (
        flights.group_by("nickname")
        .aggregate("flight_count", "market_share")
        .order_by(_.market_share.desc())
        .limit(10)
        .execute()
    )
    print("\nMarket share by carrier:")
    print(result)

    result = (
        flights.group_by("origin", "nickname")
        .aggregate("flight_count", "market_share")
        .order_by(_.market_share.desc())
        .limit(15)
        .execute()
    )
    print("\nMarket share by origin and carrier:")
    print(result)


if __name__ == "__main__":
    main()
