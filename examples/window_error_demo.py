#!/usr/bin/env python3
"""Demonstrates window function error with xorq vendored tables."""

import ibis
import pandas as pd

from boring_semantic_layer import to_semantic_table

try:
    from xorq.vendor.ibis.backends.profiles import Profile as XorqProfile
except ImportError:
    print("ERROR: xorq is not installed. Run: uv pip install xorq xorq-datafusion")
    exit(1)


def main():
    xorq_profile = XorqProfile(con_name="duckdb", kwargs_tuple=())
    con = xorq_profile.get_con()

    data = {
        "date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"],
        "category": ["A", "A", "B", "A", "B"],
        "value": [10, 20, 30, 40, 50],
    }

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])

    flights_tbl = con.create_table("test_data", df)

    flights = to_semantic_table(flights_tbl, name="flights").with_measures(
        total_value=lambda t: t.value.sum(),
        avg_value=lambda t: t.value.mean(),
    )

    daily_stats = flights.group_by("date", "category").aggregate("total_value", "avg_value")

    print(daily_stats.execute())

    result = daily_stats.mutate(
        rolling_avg=lambda t: t.total_value.mean().over(
            ibis.window(order_by=t.date, preceding=2, following=0),
        ),
    ).execute()
    print(result)


if __name__ == "__main__":
    main()
