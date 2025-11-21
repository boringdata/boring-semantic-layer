#!/usr/bin/env python3
"""Demonstrates window function error with xorq vendored tables.

This example shows the issue that occurs when using window functions
with tables loaded via xorq profiles (which return xorq vendored ibis tables).
"""

import ibis
import pandas as pd

from boring_semantic_layer import to_semantic_table

try:
    from xorq.vendor.ibis.backends.profiles import Profile as XorqProfile
except ImportError:
    print("ERROR: xorq is not installed. Run: uv pip install xorq xorq-datafusion")
    exit(1)


def main():
    """Run a simple window function example that triggers the error."""
    # Use xorq profile to create connection (similar to ProfileLoader)
    # This returns xorq vendored ibis connection
    xorq_profile = XorqProfile(con_name="duckdb", kwargs_tuple=())
    con = xorq_profile.get_con()

    # Create simple test data
    data = {
        "date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"],
        "category": ["A", "A", "B", "A", "B"],
        "value": [10, 20, 30, 40, 50],
    }

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])

    # Table created from xorq connection is xorq vendored ibis table
    flights_tbl = con.create_table("test_data", df)

    # Create semantic table
    # NOTE: On feat/xorqify, to_semantic_table automatically converts
    # regular ibis tables to xorq vendored tables
    flights = to_semantic_table(flights_tbl, name="flights").with_measures(
        total_value=lambda t: t.value.sum(),
        avg_value=lambda t: t.value.mean(),
    )

    # Aggregate by date and category
    daily_stats = flights.group_by("date", "category").aggregate("total_value", "avg_value")

    print("Basic aggregation works:")
    print(daily_stats.execute())
    print()

    # Now try to add a window function - this will trigger the error
    print("Attempting window function with order_by...")
    try:
        result = daily_stats.mutate(
            rolling_avg=lambda t: t.total_value.mean().over(
                ibis.window(order_by=t.date, preceding=2, following=0),
            ),
        ).execute()
        print("SUCCESS! Window function worked:")
        print(result)
    except Exception as e:
        print(f"ERROR: {type(e).__name__}")
        print(f"Message: {str(e)[:200]}...")
        print()
        print("This error occurs because t.date resolves to the entire expression")
        print("tree instead of just the column reference when using xorq tables.")


if __name__ == "__main__":
    main()
