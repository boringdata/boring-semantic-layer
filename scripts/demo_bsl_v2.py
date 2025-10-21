#!/usr/bin/env python3
"""
Demo of Boring Semantic Layer (BSL) v2 features, using ibis + DuckDB.

This script showcases:
  - Defining SemanticTables, dimensions, and measures
  - Simple group_by/aggregate queries
  - Percent-of-total via mutate and window functions
  - Composing semantic tables via join_one/join_many
  - Rolling-window calculations
"""
import pandas as pd
import ibis

from boring_semantic_layer.semantic_api import to_semantic_table


def main():
    # Setup in-memory DuckDB
    con = ibis.duckdb.connect(":memory:")

    # ------------------------------------------------------------------------
    # Example 1: Basic SemanticTable definition and simple aggregation
    flights_df = pd.DataFrame({
        "origin": ["A", "B", "A", "C", "B", "A"],
        "distance": [100, 200, 150, 120, 180, 130],
    })
    flights_tbl = con.create_table("flights", flights_df)

    flights_st = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(flight_count=lambda t: t.count(),
                       total_distance=lambda t: t.distance.sum())
    )

    df1 = flights_st.group_by("origin").aggregate("flight_count").execute()
    print("Flights per origin:\n", df1)

    # ------------------------------------------------------------------------
    # Example 2: Percent-of-total (market share) via mutate and window
    expr2 = (
        flights_st
        .group_by("origin")
        .aggregate("flight_count")
        .mutate(
            market_share=lambda t: t.flight_count / t.flight_count.sum()
        )
    )
    df2 = expr2.execute()
    print("\nMarket share per origin:\n", df2)

    # ------------------------------------------------------------------------
    # Example 3: Composing semantic tables via join_one
    marketing_df = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "segment": ["A", "B", "A"],
        "monthly_spend": [100, 200, 150],
    })
    support_df = pd.DataFrame({
        "case_id": [10, 11, 12],
        "customer_id": [1, 2, 3],
        "priority": ["high", "low", "high"],
    })
    marketing_tbl = con.create_table("marketing", marketing_df)
    support_tbl = con.create_table("support", support_df)

    marketing_st = (
        to_semantic_table(marketing_tbl, name="marketing")
        .with_dimensions(customer_id=lambda t: t.customer_id,
                         segment=lambda t: t.segment)
        .with_measures(avg_spend=lambda t: t.monthly_spend.mean())
    )
    support_st = (
        to_semantic_table(support_tbl, name="support")
        .with_dimensions(case_id=lambda t: t.case_id,
                         customer_id=lambda t: t.customer_id,
                         priority=lambda t: t.priority)
        .with_measures(case_count=lambda t: t.count())
    )

    cross_team = (
        marketing_st.join_one(support_st, "customer_id", "customer_id")
        .with_measures(cases_per_spend=lambda t: t.case_count / t.avg_spend)
    )
    df3 = cross_team.group_by("segment").aggregate("cases_per_spend").execute()
    print("\nCases per spend by segment:\n", df3)

    # ------------------------------------------------------------------------
    # Example 4: Rolling-window calculation
    ts_df = pd.DataFrame({
        "date": pd.date_range("2023-01-01", periods=6, freq="D"),
        "value": [10, 20, 30, 40, 50, 60],
    })
    ts_tbl = con.create_table("ts", ts_df)
    ts_st = (
        to_semantic_table(ts_tbl, name="timeseries")
        .with_dimensions(date=lambda t: t.date)
        .with_measures(sum_val=lambda t: t.value.sum())
    )

    rolling_window = ibis.window(order_by="date", rows=(1, 1))
    expr4 = (
        ts_st
        .group_by("date")
        .aggregate("sum_val")
        .mutate(
            rolling_avg=lambda t: t.sum_val.mean().over(rolling_window)
        )
    )
    df4 = expr4.execute()
    print("\nRolling average:\n", df4)


if __name__ == "__main__":
    main()