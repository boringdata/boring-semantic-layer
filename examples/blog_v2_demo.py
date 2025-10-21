#!/usr/bin/env python3
"""
BSL v2 Blog Post Examples - Runnable Demo

This script demonstrates all the examples from the BSL v2 blog post:
https://julienhuraultsubstack.com/p/boring-semantic-layer-v2

Run this to verify all blog examples work correctly.
"""

import pandas as pd
import ibis
from boring_semantic_layer.semantic_api import to_semantic_table


def print_section(title):
    """Pretty print section headers."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def demo_basic_semantic_table():
    """Example 1: Basic SemanticTable definition."""
    print_section("Example 1: Basic SemanticTable Definition")

    con = ibis.duckdb.connect(":memory:")

    # Create sample flights data
    flights_df = pd.DataFrame({
        "origin": ["JFK", "LAX", "JFK", "ORD", "LAX", "JFK"],
        "destination": ["LAX", "JFK", "ORD", "JFK", "ORD", "LAX"],
        "distance": [2475, 2475, 740, 740, 1744, 2475],
        "carrier": ["AA", "UA", "AA", "UA", "AA", "UA"],
    })

    flights_tbl = con.create_table("flights", flights_df)

    # Convert to semantic table
    flight_semantic_table = to_semantic_table(flights_tbl, name="flights")

    # Add dimensions and measures
    flight_semantic_table = (
        flight_semantic_table
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(flight_count=lambda t: t.count())
    )

    # Query with group_by and aggregate
    result = (
        flight_semantic_table
        .group_by("origin")
        .aggregate("flight_count")
        .execute()
    )

    print("Flight counts by origin:")
    print(result)

    return flight_semantic_table, con


def demo_adhoc_transformations(flight_semantic_table):
    """Example 2: Ad-hoc transformations alongside semantic queries."""
    print_section("Example 2: Ad-hoc Transformations")

    # Mix semantic measures with ad-hoc aggregations
    result = (
        flight_semantic_table
        .group_by("origin")  # origin = dimension
        .aggregate(
            "flight_count",  # flight_count = measure
            mean_distance=lambda t: t.distance.mean()  # ad-hoc aggregation
        )
        .execute()
    )

    print("Flights and average distance by origin:")
    print(result)


def demo_percent_of_total(flight_semantic_table):
    """Example 3: Percentage of total (market share)."""
    print_section("Example 3: Percentage of Total (Market Share)")

    result = (
        flight_semantic_table
        .group_by("carrier")
        .aggregate("flight_count")
        .mutate(
            market_share=lambda t: t["flight_count"] / t.all(t["flight_count"])
        )
        .execute()
    )

    print("Market share by carrier:")
    print(result)


def demo_rolling_averages(con):
    """Example 4: Rolling averages with window functions."""
    print_section("Example 4: Rolling Averages")

    # Create monthly time series data
    dates = pd.date_range("2023-01-01", periods=12, freq="ME")
    monthly_df = pd.DataFrame({
        "month": dates,
        "flights": [100, 120, 115, 130, 140, 135, 150, 160, 155, 170, 180, 175],
    })

    monthly_tbl = con.create_table("monthly_flights", monthly_df)

    flights_st = (
        to_semantic_table(monthly_tbl, name="monthly_flights")
        .with_dimensions(month=lambda t: t.month)
        .with_measures(monthly_flights=lambda t: t.flights.sum())
    )

    # Define rolling window
    rolling_window = ibis.window(order_by="month", preceding=2, following=0)

    result = (
        flights_st
        .group_by("month")
        .aggregate("monthly_flights")
        .mutate(
            rolling_avg=lambda t: t["monthly_flights"].mean().over(rolling_window)
        )
        .order_by("month")
        .execute()
    )

    print("Monthly flights with 3-month rolling average:")
    print(result)


def demo_composability(con):
    """Example 5: Cross-team composability."""
    print_section("Example 5: Cross-Team Composability")

    # Marketing team's Users semantic table
    users_df = pd.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "segment": ["Premium", "Basic", "Premium", "Basic", "Enterprise"],
        "signup_date": pd.to_datetime([
            "2023-01-15", "2023-02-20", "2023-03-10", "2023-04-05", "2023-05-12"
        ]),
        "monthly_spend": [500.0, 100.0, 600.0, 150.0, 2000.0],
    })

    users_tbl = con.create_table("users", users_df)

    marketing_st = (
        to_semantic_table(users_tbl, name="users")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
            segment=lambda t: t.segment,
            signup_date=lambda t: t.signup_date,
        )
        .with_measures(
            user_count=lambda t: t.customer_id.count(),
            total_revenue=lambda t: t.monthly_spend.sum(),
            avg_revenue_per_user=lambda t: t.monthly_spend.mean(),
        )
    )

    # Support team's SupportCases semantic table
    support_df = pd.DataFrame({
        "case_id": [101, 102, 103, 104, 105, 106, 107],
        "customer_id": [1, 2, 1, 3, 4, 1, 5],
        "created_date": pd.to_datetime([
            "2023-06-01", "2023-06-05", "2023-06-10",
            "2023-06-15", "2023-06-20", "2023-06-25", "2023-06-30"
        ]),
        "priority": ["High", "Low", "High", "Medium", "Low", "High", "High"],
    })

    support_tbl = con.create_table("support_cases", support_df)

    support_st = (
        to_semantic_table(support_tbl, name="support_cases")
        .with_dimensions(
            case_id=lambda t: t.case_id,
            customer_id=lambda t: t.customer_id,
            created_date=lambda t: t.created_date,
            priority=lambda t: t.priority,
        )
        .with_measures(
            case_count=lambda t: t.case_id.count(),
            high_priority_cases=lambda t: (t.priority == "High").sum(),
        )
    )

    # Join marketing and support semantic tables
    # First join the underlying ibis tables
    joined_tbl = users_tbl.join(
        support_tbl,
        users_tbl.customer_id == support_tbl.customer_id,
        how="inner"
    )

    # Create a new semantic table from the join
    cross_team_st = (
        to_semantic_table(joined_tbl, name="cross_team")
        .with_dimensions(
            segment=lambda t: t.segment,
            priority=lambda t: t.priority,
        )
        .with_measures(
            case_count=lambda t: t.case_id.count(),
            avg_spend=lambda t: t.monthly_spend.mean(),
        )
    )

    # Query across teams: cases per customer segment
    efficiency_metrics = (
        cross_team_st
        .group_by("segment")  # dimension from marketing
        .aggregate("case_count", "avg_spend")  # measures from both teams
        .mutate(
            cases_per_1k_revenue=lambda t: (t["case_count"] / t["avg_spend"]) * 1000
        )
        .execute()
    )

    print("Support efficiency metrics by customer segment:")
    print(efficiency_metrics)

    # Detailed breakdown by segment and priority
    detailed_metrics = (
        cross_team_st
        .group_by("segment", "priority")
        .aggregate("case_count")
        .order_by("segment", "priority")
        .execute()
    )

    print("\nDetailed case breakdown by segment and priority:")
    print(detailed_metrics)


def demo_advanced_patterns(con):
    """Example 6: Advanced analytical patterns."""
    print_section("Example 6: Advanced Analytical Patterns")

    # Create sales data with multiple dimensions
    sales_df = pd.DataFrame({
        "date": pd.date_range("2023-01-01", periods=30, freq="D"),
        "product": ["A", "B"] * 15,
        "region": (["North"] * 10 + ["South"] * 10 + ["East"] * 10),
        "sales": [
            100, 150, 120, 160, 110, 170, 130, 180, 140, 190,
            150, 200, 160, 210, 170, 220, 180, 230, 190, 240,
            200, 250, 210, 260, 220, 270, 230, 280, 240, 290,
        ],
    })

    sales_tbl = con.create_table("sales", sales_df)

    sales_st = (
        to_semantic_table(sales_tbl, name="sales")
        .with_dimensions(
            date=lambda t: t.date,
            product=lambda t: t.product,
            region=lambda t: t.region,
        )
        .with_measures(
            total_sales=lambda t: t.sales.sum(),
            avg_sales=lambda t: t.sales.mean(),
        )
    )

    # Pattern 1: Cumulative sum (running total)
    print("\n--- Pattern 1: Running Total ---")
    result = (
        sales_st
        .group_by("date")
        .aggregate("total_sales")
        .mutate(
            cumulative_sales=lambda t: t["total_sales"].sum().over(
                ibis.window(order_by="date", preceding=None, following=0)
            )
        )
        .order_by("date")
        .limit(10)
        .execute()
    )
    print("Daily sales with running total (first 10 days):")
    print(result)

    # Pattern 2: Rank within groups
    print("\n--- Pattern 2: Rankings ---")
    result = (
        sales_st
        .group_by("product", "region")
        .aggregate("total_sales")
        .mutate(
            rank_overall=lambda t: t["total_sales"].rank(),
            rank_by_product=lambda t: t["total_sales"].rank().over(
                ibis.window(group_by="product", order_by=ibis.desc(t["total_sales"]))
            ),
        )
        .order_by("product", ibis.desc("total_sales"))
        .execute()
    )
    print("Sales rankings by product and region:")
    print(result)

    # Pattern 3: Percent of total by multiple dimensions
    print("\n--- Pattern 3: Multi-Dimensional Percent of Total ---")
    result = (
        sales_st
        .group_by("product", "region")
        .aggregate("total_sales")
        .mutate(
            pct_of_total=lambda t: t["total_sales"] / t.all(t["total_sales"]),
            pct_of_product=lambda t: t["total_sales"] / t["total_sales"].sum().over(
                ibis.window(group_by="product")
            ),
            pct_of_region=lambda t: t["total_sales"] / t["total_sales"].sum().over(
                ibis.window(group_by="region")
            ),
        )
        .execute()
    )
    print("Multi-dimensional market share analysis:")
    print(result)


def demo_filter_examples(con):
    """Example 7: Flexible filtering."""
    print_section("Example 7: Flexible Filtering")

    # Create sample data
    orders_df = pd.DataFrame({
        "order_id": list(range(1, 21)),
        "customer_id": [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
        "order_date": pd.date_range("2023-01-01", periods=20, freq="D"),
        "amount": [100, 150, 200, 120, 180, 90, 110, 160, 140, 130,
                  170, 190, 80, 150, 120, 200, 110, 140, 160, 180],
        "status": ["completed"] * 15 + ["cancelled"] * 5,
    })

    orders_tbl = con.create_table("orders", orders_df)

    orders_st = (
        to_semantic_table(orders_tbl, name="orders")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
            order_month=lambda t: t.order_date.truncate("month"),
            status=lambda t: t.status,
        )
        .with_measures(
            order_count=lambda t: t.count(),
            total_revenue=lambda t: t.amount.sum(),
            avg_order_value=lambda t: t.amount.mean(),
        )
    )

    # Filter before aggregation
    print("--- Completed orders only ---")
    result = (
        orders_st
        .filter(lambda t: t.status == "completed")
        .group_by("customer_id")
        .aggregate("order_count", "total_revenue")
        .execute()
    )
    print(result)

    # Filter on date range
    print("\n--- Orders from January 2023 ---")
    result = (
        orders_st
        .filter(lambda t: (t.order_date >= pd.Timestamp("2023-01-01")) &
                         (t.order_date < pd.Timestamp("2023-02-01")))
        .group_by("customer_id")
        .aggregate("order_count", "total_revenue")
        .execute()
    )
    print(result)


def demo_inline_measure_definitions():
    """Example 8: Inline measure definitions in aggregate."""
    print_section("Example 8: Inline Measure Definitions")

    con = ibis.duckdb.connect(":memory:")

    products_df = pd.DataFrame({
        "product_id": [1, 2, 3, 4, 5],
        "category": ["Electronics", "Clothing", "Electronics", "Home", "Clothing"],
        "price": [999.99, 49.99, 1499.99, 29.99, 79.99],
        "units_sold": [50, 200, 30, 500, 150],
    })

    products_tbl = con.create_table("products", products_df)

    products_st = (
        to_semantic_table(products_tbl, name="products")
        .with_dimensions(category=lambda t: t.category)
    )

    # Define measures inline during aggregation
    result = (
        products_st
        .group_by("category")
        .aggregate(
            total_revenue=lambda t: (t.price * t.units_sold).sum(),
            units_sold=lambda t: t.units_sold.sum(),
            avg_price=lambda t: t.price.mean(),
        )
        .mutate(
            avg_revenue_per_unit=lambda t: t["total_revenue"] / t["units_sold"]
        )
        .order_by(ibis.desc("total_revenue"))
        .execute()
    )

    print("Product category analysis with inline measures:")
    print(result)


def main():
    """Run all blog post examples."""
    print("\n" + "#" * 80)
    print("#" + " " * 78 + "#")
    print("#" + "  BSL v2 Blog Post Examples - Complete Runnable Demo".center(78) + "#")
    print("#" + " " * 78 + "#")
    print("#" * 80)

    # Run all examples
    flight_semantic_table, con = demo_basic_semantic_table()
    demo_adhoc_transformations(flight_semantic_table)
    demo_percent_of_total(flight_semantic_table)
    demo_rolling_averages(con)
    demo_composability(con)
    demo_advanced_patterns(con)
    demo_filter_examples(con)
    demo_inline_measure_definitions()

    print("\n" + "=" * 80)
    print("  ✅ All examples completed successfully!")
    print("=" * 80)
    print("\nKey Takeaways:")
    print("  • Use bracket notation t['column'] in post-aggregation mutate()")
    print("  • Use t.all(t['column']) for grand totals/percent of total")
    print("  • SemanticTables are composable via joins")
    print("  • Mix semantic measures with ad-hoc aggregations freely")
    print("  • Window functions work seamlessly for advanced analytics")
    print("\nFor more examples, see:")
    print("  - examples/advanced_patterns.py")
    print("  - src/boring_semantic_layer/semantic_api/tests/test_real_world_scenarios.py")
    print("  - src/boring_semantic_layer/semantic_api/tests/malloy_benchmark/")
    print()


if __name__ == "__main__":
    main()
