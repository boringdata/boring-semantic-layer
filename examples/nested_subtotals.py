#!/usr/bin/env python3
"""
Example 6: Nested Subtotals (Drill-Down Analysis)

This example demonstrates how to create hierarchical drill-down analyses,
similar to Malloy's nested subtotals pattern. This is commonly requested
for analyzing revenue by time periods (year → quarter → month → day) or
segments (region → state → city).

Pattern:
1. Start with high-level aggregation (e.g., by year)
2. Add intermediate levels (e.g., by quarter within year)
3. Add detail levels (e.g., top days within each quarter)
4. Display results in a hierarchical format

This is equivalent to Malloy's "nest:" clause but achieved through
multiple aggregation levels and pandas grouping.
"""

import pandas as pd
import ibis
from datetime import datetime, timedelta
from boring_semantic_layer.semantic_api import to_semantic_table


def main():
    print("=" * 80)
    print("  Example 6: Nested Subtotals (Drill-Down Analysis)")
    print("=" * 80)

    con = ibis.duckdb.connect(":memory:")

    # Create order items data with dates spanning multiple years
    np_random = __import__('numpy').random
    np_random.seed(42)

    # Generate dates across 4 years (2019-2022)
    base_date = datetime(2019, 1, 1)
    n_orders = 2000

    dates = [base_date + timedelta(days=int(x)) for x in np_random.uniform(0, 1460, n_orders)]

    orders_df = pd.DataFrame({
        "id": list(range(1, n_orders + 1)),
        "created_at": dates,
        "sale_price": np_random.uniform(10, 500, n_orders),
        "product_category": np_random.choice(["Electronics", "Clothing", "Home", "Sports"], n_orders),
    })

    # Add computed date columns
    orders_df["year"] = orders_df["created_at"].dt.year
    orders_df["quarter"] = orders_df["created_at"].dt.to_period("Q").astype(str)
    orders_df["month"] = orders_df["created_at"].dt.to_period("M").astype(str)
    orders_df["day"] = orders_df["created_at"].dt.date

    orders_tbl = con.create_table("order_items", orders_df)

    print("\n📊 Sample Order Items Data:")
    print(orders_df.head(10))
    print(f"\nTotal orders: {len(orders_df)}")
    print(f"Date range: {orders_df['created_at'].min().date()} to {orders_df['created_at'].max().date()}")
    print(f"Total sales: ${orders_df['sale_price'].sum():,.2f}")

    # Create semantic table
    order_items = (
        to_semantic_table(orders_tbl, name="order_items")
        .with_dimensions(
            year=lambda t: t.year,
            quarter=lambda t: t.quarter,
            month=lambda t: t.month,
            day=lambda t: t.day,
            product_category=lambda t: t.product_category,
        )
        .with_measures(
            total_sales=lambda t: t.sale_price.sum(),
            order_count=lambda t: t.count(),
            avg_sale_price=lambda t: t.sale_price.mean(),
        )
    )

    # Level 1: Annual Sales
    print("\n" + "-" * 80)
    print("Level 1: Annual Sales Summary")
    print("-" * 80)

    annual_sales = (
        order_items
        .group_by("year")
        .aggregate("total_sales", "order_count")
        .order_by(lambda t: ibis.desc(t.year))
        .execute()
    )

    print("\nAnnual sales:")
    print(annual_sales.to_string(index=False))

    # Level 2: Drill down into Quarters
    print("\n" + "-" * 80)
    print("Level 2: Nested - Annual Sales with Quarterly Breakdown")
    print("-" * 80)

    quarterly_sales = (
        order_items
        .group_by("year", "quarter")
        .aggregate("total_sales", "order_count")
        .order_by(lambda t: ibis.desc(t.year), "quarter")
        .execute()
    )

    print("\nDrill-down by quarter (nested within year):")
    for year in sorted(quarterly_sales["year"].unique(), reverse=True):
        year_data = quarterly_sales[quarterly_sales["year"] == year]
        year_total = year_data["total_sales"].sum()
        print(f"\n{year} - Total Sales: ${year_total:,.2f}")
        print("  Quarterly breakdown:")
        for _, row in year_data.iterrows():
            pct = (row["total_sales"] / year_total * 100)
            print(f"    {row['quarter']}: ${row['total_sales']:>12,.2f} ({pct:>5.1f}%)")

    # Level 3: Drill down even further - Top 5 days per quarter
    print("\n" + "-" * 80)
    print("Level 3: Deep Nested - Top 5 Sales Days per Quarter")
    print("-" * 80)

    # Get top 5 days per quarter using window functions
    daily_sales = (
        order_items
        .group_by("year", "quarter", "day")
        .aggregate("total_sales")
        .mutate(
            rank_in_quarter=lambda t: ibis.rank().over(
                ibis.window(
                    group_by=["year", "quarter"],
                    order_by=ibis.desc(t["total_sales"])
                )
            )
        )
        .to_ibis()
        .filter(lambda t: t.rank_in_quarter <= 5)
        .order_by("year", "quarter", "rank_in_quarter")
        .execute()
    )

    print("\nTop 5 sales days per quarter (showing 2022 and 2021):")
    for year in [2022, 2021]:
        year_quarters = quarterly_sales[quarterly_sales["year"] == year]
        print(f"\n{year}:")
        for _, q_row in year_quarters.iterrows():
            quarter = q_row["quarter"]
            q_total = q_row["total_sales"]
            print(f"  {quarter} - Total: ${q_total:,.2f}")
            print("    Top 5 days:")

            top_days = daily_sales[
                (daily_sales["year"] == year) &
                (daily_sales["quarter"] == quarter)
            ]

            for _, day_row in top_days.iterrows():
                day_pct = (day_row["total_sales"] / q_total * 100)
                print(f"      {day_row['day']}: ${day_row['total_sales']:>10,.2f} ({day_pct:>4.1f}%)")

    # Example 4: Nested by Product Category
    print("\n" + "-" * 80)
    print("Level 4: Nested by Product Category within Years")
    print("-" * 80)

    category_sales = (
        order_items
        .group_by("year", "product_category")
        .aggregate("total_sales", "order_count")
        .order_by(lambda t: ibis.desc(t.year), lambda t: ibis.desc(t.total_sales))
        .execute()
    )

    print("\nProduct category performance by year:")
    for year in sorted(category_sales["year"].unique(), reverse=True):
        year_data = category_sales[category_sales["year"] == year]
        year_total = year_data["total_sales"].sum()
        print(f"\n{year} - Total Sales: ${year_total:,.2f}")
        print("  Category breakdown:")
        for _, row in year_data.iterrows():
            pct = (row["total_sales"] / year_total * 100)
            print(f"    {row['product_category']:<15}: ${row['total_sales']:>12,.2f} ({pct:>5.1f}%) - {row['order_count']} orders")

    # Example 5: Multi-level with filtering
    print("\n" + "-" * 80)
    print("Level 5: Nested with Filtering - Electronics Category Only")
    print("-" * 80)

    # ✅ GOOD: Use .filter() directly - preserves semantic layer measures and dimensions!
    electronics = order_items.filter(lambda t: t.product_category == "Electronics")

    # Aggregate using semantic layer measures - no need to recreate!
    electronics_by_month = (
        electronics
        .group_by("year", "month")  # Dimensions from semantic layer!
        .aggregate("total_sales", "order_count")  # Measures from semantic layer!
        .order_by("year", "month")
        .execute()
    )

    print("\nElectronics sales by month (2022 only):")
    elec_2022 = electronics_by_month[electronics_by_month["year"] == 2022]
    for _, row in elec_2022.iterrows():
        # Month is already a string like '2022-01', parse it to get the month name
        month_str = str(row['month'])
        month_date = pd.to_datetime(month_str)
        month_name = month_date.strftime("%B")
        print(f"  {month_name:>10}: ${row['total_sales']:>10,.2f} ({int(row['order_count'])} orders)")

    print("\n" + "=" * 80)
    print("✅ Example completed successfully!")
    print("=" * 80)
    print("\nKey Takeaways:")
    print("  • Nested subtotals enable hierarchical drill-down analysis")
    print("  • Start broad (year) and drill down (quarter → month → day)")
    print("  • Use multiple group_by levels to create nested views")
    print("  • Window functions with group_by for top N within groups")
    print("  • Display results hierarchically using pandas grouping")
    print("  • Combine with filtering for segment-specific analysis")
    print("  • Essential for executive dashboards and drill-down reports")
    print("\nNext: See examples/README.md for all patterns")
    print()


if __name__ == "__main__":
    main()
