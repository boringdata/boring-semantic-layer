#!/usr/bin/env python3
"""
Advanced BSL v2 Patterns and Real-World Examples

This script demonstrates advanced analytical patterns using the BSL v2 semantic_api:
1. Multi-table joins with complex relationships
2. Calculated measures and measure composition
3. Time-based analysis (YoY, MoM comparisons)
4. Funnel analysis and conversion tracking
5. Advanced window functions (running totals, cumulative metrics)
6. Partitioned aggregations
"""

import pandas as pd
import ibis
from boring_semantic_layer.semantic_api import to_semantic_table


def demo_multi_table_joins():
    """
    Demonstrate complex multi-table joins across 4 related tables.
    Use case: E-commerce order fulfillment analysis
    """
    print("=" * 80)
    print("DEMO: Multi-Table Joins - Order Fulfillment Analysis")
    print("=" * 80)

    con = ibis.duckdb.connect(":memory:")

    # Create sample data
    customers = pd.DataFrame({
        "customer_id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "tier": ["gold", "silver", "gold", "bronze"],
        "signup_date": pd.to_datetime(["2023-01-01", "2023-01-15", "2023-02-01", "2023-02-10"]),
    })

    orders = pd.DataFrame({
        "order_id": [101, 102, 103, 104, 105],
        "customer_id": [1, 2, 1, 3, 4],
        "order_date": pd.to_datetime(["2023-03-01", "2023-03-05", "2023-03-10", "2023-03-15", "2023-03-20"]),
        "total_amount": [150.0, 200.0, 300.0, 100.0, 250.0],
    })

    shipments = pd.DataFrame({
        "shipment_id": [1, 2, 3, 4, 5],
        "order_id": [101, 102, 103, 104, 105],
        "shipped_date": pd.to_datetime(["2023-03-02", "2023-03-06", "2023-03-11", "2023-03-16", "2023-03-21"]),
        "carrier": ["UPS", "FedEx", "UPS", "USPS", "FedEx"],
    })

    deliveries = pd.DataFrame({
        "delivery_id": [1, 2, 3, 4, 5],
        "shipment_id": [1, 2, 3, 4, 5],
        "delivered_date": pd.to_datetime(["2023-03-05", "2023-03-09", "2023-03-15", "2023-03-20", "2023-03-26"]),
        "on_time": [True, True, True, True, False],
    })

    # Create ibis tables
    customers_tbl = con.create_table("customers", customers)
    orders_tbl = con.create_table("orders", orders)
    shipments_tbl = con.create_table("shipments", shipments)
    deliveries_tbl = con.create_table("deliveries", deliveries)

    # Build the semantic model with joins
    # Join 1: customers <- orders (one-to-many)
    orders_with_customers = orders_tbl.join(
        customers_tbl, orders_tbl.customer_id == customers_tbl.customer_id, how="inner"
    )

    # Join 2: orders <- shipments (one-to-one)
    orders_with_shipments = orders_with_customers.join(
        shipments_tbl, orders_with_customers.order_id == shipments_tbl.order_id, how="left"
    )

    # Join 3: shipments <- deliveries (one-to-one)
    full_orders = orders_with_shipments.join(
        deliveries_tbl, orders_with_shipments.shipment_id == deliveries_tbl.shipment_id, how="left"
    )

    # Create semantic table
    fulfillment_st = (
        to_semantic_table(full_orders, name="fulfillment")
        .with_dimensions(
            tier=lambda t: t.tier,
            carrier=lambda t: t.carrier,
            order_month=lambda t: t.order_date.truncate("month"),
        )
        .with_measures(
            total_revenue=lambda t: t.total_amount.sum(),
            order_count=lambda t: t.order_id.nunique(),
            on_time_deliveries=lambda t: t.on_time.sum(),
            delivery_count=lambda t: t.delivery_id.count(),
        )
    )

    # Analysis 1: On-time delivery rate by carrier
    result1 = (
        fulfillment_st
        .group_by("carrier")
        .aggregate("on_time_deliveries", "delivery_count")
        .mutate(
            on_time_rate=lambda t: t.on_time_deliveries / t.delivery_count,
        )
        .order_by(ibis.desc("on_time_rate"))
        .execute()
    )

    print("\nOn-Time Delivery Rate by Carrier:")
    print(result1)

    # Analysis 2: Revenue and orders by customer tier
    result2 = (
        fulfillment_st
        .group_by("tier")
        .aggregate("total_revenue", "order_count")
        .mutate(
            avg_order_value=lambda t: t.total_revenue / t.order_count,
            pct_of_revenue=lambda t: t.total_revenue / t.all(t.total_revenue),
        )
        .order_by(ibis.desc("total_revenue"))
        .execute()
    )

    print("\nRevenue Analysis by Customer Tier:")
    print(result2)


def demo_calculated_measures():
    """
    Demonstrate calculated measures and measure composition.
    Use case: SaaS subscription metrics with complex calculations
    """
    print("\n" + "=" * 80)
    print("DEMO: Calculated Measures - SaaS Subscription Metrics")
    print("=" * 80)

    con = ibis.duckdb.connect(":memory:")

    subscriptions = pd.DataFrame({
        "subscription_id": list(range(1, 21)),
        "plan": ["basic"] * 8 + ["pro"] * 8 + ["enterprise"] * 4,
        "mrr": [10.0] * 8 + [50.0] * 8 + [200.0] * 4,
        "status": ["active"] * 15 + ["cancelled"] * 5,
        "signup_date": pd.to_datetime([
            "2023-01-01", "2023-01-15", "2023-02-01", "2023-02-15",
            "2023-03-01", "2023-03-15", "2023-04-01", "2023-04-15",
            "2023-01-05", "2023-01-20", "2023-02-05", "2023-02-20",
            "2023-03-05", "2023-03-20", "2023-04-05", "2023-04-20",
            "2023-01-10", "2023-02-10", "2023-03-10", "2023-04-10",
        ]),
    })

    subs_tbl = con.create_table("subscriptions", subscriptions)

    # Create semantic table with base measures
    subs_st = (
        to_semantic_table(subs_tbl, name="subscriptions")
        .with_dimensions(
            plan=lambda t: t.plan,
            status=lambda t: t.status,
        )
        .with_measures(
            total_mrr=lambda t: t.mrr.sum(),
            subscriber_count=lambda t: t.subscription_id.count(),
            active_subs=lambda t: (t.status == "active").sum(),
            cancelled_subs=lambda t: (t.status == "cancelled").sum(),
        )
    )

    # Complex calculated metrics
    result = (
        subs_st
        .group_by("plan")
        .aggregate("total_mrr", "subscriber_count", "active_subs", "cancelled_subs")
        .mutate(
            # ARPU: Average Revenue Per User
            arpu=lambda t: t.total_mrr / t.subscriber_count,
            # Churn rate
            churn_rate=lambda t: t.cancelled_subs / t.subscriber_count,
            # MRR per active subscriber
            arpu_active=lambda t: t.total_mrr / t.active_subs,
            # Percent of total MRR
            mrr_contribution=lambda t: t.total_mrr / t.all(t.total_mrr),
        )
        .order_by(ibis.desc("total_mrr"))
        .execute()
    )

    print("\nSaaS Subscription Metrics by Plan:")
    print(result)


def demo_time_based_analysis():
    """
    Demonstrate time-based analysis patterns.
    Use case: Sales trends with YoY and MoM comparisons
    """
    print("\n" + "=" * 80)
    print("DEMO: Time-Based Analysis - Sales Trends")
    print("=" * 80)

    con = ibis.duckdb.connect(":memory:")

    # Create 2 years of monthly sales data
    dates = pd.date_range("2022-01-01", "2023-12-31", freq="M")
    sales = pd.DataFrame({
        "date": dates,
        "revenue": [
            100000, 105000, 110000, 115000, 120000, 125000,
            130000, 135000, 140000, 145000, 150000, 160000,  # 2022
            170000, 175000, 180000, 185000, 190000, 195000,
            200000, 205000, 210000, 215000, 220000, 230000,  # 2023
        ],
        "region": ["North"] * 6 + ["South"] * 6 + ["North"] * 6 + ["South"] * 6,
    })

    sales_tbl = con.create_table("sales", sales)

    sales_st = (
        to_semantic_table(sales_tbl, name="sales")
        .with_dimensions(
            year=lambda t: t.date.year(),
            month=lambda t: t.date.month(),
            region=lambda t: t.region,
        )
        .with_measures(
            total_revenue=lambda t: t.revenue.sum(),
        )
    )

    # Month-over-month and year-over-year analysis
    result = (
        sales_st
        .group_by("year", "month")
        .aggregate("total_revenue")
        .mutate(
            # Month-over-month change
            mom_revenue=lambda t: t.total_revenue.lag(1),
            mom_growth=lambda t: (t.total_revenue - t.total_revenue.lag(1)) / t.total_revenue.lag(1),
            # Year-over-year change
            yoy_revenue=lambda t: t.total_revenue.lag(12),
            yoy_growth=lambda t: (t.total_revenue - t.total_revenue.lag(12)) / t.total_revenue.lag(12),
        )
        .order_by("year", "month")
        .execute()
    )

    print("\nSales Trends with MoM and YoY Comparisons:")
    print(result.tail(12))  # Show last 12 months


def demo_funnel_analysis():
    """
    Demonstrate funnel analysis and conversion tracking.
    Use case: User journey through signup -> activation -> purchase
    """
    print("\n" + "=" * 80)
    print("DEMO: Funnel Analysis - User Conversion Journey")
    print("=" * 80)

    con = ibis.duckdb.connect(":memory:")

    # Funnel events
    events = pd.DataFrame({
        "user_id": [1, 1, 1, 2, 2, 3, 3, 3, 4, 5, 5, 6, 6, 6, 7, 7, 8, 9, 9, 10],
        "event": [
            "signup", "activation", "purchase",  # User 1: completed
            "signup", "activation",              # User 2: activated but didn't purchase
            "signup", "activation", "purchase",  # User 3: completed
            "signup",                            # User 4: only signed up
            "signup", "activation",              # User 5: activated but didn't purchase
            "signup", "activation", "purchase",  # User 6: completed
            "signup", "activation",              # User 7: activated but didn't purchase
            "signup",                            # User 8: only signed up
            "signup", "activation",              # User 9: activated but didn't purchase
            "signup",                            # User 10: only signed up
        ],
        "event_date": pd.to_datetime([
            "2023-01-01", "2023-01-02", "2023-01-05",
            "2023-01-03", "2023-01-04",
            "2023-01-05", "2023-01-06", "2023-01-10",
            "2023-01-07",
            "2023-01-08", "2023-01-09",
            "2023-01-10", "2023-01-11", "2023-01-15",
            "2023-01-12", "2023-01-13",
            "2023-01-14",
            "2023-01-15", "2023-01-16",
            "2023-01-17",
        ]),
        "source": [
            "organic", "organic", "organic",
            "paid", "paid",
            "organic", "organic", "organic",
            "referral",
            "paid", "paid",
            "organic", "organic", "organic",
            "paid", "paid",
            "referral",
            "organic", "organic",
            "paid",
        ],
    })

    events_tbl = con.create_table("events", events)

    funnel_st = (
        to_semantic_table(events_tbl, name="funnel")
        .with_dimensions(
            event=lambda t: t.event,
            source=lambda t: t.source,
        )
        .with_measures(
            user_count=lambda t: t.user_id.nunique(),
            event_count=lambda t: t.count(),
        )
    )

    # Overall funnel metrics
    result1 = (
        funnel_st
        .group_by("event")
        .aggregate("user_count")
        .mutate(
            percent_of_signups=lambda t: t.user_count / t.all(t.user_count).max(),  # Use max to get signup count
        )
        .order_by("event")  # Will order alphabetically: activation, purchase, signup
        .execute()
    )

    print("\nOverall Funnel Metrics:")
    print(result1)

    # Funnel by acquisition source
    result2 = (
        funnel_st
        .group_by("source", "event")
        .aggregate("user_count")
        .mutate(
            # Percent by source (partitioned aggregation)
            users_by_source=lambda t: t.user_count.sum().over(ibis.window(group_by="source")),
            conversion_rate=lambda t: t.user_count / t.user_count.sum().over(ibis.window(group_by="source")),
        )
        .order_by("source", "event")
        .execute()
    )

    print("\nFunnel Metrics by Acquisition Source:")
    print(result2)


def demo_advanced_window_functions():
    """
    Demonstrate advanced window functions.
    Use case: Running totals, cumulative metrics, and ranking
    """
    print("\n" + "=" * 80)
    print("DEMO: Advanced Window Functions - Running Totals and Rankings")
    print("=" * 80)

    con = ibis.duckdb.connect(":memory:")

    # Daily sales data
    dates = pd.date_range("2023-01-01", periods=30, freq="D")
    daily_sales = pd.DataFrame({
        "date": dates,
        "sales": [
            1000, 1100, 950, 1200, 1300, 1150, 1400,
            1500, 1350, 1600, 1700, 1550, 1800, 1900,
            1750, 2000, 2100, 1950, 2200, 2300, 2150,
            2400, 2500, 2350, 2600, 2700, 2550, 2800,
            2900, 2750,
        ],
        "category": ["A", "B"] * 15,
    })

    sales_tbl = con.create_table("daily_sales", daily_sales)

    sales_st = (
        to_semantic_table(sales_tbl, name="daily_sales")
        .with_dimensions(
            date=lambda t: t.date,
            category=lambda t: t.category,
        )
        .with_measures(
            daily_sales=lambda t: t.sales.sum(),
        )
    )

    result = (
        sales_st
        .group_by("date", "category")
        .aggregate("daily_sales")
        .mutate(
            # Running total
            cumulative_sales=lambda t: t.daily_sales.sum().over(
                ibis.window(order_by="date", preceding=None, following=0)
            ),
            # 7-day moving average
            ma_7=lambda t: t.daily_sales.mean().over(
                ibis.window(order_by="date", preceding=6, following=0)
            ),
            # Rank by sales within category
            sales_rank=lambda t: t.daily_sales.rank().over(
                ibis.window(group_by="category", order_by=ibis.desc(t.daily_sales))
            ),
            # Percent rank
            pct_rank=lambda t: t.daily_sales.percent_rank().over(
                ibis.window(group_by="category")
            ),
        )
        .order_by("date", "category")
        .execute()
    )

    print("\nDaily Sales with Running Totals and Rankings:")
    print(result.tail(10))


def demo_partitioned_aggregations():
    """
    Demonstrate partitioned aggregations (Malloy-style all() with dimensions).
    Use case: Market share analysis across multiple dimensions
    """
    print("\n" + "=" * 80)
    print("DEMO: Partitioned Aggregations - Market Share Analysis")
    print("=" * 80)

    con = ibis.duckdb.connect(":memory:")

    # Product sales data
    sales_data = pd.DataFrame({
        "product": ["Widget A", "Widget B", "Widget C", "Widget A", "Widget B", "Widget C"] * 4,
        "region": ["North"] * 6 + ["South"] * 6 + ["East"] * 6 + ["West"] * 6,
        "channel": ["Online", "Retail"] * 12,
        "sales": [
            10000, 15000, 12000, 8000, 18000, 9000,
            11000, 14000, 13000, 9000, 17000, 10000,
            12000, 16000, 11000, 10000, 19000, 11000,
            13000, 15000, 14000, 11000, 18000, 12000,
        ],
    })

    sales_tbl = con.create_table("product_sales", sales_data)

    product_st = (
        to_semantic_table(sales_tbl, name="products")
        .with_dimensions(
            product=lambda t: t.product,
            region=lambda t: t.region,
            channel=lambda t: t.channel,
        )
        .with_measures(
            total_sales=lambda t: t.sales.sum(),
        )
    )

    result = (
        product_st
        .group_by("product", "region", "channel")
        .aggregate("total_sales")
        .mutate(
            # Market share calculations at different levels
            **{
                "product_market_share": lambda t: t.total_sales.sum().over(
                    ibis.window(group_by="product")
                ) / t.all(t.total_sales),
                "region_market_share": lambda t: t.total_sales.sum().over(
                    ibis.window(group_by="region")
                ) / t.all(t.total_sales),
                "channel_market_share": lambda t: t.total_sales.sum().over(
                    ibis.window(group_by="channel")
                ) / t.all(t.total_sales),
                "product_within_region": lambda t: t.total_sales / t.total_sales.sum().over(
                    ibis.window(group_by="region")
                ),
                "channel_within_product": lambda t: t.total_sales / t.total_sales.sum().over(
                    ibis.window(group_by="product")
                ),
            }
        )
        .order_by("product", "region", "channel")
        .execute()
    )

    print("\nMulti-Dimensional Market Share Analysis:")
    print(result)


def main():
    """Run all demos."""
    demo_multi_table_joins()
    demo_calculated_measures()
    demo_time_based_analysis()
    demo_funnel_analysis()
    demo_advanced_window_functions()
    demo_partitioned_aggregations()

    print("\n" + "=" * 80)
    print("All demos completed!")
    print("=" * 80)


if __name__ == "__main__":
    main()
