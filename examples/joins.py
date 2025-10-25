#!/usr/bin/env python3
"""
Joining Semantic Tables (Foreign Sums and Averages)

Example: Flights → Aircraft → Aircraft Models
- Flights table: individual flight records (many)
- Aircraft table: aircraft information (many-to-one from flights)
- Aircraft Models: model specifications like seat count (one-to-one from aircraft)
"""

import pandas as pd
import ibis
from ibis import _
from boring_semantic_layer import to_semantic_table


def main():
    print("=" * 80)
    print("  Example 4: Joining Semantic Tables")
    print("=" * 80)

    con = ibis.duckdb.connect(":memory:")

    # 1. Aircraft Models (root table - specifications for aircraft types)
    models_df = pd.DataFrame({
        "aircraft_model_code": ["B737", "A320", "B777"],
        "manufacturer": ["Boeing", "Airbus", "Boeing"],
        "seats": [150, 180, 350],
        "range_miles": [3000, 3500, 9000],
    })

    models_tbl = con.create_table("aircraft_models", models_df)

    print("\n📊 Aircraft Models Data:")
    print(models_df)

    # 2. Aircraft (individual planes)
    aircraft_df = pd.DataFrame({
        "tail_num": ["N123", "N456", "N789", "N111", "N222"],
        "aircraft_model_code": ["B737", "B737", "A320", "A320", "B777"],
        "year_manufactured": [2015, 2018, 2016, 2019, 2020],
    })

    aircraft_tbl = con.create_table("aircraft", aircraft_df)

    print("\n✈️  Aircraft Data:")
    print(aircraft_df)

    # 3. Flights (individual flight records)
    flights_df = pd.DataFrame({
        "flight_id": list(range(1, 21)),
        "tail_num": ["N123", "N456", "N123", "N789", "N111"] * 4,
        "carrier": ["AA", "AA", "UA", "UA", "DL"] * 4,
        "distance": [500, 600, 1200, 800, 5000] * 4,
    })

    flights_tbl = con.create_table("flights", flights_df)

    print("\n🛫 Flights Data (showing first 10 rows):")
    print(flights_df.head(10))

    # Create semantic tables
    print("\n" + "-" * 80)
    print("Step 1: Create Semantic Tables")
    print("-" * 80)

    models = (
        to_semantic_table(models_tbl, name="models")
        .with_dimensions(
            aircraft_model_code=lambda t: t.aircraft_model_code,
            manufacturer=lambda t: t.manufacturer,
        )
        .with_measures(
            model_count=lambda t: t.count(),
            total_seats=lambda t: t.seats.sum(),
            avg_seats=lambda t: t.seats.mean(),
        )
    )

    aircraft = (
        to_semantic_table(aircraft_tbl, name="aircraft")
        .with_dimensions(
            tail_num=lambda t: t.tail_num,
            aircraft_model_code=lambda t: t.aircraft_model_code,
        )
        .with_measures(
            aircraft_count=lambda t: t.count(),
            avg_year=lambda t: t.year_manufactured.mean(),
        )
    )

    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            tail_num=lambda t: t.tail_num,
            carrier=lambda t: t.carrier,
        )
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
        )
    )

    print("✓ Created 3 semantic tables: models, aircraft, flights")

    # Join flights → aircraft
    print("\n" + "-" * 80)
    print("Step 2: Join Flights → Aircraft")
    print("-" * 80)

    flights_with_aircraft = flights.join_one(
        aircraft,
        left_on="tail_num",
        right_on="tail_num"
    )

    print("✓ Joined flights with aircraft (many-to-one relationship)")
    print(f"  Available dimensions: {list(flights_with_aircraft.dimensions)[:5]}...")
    print(f"  Available measures: {flights_with_aircraft.measures[:5]}...")

    # Query joined table
    result = (
        flights_with_aircraft
        .group_by("flights.carrier")
        .aggregate("flights.flight_count", "aircraft.aircraft_count")
        .order_by(_["flights.flight_count"].desc())
        .execute()
    )

    print("\nQuery Result - Flights and Aircraft per Carrier:")
    print(result)

    # Join all three tables: flights → aircraft → models
    print("\n" + "-" * 80)
    print("Step 3: Three-Way Join (Flights → Aircraft → Models)")
    print("-" * 80)

    # First join aircraft with models
    aircraft_with_models = aircraft.join_one(
        models,
        left_on="aircraft_model_code",
        right_on="aircraft_model_code"
    )

    # Then join flights with the combined aircraft+models
    # Use the prefixed column name from the first join
    flights_full = flights.join_one(
        aircraft_with_models,
        left_on="tail_num",
        right_on="tail_num"
    )

    print("✓ Created three-way join: flights → aircraft → models")

    # Foreign Sums: Compute aggregations at different levels
    print("\n" + "-" * 80)
    print("Query 1: Foreign Sums - Aggregations at Each Level")
    print("-" * 80)
    print("This shows Malloy-style 'foreign sums' - aggregating at each join level")

    result = (
        flights_full
        .group_by("flights.carrier")
        .aggregate(
            "flights.flight_count",           # Count flights
            "aircraft.aircraft_count",         # Count unique aircraft
            "models.model_count",              # Count unique models
        )
        .order_by(_["flights.flight_count"].desc())
        .execute()
    )

    print(result)

    # Add measures that reference the joined table data
    print("\n" + "-" * 80)
    print("Query 2: Adding Measures with Joined Data")
    print("-" * 80)

    flights_full_with_measures = flights_full.with_measures(
        # Calculate total seats across all flights (seats × flights)
        total_seats_for_sale=lambda t: t.total_seats,
        # Average seats per model
        avg_seats_per_model=lambda t: t.avg_seats,
    )

    result = (
        flights_full_with_measures
        .group_by("flights.carrier")
        .aggregate(
            "flights.flight_count",
            "aircraft.aircraft_count",
            "total_seats_for_sale",
            "avg_seats_per_model",
        )
        .order_by(_["flights.flight_count"].desc())
        .execute()
    )

    print(result)

    # Cross-team example: Marketing + Support
    print("\n" + "-" * 80)
    print("Query 3: Cross-Team Analysis (Different Domain Example)")
    print("-" * 80)

    # Marketing team's semantic table
    customers_df = pd.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "segment": ["Premium", "Basic", "Premium", "Enterprise", "Basic"],
        "monthly_spend": [500.0, 100.0, 600.0, 2000.0, 150.0],
    })
    customers_tbl = con.create_table("customers", customers_df)

    customers = (
        to_semantic_table(customers_tbl, name="customers")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
            segment=lambda t: t.segment,
        )
        .with_measures(
            customer_count=lambda t: t.count(),
            total_revenue=lambda t: t.monthly_spend.sum(),
            avg_revenue_per_customer=lambda t: t.monthly_spend.mean(),
        )
    )

    # Support team's semantic table
    support_df = pd.DataFrame({
        "case_id": list(range(1, 11)),
        "customer_id": [1, 1, 2, 3, 3, 3, 4, 5, 5, 5],
        "priority": ["High", "Medium", "Low", "High", "High", "Medium",
                     "High", "Low", "Low", "Medium"],
    })
    support_tbl = con.create_table("support_cases", support_df)

    support = (
        to_semantic_table(support_tbl, name="support")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
            priority=lambda t: t.priority,
        )
        .with_measures(
            case_count=lambda t: t.count(),
            high_priority_cases=lambda t: (t.priority == "High").sum(),
        )
    )

    # Join for cross-team analysis
    customer_support = customers.join_one(
        support,
        left_on="customer_id",
        right_on="customer_id"
    )

    result = (
        customer_support
        .group_by("customers.segment")
        .aggregate(
            "customers.customer_count",
            "support.case_count",
            "customers.avg_revenue_per_customer",
        )
        .mutate(
            cases_per_customer=lambda t: t["support.case_count"] / t["customers.customer_count"],
            cases_per_1k_revenue=lambda t: (
                t["support.case_count"] / t["customers.avg_revenue_per_customer"] * 1000
            ),
        )
        .order_by(_.cases_per_customer.desc())
        .execute()
    )

    print("Customer Support Efficiency by Segment:")
    print(result)

    print("\n" + "=" * 80)
    print("✅ Example completed successfully!")
    print("=" * 80)
    print("\nKey Takeaways:")
    print("  • Use .join_one() for many-to-one relationships")
    print("  • Measures are automatically prefixed with table names (table__measure)")
    print("  • Aggregations work correctly at each level of the join tree")
    print("  • Enables cross-team composability and reusability")
    print("  • Similar to Malloy's 'foreign sums and averages' pattern")
    print("\nNext: Run all examples with: python examples/01_basic_flights.py")
    print()


if __name__ == "__main__":
    main()
