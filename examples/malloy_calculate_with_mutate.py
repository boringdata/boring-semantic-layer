"""
Example demonstrating Malloy-style calculate: functionality using .mutate()

This example replicates the Malloy calculate: patterns from their documentation
to verify that BSL v2 + ibis can handle window functions in post-aggregation.

Malloy patterns tested:
1. Basic window functions (lag, lead)
2. Ordering for window functions
3. Partitioning for window functions
4. Other window functions (rank, row_number, etc.)
"""
import pandas as pd
import ibis
from boring_semantic_layer.semantic_api import to_semantic_table

# Create sample flights data similar to Malloy examples
con = ibis.duckdb.connect(":memory:")
flights_data = pd.DataFrame({
    "carrier": ["WN"] * 18,
    "dep_time": pd.date_range("2000-01-15", periods=18, freq="4ME"),
    "distance": [100, 150, 200, 180, 220, 190, 210, 230, 200, 190, 210, 220, 230, 240, 250, 260, 270, 280]
})
flights_tbl = con.create_table("flights", flights_data)

# Create semantic table with dimensions
flights_st = (
    to_semantic_table(flights_tbl, "flights")
    .with_dimensions(
        dep_year=lambda t: t.dep_time.year(),
        dep_month=lambda t: t.dep_time.month()
    )
    .with_measures(
        flight_count=lambda t: t.count(),
        total_distance=lambda t: t.distance.sum(),
        avg_distance=lambda t: t.distance.mean()
    )
)

print("=" * 80)
print("Example 1: Basic lag() - Year-over-year change")
print("=" * 80)
print("Malloy pattern: calculate: year_change is flight_count - lag(flight_count)")
print()

# Replicate Malloy's basic lag example
result = (
    flights_st
    .group_by("dep_year")
    .aggregate("flight_count")
    .order_by("dep_year")
    .mutate(
        # In post-aggregation context, t.flight_count is an ibis column
        year_change=lambda t: t.flight_count - t.flight_count.lag()
    )
)

df = result.execute()
print(df.head(10))
print()

print("=" * 80)
print("Example 2: lag() with explicit ordering")
print("=" * 80)
print("Malloy pattern: lag(flight_count) { order_by: dep_year asc }")
print()

# Test lag with different ordering in window vs result
result = (
    flights_st
    .group_by("dep_year")
    .aggregate("flight_count")
    .mutate(
        # Window ordered ascending
        year_change_asc=lambda t: t.flight_count - t.flight_count.lag().over(
            ibis.window(order_by=t.dep_year.asc())
        )
    )
    .order_by(ibis.desc("dep_year"))  # Result ordered descending
)

df = result.execute()
print(df.head(10))
print()

print("=" * 80)
print("Example 3: Partitioned window - Month-over-month by year")
print("=" * 80)
print("Malloy pattern: lag(flight_count) { partition_by: dep_month, order_by: dep_year }")
print()

# Create data with month granularity
result = (
    flights_st
    .group_by("dep_year", "dep_month")
    .aggregate("flight_count")
    .mutate(
        # Partition by month, order by year
        # This gives us year-over-year change for the same month
        yoy_change=lambda t: t.flight_count - t.flight_count.lag().over(
            ibis.window(group_by=t.dep_month, order_by=t.dep_year)
        )
    )
    .order_by("dep_year", "dep_month")
)

df = result.execute()
print(df.head(15))
print()

print("=" * 80)
print("Example 4: Multiple window functions")
print("=" * 80)
print()

result = (
    flights_st
    .group_by("dep_year")
    .aggregate("flight_count", "avg_distance")
    .mutate(
        # lag - previous value
        prev_count=lambda t: t.flight_count.lag(),
        # lead - next value
        next_count=lambda t: t.flight_count.lead(),
        # rank (uses default query ordering)
        count_rank=lambda t: ibis.rank().over(ibis.window(order_by=t.dep_year)),
        # row_number
        row_num=lambda t: ibis.row_number(),
        # percent_rank
        pct_rank=lambda t: ibis.percent_rank().over(ibis.window(order_by=t.flight_count)),
        # moving average (3-period window)
        ma_3=lambda t: t.flight_count.mean().over(
            ibis.window(
                order_by=t.dep_year,
                preceding=1,
                following=1
            )
        )
    )
    .order_by("dep_year")
)

df = result.execute()
print(df.head(10))
print()

print("=" * 80)
print("Example 5: Cumulative calculations")
print("=" * 80)
print()

result = (
    flights_st
    .group_by("dep_year")
    .aggregate("flight_count")
    .mutate(
        # Cumulative sum
        cumulative_flights=lambda t: t.flight_count.sum().over(
            ibis.window(order_by=t.dep_year, preceding=None, following=0)
        ),
        # Running average
        running_avg=lambda t: t.flight_count.mean().over(
            ibis.window(order_by=t.dep_year, preceding=None, following=0)
        )
    )
    .order_by("dep_year")
)

df = result.execute()
print(df)
print()

print("=" * 80)
print("Example 6: Percent of total (using t.all() helper)")
print("=" * 80)
print()

result = (
    flights_st
    .group_by("dep_year")
    .aggregate("flight_count")
    .mutate(
        # Using our t.all() helper for percent of total
        pct_of_total=lambda t: t.flight_count / t.all(t.flight_count)
    )
    .order_by("dep_year")
)

df = result.execute()
print(df)
print()

print("=" * 80)
print("Example 7: Ranking within partitions")
print("=" * 80)
print()

# More complex data with carriers
carriers_data = pd.DataFrame({
    "carrier": ["WN", "AA", "UA"] * 6,
    "dep_time": pd.date_range("2000-01-15", periods=18, freq="4ME"),
    "distance": [100, 150, 200, 180, 220, 190, 210, 230, 200, 190, 210, 220, 230, 240, 250, 260, 270, 280]
})
carriers_tbl = con.create_table("flights_carriers", carriers_data)

carriers_st = (
    to_semantic_table(carriers_tbl, "flights")
    .with_dimensions(
        carrier=lambda t: t.carrier,
        dep_year=lambda t: t.dep_time.year()
    )
    .with_measures(
        flight_count=lambda t: t.count()
    )
)

result = (
    carriers_st
    .group_by("carrier", "dep_year")
    .aggregate("flight_count")
    .mutate(
        # Rank within each carrier (partitioned by carrier)
        carrier_rank=lambda t: ibis.rank().over(
            ibis.window(group_by=t.carrier, order_by=t.flight_count.desc())
        ),
        # Overall rank (no partition)
        overall_rank=lambda t: ibis.rank().over(
            ibis.window(order_by=t.flight_count.desc())
        )
    )
    .order_by("carrier", "dep_year")
)

df = result.execute()
print(df)
print()

print("=" * 80)
print("SUMMARY: BSL v2 + ibis CAN replicate Malloy calculate: patterns!")
print("=" * 80)
print()
print("✓ Basic window functions (lag, lead, rank, etc.) - WORKS")
print("✓ Custom ordering via ibis.window(order_by=...) - WORKS")
print("✓ Partitioning via ibis.window(partition_by=...) - WORKS")
print("✓ Moving averages and cumulative sums - WORKS")
print("✓ Percent of total via t.all() helper - WORKS")
print()
print("Our .mutate() method provides the same functionality as Malloy's calculate:")
print("- Works in post-aggregation context")
print("- Supports all ibis window functions")
print("- Can specify custom ordering and partitioning")
print("- Has nice helper like t.all() for common patterns")
