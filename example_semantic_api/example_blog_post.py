"""
BSL v2 Blog Post Examples - Testing the queries from the blog post
"""

import ibis
from boring_semantic_layer.semantic_api.api import to_semantic_table

con = ibis.duckdb.connect(":memory:")
BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"
flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")

# Convert to semantic table
flight_semantic_table = to_semantic_table(flights_tbl)

# Add dimensions and measures
flight_semantic_table = (
    flight_semantic_table
    .with_dimensions(
        origin=lambda t: t.origin,
        carrier=lambda t: t.carrier,
        month=lambda t: t.arr_time.truncate('month')
    )
    .with_measures(
        flight_count=lambda t: t.count(),
        avg_distance=lambda t: t.distance.mean()
    )
)

print("=== Basic semantic table query ===")
result1 = (
    flight_semantic_table
    .group_by("origin")
    .aggregate("flight_count")
    .execute()
)
print(result1.head())

print("\n=== Mixed semantic + ad-hoc query ===")
# Note: Blog post shows mixed syntax but implementation requires all kwargs
result2 = (
    flight_semantic_table
    .group_by("origin") 
    .aggregate(
        flight_count=lambda t: t.count(),  # semantic measure equivalent  
        mean_distance=lambda x: x.distance.mean()  # ad-hoc measure
    )
    .execute()
)
print(result2.head())

print("\n=== Percentage of total (market share) ===")
# Note: Blog shows flights_st.flight_count but needs lambda wrapper
result3 = (
    flight_semantic_table
    .group_by('carrier')
    .aggregate(flight_count=lambda t: t.count())
    .mutate(
        market_share=lambda t: t.flight_count / t.flight_count.sum()
    )
    .execute()
)
print(result3.head())

print("\n=== Rolling averages ===")
rolling_window = ibis.window(order_by='month', rows=(0, 2))

# Note: Blog shows flights_st references but needs lambda wrappers  
result4 = (
    flight_semantic_table
    .group_by(month=lambda t: t.arr_time.truncate('month'))
    .aggregate(monthly_flights=lambda t: t.count())
    .mutate(
        rolling_avg=lambda t: t.monthly_flights.mean().over(rolling_window)
    )
    .execute()
)
print(result4.head())

print("\n=== Using semantic measures directly ===")
result5 = (
    flight_semantic_table
    .group_by("origin")
    .aggregate(lambda t: t.flight_count)
    .execute()
)
print(result5.head())

print("\n=== Composability: Creating derived tables ===")
# Create a marketing-focused aggregated table
marketing_agg = (
    flight_semantic_table
    .group_by("carrier")
    .aggregate(
        total_flights=lambda t: t.count(),
        avg_distance=lambda t: t.distance.mean()
    )
    .execute()
)

# Create operations-focused aggregated table  
operations_agg = (
    flight_semantic_table
    .group_by("origin")
    .aggregate(
        flight_volume=lambda t: t.count(),
        avg_delay=lambda t: t.dep_delay.mean()
    )
    .execute()
)

print("Marketing table sample:")
print(marketing_agg.head())

print("\nOperations table sample:")  
print(operations_agg.head())

print("\n=== Composing queries from semantic table ===")
# Example of building on top of the semantic table
complex_analysis = (
    flight_semantic_table
    .group_by("carrier", "origin")
    .aggregate(
        flights=lambda t: t.count(),
        avg_distance=lambda t: t.distance.mean()
    )
    .mutate(
        efficiency_score=lambda t: t.flights / t.avg_distance
    )
    .filter(lambda t: t.flights > 100)  # Only major routes
    .order_by(ibis.desc("efficiency_score"))
    .limit(10)
    .execute()
)

print("Top 10 most efficient carrier-route combinations:")
print(complex_analysis)

print("\n=== Cross-team semantic table joins ===")
# Simulate marketing team's users table
users_data = {
    'customer_id': [1, 2, 3, 4, 5],
    'segment': ['Premium', 'Standard', 'Premium', 'Basic', 'Standard'],
    'signup_date': ['2023-01-15', '2023-02-20', '2023-01-10', '2023-03-05', '2023-02-28'],
    'monthly_spend': [500, 200, 600, 100, 250]
}
users_table = ibis.memtable(users_data)

marketing_st = (
    to_semantic_table(users_table)
    .with_dimensions(
        customer_id=lambda t: t.customer_id,
        segment=lambda t: t.segment,
        signup_date=lambda t: t.signup_date
    )
    .with_measures(
        user_count=lambda t: t.customer_id.count(),
        total_revenue=lambda t: t.monthly_spend.sum(),
        avg_revenue_per_user=lambda t: t.monthly_spend.mean()
    )
)

# Simulate support team's support cases table
support_data = {
    'case_id': [101, 102, 103, 104, 105],
    'customer_id': [1, 2, 1, 3, 4],
    'created_date': ['2023-03-01', '2023-03-02', '2023-03-03', '2023-03-04', '2023-03-05'],
    'priority': ['High', 'Medium', 'High', 'Low', 'Medium']
}
support_cases_table = ibis.memtable(support_data)

support_st = (
    to_semantic_table(support_cases_table)
    .with_dimensions(
        case_id=lambda t: t.case_id,
        customer_id=lambda t: t.customer_id,
        created_date=lambda t: t.created_date,
        priority=lambda t: t.priority
    )
    .with_measures(
        case_count=lambda t: t.case_id.count(),
        high_priority_cases=lambda t: (t.priority == 'High').sum()
    )
)

# Join the semantic tables
cross_team_analysis = (
    marketing_st
    .join(support_st, on=lambda u, s: u.customer_id == s.customer_id)
    .group_by("segment", "priority")
    .aggregate(
        cases=lambda t: t.case_count,
        revenue=lambda t: t.total_revenue
    )
    .execute()
)

print("Cross-team analysis (support cases by customer segment and priority):")
print(cross_team_analysis)

# Additional cross-team analysis as mentioned in blog post
cross_team_st = (
    to_semantic_table(
        marketing_st.join(support_st, on=lambda u, s: u.customer_id == s.customer_id),
        name="cross_team_st"
    )
    .with_measures(
        avg_case_value=lambda t: t.monthly_spend.mean() / t.case_id.count()  # cases per revenue value
    )
)

# Note: Blog shows "users__segment" but join doesn't create prefixed columns
efficiency_metrics = (
    cross_team_st
    .group_by("segment")  # dimension coming from marketing
    .aggregate(
        avg_case_value=lambda t: t.avg_case_value  # measure coming from Support (now defined)
    )
    .execute()
)

print("\nEfficiency metrics (cases per customer segment):")
print(efficiency_metrics)

print("\n=== Filters example ===")
# Filter before aggregation
jfk_flights = (
    flight_semantic_table
    .filter(lambda t: t.origin == 'JFK')
    .group_by("carrier")
    .aggregate(flight_count=lambda t: t.count())
    .execute()
)

print("JFK flights by carrier:")
print(jfk_flights.head())

# Filter after aggregation
high_volume_carriers = (
    flight_semantic_table
    .group_by("carrier")
    .aggregate(total_flights=lambda t: t.count())
    .filter(lambda t: t.total_flights > 10000)
    .execute()
)

print("\nHigh-volume carriers (>10,000 flights):")
print(high_volume_carriers)

print("\nAll BSL v2 queries executed successfully!")