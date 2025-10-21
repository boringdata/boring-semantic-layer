"""
Example demonstrating different ways to reference measures in lambdas.

This example shows that you can now reference measures using:
1. Attribute access (existing): t.flight_count
2. Bracket notation (existing): t["flight_count"]
3. String names in t.all() (NEW): t.all("flight_count")
"""
import pandas as pd
import ibis

from boring_semantic_layer.semantic_api import to_semantic_table

# Create sample data
con = ibis.duckdb.connect(":memory:")
flights = pd.DataFrame({
    "carrier": ["AA", "AA", "UA", "DL", "DL", "DL"],
    "distance": [100, 200, 150, 300, 250, 350]
})
f_tbl = con.create_table("flights", flights)

# Create semantic table with base measures
flights_st = to_semantic_table(f_tbl, "flights").with_measures(
    flight_count=lambda t: t.count(),
    total_distance=lambda t: t.distance.sum(),
)

# Now you can reference measures in multiple ways:

# Method 1: Attribute access (existing behavior)
flights_st = flights_st.with_measures(
    pct_by_attr=lambda t: t.flight_count / t.all(t.flight_count)
)

# Method 2: Bracket notation (existing behavior)
flights_st = flights_st.with_measures(
    pct_by_bracket=lambda t: t["flight_count"] / t.all(t["flight_count"])
)

# Method 3: String names in t.all() (NEW!)
flights_st = flights_st.with_measures(
    pct_by_string=lambda t: t.flight_count / t.all("flight_count")
)

# Method 4: Mixed styles (NEW!)
flights_st = flights_st.with_measures(
    avg_distance_pct=lambda t: (t["total_distance"] / t.flight_count) / t.all("total_distance")
)

# Query the results
result = (
    flights_st
    .group_by("carrier")
    .aggregate("pct_by_attr", "pct_by_bracket", "pct_by_string", "avg_distance_pct")
    .order_by("carrier")
)

print("Results:")
print(result.execute())
print("\nAll three pct methods give the same results!")
print("The new string-based reference style makes code more readable in some contexts.")
