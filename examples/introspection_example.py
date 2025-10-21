"""
Example demonstrating the .dims and .measures introspection properties.

These properties allow you to easily inspect what dimensions and measures
are available on a semantic table at any point in your analysis.
"""
import pandas as pd
import ibis

from boring_semantic_layer.semantic_api import to_semantic_table

# Create sample data
con = ibis.duckdb.connect(":memory:")
flights = pd.DataFrame({
    "carrier": ["AA", "AA", "UA", "DL", "DL", "DL"],
    "origin": ["JFK", "LAX", "JFK", "ATL", "ATL", "LAX"],
    "distance": [100, 200, 150, 300, 250, 350],
    "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-06"]
})
carriers = pd.DataFrame({
    "code": ["AA", "UA", "DL"],
    "name": ["American Airlines", "United Airlines", "Delta Air Lines"],
    "country": ["USA", "USA", "USA"]
})

f_tbl = con.create_table("flights", flights)
c_tbl = con.create_table("carriers", carriers)

# Start with an empty semantic table
flights_st = to_semantic_table(f_tbl, "flights")
print("Empty semantic table:")
print(f"  Dimensions: {flights_st.dims}")
print(f"  Measures: {flights_st.measures}")
print()

# Add some dimensions
flights_st = flights_st.with_dimensions(
    carrier=lambda t: t.carrier,
    origin=lambda t: t.origin,
    year=lambda t: t.date[:4],
    month=lambda t: t.date[5:7]
)
print("After adding dimensions:")
print(f"  Dimensions: {flights_st.dims}")
print(f"  Measures: {flights_st.measures}")
print()

# Add some base measures
flights_st = flights_st.with_measures(
    flight_count=lambda t: t.count(),
    total_distance=lambda t: t.distance.sum(),
    avg_distance=lambda t: t.distance.mean()
)
print("After adding base measures:")
print(f"  Dimensions: {flights_st.dims}")
print(f"  Measures: {flights_st.measures}")
print()

# Add calculated measures
flights_st = flights_st.with_measures(
    pct_of_total=lambda t: t.flight_count / t.all(t.flight_count),
    distance_per_flight=lambda t: t.total_distance / t.flight_count
)
print("After adding calculated measures:")
print(f"  Dimensions: {flights_st.dims}")
print(f"  Measures: {flights_st.measures}")
print()

# Create carriers semantic table
carriers_st = (
    to_semantic_table(c_tbl, "carriers")
    .with_dimensions(
        code=lambda t: t.code,
        name=lambda t: t.name,
        country=lambda t: t.country
    )
    .with_measures(
        carrier_count=lambda t: t.count()
    )
)
print("Carriers semantic table:")
print(f"  Dimensions: {carriers_st.dims}")
print(f"  Measures: {carriers_st.measures}")
print()

# Join the tables
joined = flights_st.join(carriers_st, on=lambda f, c: f.carrier == c.code)
print("After joining (note the prefixing):")
print(f"  Dimensions: {joined.dims}")
print(f"  Measures: {joined.measures}")
print()

# After aggregation, measures become columns
aggregated = (
    flights_st
    .group_by("carrier", "origin")
    .aggregate("flight_count", "total_distance")
)
print("After aggregation (measures are materialized):")
print(f"  Dimensions: {aggregated.dims}")
print(f"  Measures: {aggregated.measures}")
print()

print("=" * 60)
print("Use cases for introspection:")
print("=" * 60)
print()
print("1. Documentation - quickly see what's available")
print("2. Debugging - verify dimensions/measures are defined correctly")
print("3. Dynamic queries - programmatically build queries based on available measures")
print("4. IDE autocomplete - can be used for better tooling support")
print("5. Data exploration - understand the semantic model structure")
