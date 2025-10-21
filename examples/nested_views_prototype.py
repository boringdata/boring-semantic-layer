"""
Prototype: Malloy-style nested views with BSL v2 + Ibis

This demonstrates how we can implement nested views using ibis.struct() and .collect()
integrated with our semantic table API.
"""
import pandas as pd
import ibis
from boring_semantic_layer.semantic_api import to_semantic_table

# Sample data
con = ibis.duckdb.connect(":memory:")
airports_data = pd.DataFrame({
    "state": ["TX", "TX", "TX", "TX", "CA", "CA", "CA", "NY", "NY"],
    "county": ["Harris", "Harris", "Dallas", "Dallas", "LA", "LA", "San Diego", "Suffolk", "Erie"],
    "fac_type": ["HELIPORT", "AIRPORT", "HELIPORT", "AIRPORT", "HELIPORT", "AIRPORT", "AIRPORT", "HELIPORT", "AIRPORT"],
    "code": ["H1", "A1", "H2", "A2", "H3", "LAX", "SAN", "H4", "BUF"],
    "major": ["N", "Y", "N", "N", "N", "Y", "Y", "N", "Y"]
})
airports_tbl = con.create_table("airports", airports_data)

# Create semantic table
airports_st = (
    to_semantic_table(airports_tbl, "airports")
    .with_dimensions(
        state=lambda t: t.state,
        county=lambda t: t.county,
        fac_type=lambda t: t.fac_type,
        major=lambda t: t.major
    )
    .with_measures(
        airport_count=lambda t: t.count()
    )
)

print("=" * 80)
print("Pattern 1: One-level nesting (state -> county)")
print("=" * 80)
print()

# Get the underlying ibis table
base = airports_st._materialize_base_with_dims()

# Step 1: County-level aggregation
county_agg = base.group_by(["state", "county"]).aggregate(
    county_count=base.count()
)

# Step 2: Create struct and collect by state
county_struct = ibis.struct({
    "county": county_agg.county,
    "count": county_agg.county_count
})

result = county_agg.mutate(county_data=county_struct).group_by("state").aggregate(
    airport_count=county_agg.county_count.sum(),
    by_county=county_agg.mutate(county_data=county_struct).county_data.collect()
)

print(result.execute())
print()

print("=" * 80)
print("Pattern 2: Two-level nesting (state -> county -> facility)")
print("=" * 80)
print()

# Step 1: Facility-level aggregation (finest granularity)
facility_agg = base.group_by(["state", "county", "fac_type"]).aggregate(
    facility_count=base.count()
)

# Step 2: Create facility struct and collect by county
facility_struct = ibis.struct({
    "fac_type": facility_agg.fac_type,
    "count": facility_agg.facility_count
})

county_with_facilities = facility_agg.mutate(fac_data=facility_struct).group_by(["state", "county"]).aggregate(
    county_count=facility_agg.facility_count.sum(),
    by_facility=facility_agg.mutate(fac_data=facility_struct).fac_data.collect()
)

# Step 3: Create county struct with nested facilities, collect by state
county_struct = ibis.struct({
    "county": county_with_facilities.county,
    "count": county_with_facilities.county_count,
    "by_facility": county_with_facilities.by_facility
})

state_result = county_with_facilities.mutate(county_data=county_struct).group_by("state").aggregate(
    airport_count=county_with_facilities.county_count.sum(),
    by_county=county_with_facilities.mutate(county_data=county_struct).county_data.collect()
)

print(state_result.execute())
print()

print("=" * 80)
print("Pattern 3: Filtering nested views")
print("=" * 80)
print()

# Apply filter at nested level
filtered_base = base.filter(base.major == 'Y')
filtered_facility_agg = filtered_base.group_by(["state", "county", "fac_type"]).aggregate(
    facility_count=filtered_base.count()
)

# Continue as before with filtered data
facility_struct = ibis.struct({
    "fac_type": filtered_facility_agg.fac_type,
    "count": filtered_facility_agg.facility_count
})

filtered_county_with_facilities = filtered_facility_agg.mutate(fac_data=facility_struct).group_by(["state", "county"]).aggregate(
    county_count=filtered_facility_agg.facility_count.sum(),
    major_facilities=filtered_facility_agg.mutate(fac_data=facility_struct).fac_data.collect()
)

county_struct = ibis.struct({
    "county": filtered_county_with_facilities.county,
    "count": filtered_county_with_facilities.county_count,
    "major_facilities": filtered_county_with_facilities.major_facilities
})

filtered_result = filtered_county_with_facilities.mutate(county_data=county_struct).group_by("state").aggregate(
    airport_count=filtered_county_with_facilities.county_count.sum(),
    by_county=filtered_county_with_facilities.mutate(county_data=county_struct).county_data.collect()
)

print(filtered_result.execute())
print()

print("=" * 80)
print("PROTOTYPE WORKS!")
print("=" * 80)
print()
print("Next steps:")
print("  1. Add .nest() method or nest parameter to .aggregate()")
print("  2. Support Malloy-like syntax for nested view specifications")
print("  3. Handle measure resolution in nested contexts")
print("  4. Add comprehensive tests")
print()
print("This proves we can implement Malloy nested views using Ibis!")
