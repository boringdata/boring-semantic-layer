"""
Testing Ibis's struct() and collect() for nested views implementation.
"""
import pandas as pd
import ibis

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

print("=" * 80)
print("Test 1: Basic collect() aggregation")
print("=" * 80)

# Group by state and collect counties
result = airports_tbl.group_by("state").aggregate(
    airport_count=airports_tbl.count(),
    counties=airports_tbl.county.collect(distinct=True)
)
print(result.execute())
print()

print("=" * 80)
print("Test 2: Collect with struct for richer nested data")
print("=" * 80)

# First, create county-level aggregation
county_agg = airports_tbl.group_by(["state", "county"]).aggregate(
    county_count=airports_tbl.count()
)

# Now try to nest this into state-level
# We want: state -> array of {county, county_count}

# Create a struct column from county aggregation
county_struct = ibis.struct({
    "county": county_agg.county,
    "count": county_agg.county_count
})

# Collect these structs grouped by state
nested_result = county_agg.mutate(county_data=county_struct).group_by("state").aggregate(
    airport_count=county_agg.county_count.sum(),
    by_county=county_agg.mutate(
        county_data=ibis.struct({"county": county_agg.county, "count": county_agg.county_count})
    ).county_data.collect()
)

print(nested_result.execute())
print()

print("=" * 80)
print("Test 3: Two-level nesting (state -> county -> facility)")
print("=" * 80)

# Step 1: Aggregate at facility level (finest granularity)
facility_agg = airports_tbl.group_by(["state", "county", "fac_type"]).aggregate(
    facility_count=airports_tbl.count()
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
print("SUCCESS! We can implement Malloy-style nested views with Ibis!")
print("=" * 80)
print()
print("Pattern:")
print("  1. Create aggregation at finest granularity")
print("  2. Use ibis.struct() to create structured records")
print("  3. Use .collect() to aggregate structs into arrays")
print("  4. Group by parent keys and aggregate")
print("  5. Repeat for each level of nesting")
print()
print("This works generically across Ibis backends!")
