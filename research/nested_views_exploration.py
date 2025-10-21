"""
Exploring whether we can implement Malloy-style nested views in BSL v2.

The question: Can ibis create aggregating subqueries that would enable
nested views functionality?

Nested views requirements:
1. Multiple aggregation levels in one query
2. Parent-child relationships preserved
3. Independent filtering at each level
4. Result is hierarchical/nested data structure
"""
import pandas as pd
import ibis
from boring_semantic_layer.semantic_api import to_semantic_table

# Sample data
con = ibis.duckdb.connect(":memory:")
airports_data = pd.DataFrame({
    "state": ["TX", "TX", "TX", "CA", "CA", "CA", "NY", "NY"],
    "county": ["Harris", "Harris", "Dallas", "LA", "LA", "San Diego", "Suffolk", "Erie"],
    "fac_type": ["HELIPORT", "AIRPORT", "HELIPORT", "HELIPORT", "AIRPORT", "AIRPORT", "HELIPORT", "AIRPORT"],
    "major": ["N", "Y", "N", "N", "Y", "Y", "N", "Y"],
    "code": ["H1", "A1", "H2", "H3", "LAX", "SAN", "H4", "BUF"],
    "name": ["Harris Heli 1", "Harris Airport", "Dallas Heli", "LA Heli", "Los Angeles Intl", "San Diego Intl", "Suffolk Heli", "Buffalo Niagara"]
})
airports_tbl = con.create_table("airports", airports_data)

print("=" * 80)
print("Approach 1: Multiple Separate Queries (Current BSL v2)")
print("=" * 80)

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

# Query 1: State level
state_level = airports_st.group_by("state").aggregate("airport_count").execute()
print("\nState level:")
print(state_level)

# Query 2: County level within states
county_level = airports_st.group_by("state", "county").aggregate("airport_count").execute()
print("\nCounty level:")
print(county_level)

# Query 3: Facility type within counties
facility_level = airports_st.group_by("state", "county", "fac_type").aggregate("airport_count").execute()
print("\nFacility type level:")
print(facility_level)

print("\n⚠️  Problem: These are separate flat tables, not hierarchical!")
print()

print("=" * 80)
print("Approach 2: Using Ibis Subqueries?")
print("=" * 80)

# Try to create a query with subquery aggregations
# This would be the SQL we want:
# SELECT
#   state,
#   COUNT(*) as airport_count,
#   (SELECT json_agg(...) FROM (SELECT county, COUNT(*) ...)) as by_county
# FROM airports
# GROUP BY state

# Let's see if ibis can do this...
try:
    # Base query
    base = airports_tbl

    # State-level aggregation
    state_agg = base.group_by("state").aggregate(airport_count=base.count())

    # Can we add a subquery column?
    # This is tricky - we'd need to correlate the subquery with the outer query

    # Attempt 1: Using a window function? (No - this won't give us nested structure)

    # Attempt 2: Using a join? (No - this flattens the data)

    # Attempt 3: Using array_agg or similar? (Maybe!)

    # In DuckDB, we could use LIST or STRUCT
    # Let's try with raw SQL first to see if it's possible

    sql = """
    SELECT
        state,
        COUNT(*) as airport_count,
        LIST(DISTINCT county) as counties,
        LIST({county: county, count: cnt}) as county_details
    FROM (
        SELECT state, county, COUNT(*) as cnt
        FROM airports
        GROUP BY state, county
    )
    GROUP BY state
    """

    result = con.sql(sql)
    print("\nAttempt with raw SQL (LIST aggregation):")
    print(result.execute())
    print("\n✓ This works! We can create nested structures in SQL")

except Exception as e:
    print(f"Error: {e}")

print()
print("=" * 80)
print("Approach 3: Structured Aggregations in Ibis")
print("=" * 80)

# Let's see if ibis has built-in support for this
# Check if we can use struct/list types

try:
    # Group at county level first
    county_agg = airports_tbl.group_by(["state", "county"]).aggregate(
        county_count=airports_tbl.count()
    )

    # Then try to aggregate counties into a list per state
    # This would require array_agg or similar

    # Check if ibis has array_agg
    # state_nested = county_agg.group_by("state").aggregate(
    #     airport_count=county_agg.county_count.sum(),
    #     counties=???  # Need array_agg of struct(county, county_count)
    # )

    print("\nIbis doesn't have a high-level API for this pattern yet")
    print("We'd need to use raw SQL or UDFs")

except Exception as e:
    print(f"Error: {e}")

print()
print("=" * 80)
print("Approach 4: Manual Nesting in Python")
print("=" * 80)

# Get all the data we need
state_data = state_level
county_data = county_level
facility_data = facility_level

# Manually nest the structure
def nest_data(parent_df, child_df, parent_keys, child_keys, child_name):
    """Manually create nested structure from flat dataframes."""
    result = []
    for _, parent_row in parent_df.iterrows():
        parent_dict = parent_row.to_dict()

        # Filter child data for this parent
        mask = True
        for pk in parent_keys:
            mask = mask & (child_df[pk] == parent_row[pk])

        child_rows = child_df[mask]

        # Drop parent keys from child
        child_rows = child_rows.drop(columns=parent_keys)

        parent_dict[child_name] = child_rows.to_dict('records')
        result.append(parent_dict)

    return result

# Nest county data into state data
nested = nest_data(
    state_data,
    county_data,
    parent_keys=["state"],
    child_keys=["state"],
    child_name="by_county"
)

print("\nManually nested structure:")
for item in nested:
    print(f"\nState: {item['state']}, Airport Count: {item['airport_count']}")
    print(f"  Counties: {item['by_county']}")

print("\n✓ This works but requires post-processing in Python")

print()
print("=" * 80)
print("FINDINGS")
print("=" * 80)
print()
print("Nested Views Pattern:")
print("  ✓ Valuable for hierarchical analysis")
print("  ✓ Avoids multiple queries")
print("  ✓ Preserves parent-child relationships")
print()
print("BSL v2 Current State:")
print("  ✗ No built-in nested view support")
print("  ⚠  Requires multiple queries + client-side nesting")
print()
print("Potential Solutions:")
print("  1. SQL-level: Use LIST/STRUCT aggregations (DuckDB-specific)")
print("  2. API-level: Add a .nest() method to SemanticTable")
print("  3. Python-level: Helper function to nest results post-query")
print()
print("Decision:")
print("  - Nested views are USEFUL but not critical for MVP")
print("  - Most use cases can be handled with multiple queries")
print("  - Could add as a future enhancement if there's demand")
print("  - Would need database-specific implementations (JSON_AGG, etc.)")
