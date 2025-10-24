#!/usr/bin/env python3
"""
BSL v2 Recreation of Malloy Airports Model
https://github.com/malloydata/malloy-samples/blob/main/faa/airports.malloy

Original Malloy Model:
  source: airports is duckdb.table('../data/airports.parquet') extend {
    rename: facility_type is fac_type
    measure: airport_count is count()
    view: by_state is { ... }
    view: by_facility_type is { ... }
    view: airports_by_region_dashboard is { ... }
  }

This BSL v2 implementation demonstrates:
- Creating semantic tables from Parquet data
- Defining reusable dimensions and measures
- Building analytical views equivalent to Malloy's nested views
- Dashboard-style hierarchical aggregations
"""

import pandas as pd
import ibis
from boring_semantic_layer import to_semantic_table


def create_airports_semantic_table(con=None, data_path=None):
    """
    Create the airports semantic table with all dimensions and measures.

    Args:
        con: Ibis connection (defaults to DuckDB in-memory)
        data_path: Path to airports.parquet file (if None, uses sample data)

    Returns:
        SemanticTable with airports data
    """
    if con is None:
        con = ibis.duckdb.connect(":memory:")

    # Load data
    if data_path:
        airports_tbl = con.read_parquet(data_path)
    else:
        # Create sample data matching Malloy structure
        sample_airports = pd.DataFrame({
            "code": ["JFK", "LAX", "ORD", "DFW", "ATL", "SFO", "SEA", "BOS", "MIA", "DEN"],
            "name": [
                "John F Kennedy Intl",
                "Los Angeles Intl",
                "Chicago O'Hare Intl",
                "Dallas Fort Worth Intl",
                "Hartsfield Jackson Atlanta Intl",
                "San Francisco Intl",
                "Seattle Tacoma Intl",
                "Boston Logan Intl",
                "Miami Intl",
                "Denver Intl"
            ],
            "city": ["New York", "Los Angeles", "Chicago", "Dallas", "Atlanta",
                    "San Francisco", "Seattle", "Boston", "Miami", "Denver"],
            "state": ["NY", "CA", "IL", "TX", "GA", "CA", "WA", "MA", "FL", "CO"],
            "fac_type": [
                "AIRPORT", "AIRPORT", "AIRPORT", "AIRPORT", "AIRPORT",
                "AIRPORT", "AIRPORT", "AIRPORT", "AIRPORT", "AIRPORT"
            ],
            "faa_region": [
                "AEA", "AWP", "AGL", "ASW", "ASO",
                "AWP", "ANM", "AEA", "ASO", "ANM"
            ],
            "elevation": [13, 126, 672, 607, 1026, 13, 433, 20, 8, 5431],
        })
        airports_tbl = con.create_table("airports", sample_airports)

    # Create semantic table with dimensions and measures
    # In Malloy: rename: facility_type is fac_type
    airports_st = (
        to_semantic_table(airports_tbl, name="airports")
        .with_dimensions(
            code=lambda t: t.code,
            name=lambda t: t.name,
            city=lambda t: t.city,
            state=lambda t: t.state,
            facility_type=lambda t: t.fac_type,  # Malloy rename
            faa_region=lambda t: t.faa_region,
            elevation=lambda t: t.elevation,
        )
        .with_measures(
            airport_count=lambda t: t.count()  # Malloy: measure: airport_count is count()
        )
    )

    return airports_st


# Malloy View: by_state
def view_by_state(airports_st):
    """
    Equivalent to Malloy view:
      view: by_state is {
        where: state != null
        group_by: state
        aggregate: airport_count
      }
    """
    return (
        airports_st
        .filter(lambda t: t.state.notnull())
        .group_by("state")
        .aggregate("airport_count")
        .order_by("state")
    )


# Malloy View: by_facility_type
def view_by_facility_type(airports_st):
    """
    Equivalent to Malloy view:
      view: by_facility_type is {
        group_by: facility_type
        aggregate: airport_count
      }
    """
    return (
        airports_st
        .group_by("facility_type")
        .aggregate("airport_count")
        .order_by("facility_type")
    )


# Malloy View: airports_by_region_dashboard
def view_airports_by_region_dashboard(airports_st):
    """
    Equivalent to Malloy view:
      view: airports_by_region_dashboard is {
        group_by: faa_region
        aggregate: airport_count
        nest:
          by_state
          by_facility_type
      }

    In BSL v2, we don't have true nested results like Malloy,
    but we can return the main aggregation which can be joined
    with the nested views client-side or in a dashboard.
    """
    return (
        airports_st
        .group_by("faa_region")
        .aggregate("airport_count")
        .order_by("faa_region")
    )


def demo_airports_model():
    """Run all airport views and display results."""
    print("=" * 80)
    print("BSL v2 Recreation of Malloy Airports Model")
    print("=" * 80)

    # Create semantic table
    airports_st = create_airports_semantic_table()

    # View 1: by_state
    print("\n--- View: by_state ---")
    print("Malloy: view: by_state is { where: state != null; group_by: state; aggregate: airport_count }")
    result = view_by_state(airports_st).execute()
    print(result)

    # View 2: by_facility_type
    print("\n--- View: by_facility_type ---")
    print("Malloy: view: by_facility_type is { group_by: facility_type; aggregate: airport_count }")
    result = view_by_facility_type(airports_st).execute()
    print(result)

    # View 3: airports_by_region_dashboard
    print("\n--- View: airports_by_region_dashboard ---")
    print("Malloy: view: airports_by_region_dashboard is { group_by: faa_region; aggregate: airport_count; nest: ... }")
    result = view_airports_by_region_dashboard(airports_st).execute()
    print(result)

    # Demonstrate nested drill-down (simulating Malloy's nest feature)
    print("\n--- Nested Drill-down: AWP Region Details ---")
    print("Simulating Malloy's nested views by filtering to a specific region")
    awp_by_state = (
        airports_st
        .filter(lambda t: (t.state.notnull()) & (t.faa_region == "AWP"))
        .group_by("state")
        .aggregate("airport_count")
        .execute()
    )
    print(awp_by_state)

    print("\n" + "=" * 80)
    print("Demo Complete!")
    print("=" * 80)


if __name__ == "__main__":
    demo_airports_model()
