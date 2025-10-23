#!/usr/bin/env python3
"""
BSL v2 Implementation of Malloy's Dimensional Index Feature

This demonstrates the dimensional index operator inspired by Malloy:
https://malloydata.github.io/documentation/language/indexes

A Dimensional Search Index is a table with 5 columns:
- fieldName: The simple field name (e.g., "state")
- fieldPath: The full path (e.g., "airports__state" for joins)
- fieldType: The type of the field ("string", "number", "date")
- fieldValue: The dimensional value or range
- weight: The importance/cardinality of the value

Use cases:
- Building autocomplete/filtering UIs
- LLM-based data exploration
- Understanding dataset structure
"""

import pandas as pd
import ibis
import ibis.selectors as s
from boring_semantic_layer.api import to_semantic_table


def create_sample_airports():
    """Create a sample airports dataset."""
    con = ibis.duckdb.connect(":memory:")

    sample_airports = pd.DataFrame({
        "code": ["JFK", "LAX", "ORD", "DFW", "ATL", "SFO", "SEA", "BOS", "MIA", "DEN",
                "PHX", "LAS", "MSP", "DTW", "PHL", "LGA", "BWI", "IAD", "SLC", "MCO"],
        "city": ["New York", "Los Angeles", "Chicago", "Dallas", "Atlanta",
                "San Francisco", "Seattle", "Boston", "Miami", "Denver",
                "Phoenix", "Las Vegas", "Minneapolis", "Detroit", "Philadelphia",
                "New York", "Baltimore", "Washington", "Salt Lake City", "Orlando"],
        "state": ["NY", "CA", "IL", "TX", "GA", "CA", "WA", "MA", "FL", "CO",
                 "AZ", "NV", "MN", "MI", "PA", "NY", "MD", "VA", "UT", "FL"],
        "fac_type": ["AIRPORT"] * 20,
        "elevation": [13, 126, 672, 607, 1026, 13, 433, 20, 8, 5431,
                     1135, 2181, 841, 645, 36, 22, 146, 313, 4227, 96],
        "major": [True, True, True, True, True, True, True, True, True, True,
                 True, True, True, True, True, False, True, True, True, True],
    })

    airports_tbl = con.create_table("airports", sample_airports)

    # Create semantic table
    airports_st = (
        to_semantic_table(airports_tbl, name="airports")
        .with_dimensions(
            code=lambda t: t.code,
            city=lambda t: t.city,
            state=lambda t: t.state,
            fac_type=lambda t: t.fac_type,
            elevation=lambda t: t.elevation,
        )
        .with_measures(
            airport_count=lambda t: t.count(),
            avg_elevation=lambda t: t.elevation.mean(),
        )
    )

    return airports_st


def demo_simple_index():
    """Demonstrate simple indexing - equivalent to Malloy: index: *"""
    print("=" * 80)
    print("1. Simple Index - All Fields")
    print("=" * 80)
    print("\nMalloy equivalent:")
    print("  run: airports -> { index: * }\n")

    airports = create_sample_airports()
    result = airports.index(s.all()).execute()
    print(result)
    print()


def demo_index_with_ordering():
    """Demonstrate indexing with ordering by weight."""
    print("=" * 80)
    print("2. Index with Ordering - Most Common Values")
    print("=" * 80)
    print("\nMalloy equivalent:")
    print("  run: airports -> { index: * } -> {")
    print("    where: fieldType = 'string'")
    print("    select: *")
    print("    order_by: weight desc")
    print("  }\n")

    airports = create_sample_airports()
    result = (
        airports
        .index(s.all())
        .filter(lambda t: t.fieldType == "string")
        .order_by(lambda t: t.weight.desc())
        .execute()
    )
    print(result)
    print()


def demo_index_search():
    """Demonstrate using index for search/filtering."""
    print("=" * 80)
    print("3. Index Search - Find Fields Containing 'New'")
    print("=" * 80)
    print("\nMalloy equivalent:")
    print("  run: airports -> { index: * } -> {")
    print("    where: fieldValue ~ r'New%'")
    print("    order_by: weight desc")
    print("  }\n")

    airports = create_sample_airports()
    result = (
        airports
        .index(s.all())
        .filter(lambda t: t.fieldValue.like("New%"))
        .order_by(lambda t: t.weight.desc())
        .execute()
    )
    print(result)
    print()


def demo_index_with_custom_weight():
    """Demonstrate indexing with custom weight measure."""
    print("=" * 80)
    print("4. Index with Custom Weight - By Average Elevation")
    print("=" * 80)
    print("\nMalloy equivalent:")
    print("  run: airports -> {")
    print("    index: *")
    print("    by: avg_elevation")
    print("  }\n")

    airports = create_sample_airports()
    result = (
        airports
        .index(s.all(), by="avg_elevation")
        .filter(lambda t: t.fieldType == "string")
        .order_by(lambda t: t.weight.desc())
        .limit(10)
        .execute()
    )
    print(result)
    print()


def demo_index_sampling():
    """Demonstrate indexing with sampling."""
    print("=" * 80)
    print("5. Index with Sampling - Sample 10 Rows")
    print("=" * 80)
    print("\nMalloy equivalent:")
    print("  run: airports -> {")
    print("    index: *")
    print("    sample: 10")
    print("  }\n")

    airports = create_sample_airports()
    result = (
        airports
        .index(s.all(), sample=10)
        .filter(lambda t: t.fieldType == "string")
        .order_by(lambda t: t.weight.desc())
        .execute()
    )
    print(result)
    print()


def demo_index_specific_fields():
    """Demonstrate indexing specific fields."""
    print("=" * 80)
    print("6. Index Specific Fields - Only state and city")
    print("=" * 80)
    print("\nMalloy equivalent:")
    print("  run: airports -> { index: state, city }\n")

    airports = create_sample_airports()
    result = (
        airports
        .index(s.cols("state", "city"))
        .order_by(lambda t: t.weight.desc())
        .execute()
    )
    print(result)
    print()


def demo_top_values_per_dimension():
    """Demonstrate getting top values for each dimension (like Malloy's nested example)."""
    print("=" * 80)
    print("7. Top Values Per Dimension")
    print("=" * 80)
    print("\nMalloy equivalent:")
    print("  run: airports -> { index: * } -> {")
    print("    group_by: fieldName")
    print("    nest: values is {")
    print("      group_by: fieldValue, weight")
    print("      order_by: weight desc")
    print("      limit: 5")
    print("    }")
    print("  }\n")

    airports = create_sample_airports()

    # Since bslv2 doesn't have nested views yet, we'll show it per field
    index_result = airports.index(s.all()).execute()

    print("\nTop 5 values by field:")
    for field_name in index_result["fieldName"].unique():
        field_data = index_result[index_result["fieldName"] == field_name]
        field_data_sorted = field_data.sort_values("weight", ascending=False).head(5)
        print(f"\n{field_name}:")
        print(field_data_sorted[["fieldValue", "weight"]].to_string(index=False))
    print()


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("BSL v2 - Dimensional Index Feature Demo")
    print("Inspired by Malloy's index operator")
    print("=" * 80 + "\n")

    demo_simple_index()
    demo_index_with_ordering()
    demo_index_search()
    demo_index_with_custom_weight()
    demo_index_sampling()
    demo_index_specific_fields()
    demo_top_values_per_dimension()

    print("=" * 80)
    print("Demo Complete!")
    print("=" * 80)
