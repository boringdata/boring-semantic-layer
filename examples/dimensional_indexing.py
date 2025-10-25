#!/usr/bin/env python3
"""Dimensional Indexing - Building Search Indexes for Data Discovery.

https://docs.malloydata.dev/documentation/patterns/dim_index
"""

import ibis
import ibis.selectors as s
from boring_semantic_layer import to_semantic_table

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def main():
    con = ibis.duckdb.connect(":memory:")

    airports_tbl = con.read_parquet(f"{BASE_URL}/airports.parquet")

    airports = (
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

    print("\n" + "=" * 80)
    print("1. Simple Index - All Dimensions")
    print("=" * 80)
    print("\nMalloy equivalent: run: airports -> { index: * }\n")

    index_all = airports.index(s.all()).execute()
    print(index_all.head(20))
    print(f"\nTotal index entries: {len(index_all)}")

    print("\n" + "=" * 80)
    print("2. Most Common String Values")
    print("=" * 80)
    print("\nMalloy equivalent:")
    print("  run: airports -> { index: * } -> {")
    print("    where: fieldType = 'string'")
    print("    order_by: weight desc")
    print("  }\n")

    top_string_values = (
        airports.index(s.all())
        .filter(lambda t: t.fieldType == "string")
        .order_by(lambda t: t.weight.desc())
        .limit(15)
        .execute()
    )
    print(top_string_values)

    print("\n" + "=" * 80)
    print("3. Search Index - Find Values Containing 'SAN'")
    print("=" * 80)
    print("\nMalloy equivalent:")
    print("  run: airports -> { index: * } -> {")
    print("    where: fieldValue ~ r'SAN%'")
    print("  }\n")

    search_results = (
        airports.index(s.all())
        .filter(lambda t: t.fieldValue.like("%SAN%"))
        .order_by(lambda t: t.weight.desc())
        .limit(10)
        .execute()
    )
    print(search_results)

    print("\n" + "=" * 80)
    print("4. Numeric Fields - Understanding Value Ranges")
    print("=" * 80)
    print("\nMalloy equivalent:")
    print("  run: airports -> { index: * } -> {")
    print("    where: fieldType = 'number'")
    print("  }\n")

    numeric_index = (
        airports.index(s.all())
        .filter(lambda t: t.fieldType == "number")
        .order_by(lambda t: t.weight.desc())
        .execute()
    )
    print(numeric_index)

    print("\n" + "=" * 80)
    print("5. Index Specific Fields - State and City Only")
    print("=" * 80)
    print("\nMalloy equivalent: run: airports -> { index: state, city }\n")

    specific_fields = (
        airports.index(s.cols("state", "city"))
        .order_by(lambda t: t.weight.desc())
        .limit(15)
        .execute()
    )
    print(specific_fields)

    print("\n" + "=" * 80)
    print("6. Sampled Index - Sample 100 Rows")
    print("=" * 80)
    print("\nMalloy equivalent:")
    print("  run: airports -> { index: *, sample: 100 }\n")

    sampled_index = (
        airports.index(s.all(), sample=100)
        .filter(lambda t: t.fieldType == "string")
        .order_by(lambda t: t.weight.desc())
        .limit(10)
        .execute()
    )
    print(sampled_index)
    print("\nNote: Sampling is useful for very large datasets to get quick insights")

    print("\n" + "=" * 80)
    print("7. Autocomplete Use Case - City Search")
    print("=" * 80)
    print("\nBuilding autocomplete suggestions for city names\n")

    def get_autocomplete_suggestions(prefix: str, limit: int = 10):
        """Get autocomplete suggestions for a given prefix."""
        return (
            airports.index(s.cols("city"))
            .filter(lambda t: t.fieldValue.like(f"{prefix.upper()}%"))
            .order_by(lambda t: t.weight.desc())
            .limit(limit)
            .execute()
        )

    for search_term in ["SAN", "NEW", "LOS"]:
        suggestions = get_autocomplete_suggestions(search_term, 5)
        print(f"\nSearch: '{search_term}'")
        if len(suggestions) > 0:
            for _, row in suggestions.iterrows():
                print(f"  - {row['fieldValue']} (weight: {row['weight']})")
        else:
            print("  No suggestions found")

    print("\n" + "=" * 80)
    print("8. Data Distribution - Top Values per Field")
    print("=" * 80)
    print("\nAnalyzing the cardinality and top values of each dimension\n")

    index_result = airports.index(s.all()).execute()

    for field_name in index_result["fieldName"].unique():
        field_data = index_result[index_result["fieldName"] == field_name]
        total_values = len(field_data)
        field_type = field_data["fieldType"].iloc[0]
        top_5 = field_data.nlargest(5, "weight")

        print(f"\n{field_name} ({field_type})")
        print(f"  Unique values: {total_values}")
        print("  Top 5 values:")
        for _, row in top_5.iterrows():
            value_str = (
                str(row["fieldValue"]) if row["fieldValue"] is not None else "None"
            )
            print(f"    {value_str:20s} (weight: {row['weight']})")

    print("\n" + "=" * 80)
    print("9. Custom Weight - Cities Weighted by Average Elevation")
    print("=" * 80)
    print("\nMalloy equivalent:")
    print("  run: airports -> {")
    print("    index: city, state")
    print("    by: avg_elevation")
    print("  }\n")
    print("This finds cities with the highest average elevation")
    print("(useful for finding mountain airports)\n")

    high_elevation_cities = (
        airports.index(s.cols("city", "state"), by="avg_elevation")
        .order_by(lambda t: t.weight.desc())
        .limit(10)
        .execute()
    )
    print(high_elevation_cities)

    print("\n" + "=" * 80)
    print("10. Index Across Joins - Flights with Airport Data")
    print("=" * 80)
    print("\nMalloy equivalent:")
    print("  source: flights extend {")
    print("    join_one: airports on origin = airports.code")
    print("    index: carrier, airports.state")
    print("  }\n")

    # Load flights data
    flights_tbl = con.read_parquet(f"{BASE_URL}/flights.parquet")

    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            carrier=lambda t: t.carrier,
            origin=lambda t: t.origin,
        )
        .with_measures(
            flight_count=lambda t: t.count(),
        )
    )

    flights_with_origin = flights.join_one(airports, left_on="origin", right_on="code")

    joined_index = (
        flights_with_origin.index(s.cols("carrier", "airports__state"))
        .order_by(lambda t: t.weight.desc())
        .limit(15)
        .execute()
    )
    print(joined_index)
    print("\nNote: Index works across joins, showing values from both tables")


if __name__ == "__main__":
    main()
