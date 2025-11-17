#!/usr/bin/env python3
"""
Example: Loading Semantic Models from YAML

This example demonstrates how to define semantic models in YAML
and load them into the Boring Semantic Layer.

Benefits of YAML configuration:
- Declarative model definitions
- Easy to version control and review
- Non-Python users can define models
- Clean separation of model definition from query logic
"""

from pathlib import Path

from boring_semantic_layer import from_yaml


def main():
    print("=" * 80)
    print("  Example: Loading Semantic Models from YAML with Profiles")
    print("=" * 80)

    # ============================================================================
    # STEP 1: Load semantic models from YAML using profiles
    # ============================================================================
    print("\n" + "-" * 80)
    print("STEP 1: Load semantic models from YAML using profiles")
    print("-" * 80)
    print("\n💡 This example uses profiles.yml to define the database connection")
    print("   and automatically load tables from remote parquet files.")

    # Load models from YAML file (path relative to this script)
    # The yaml_example.yml file references the 'my_flights_db' profile
    # which is defined in profiles.yml
    yaml_path = Path(__file__).parent / "yaml_example.yml"
    models = from_yaml(str(yaml_path))

    print(f"\n✓ Loaded {len(models)} models from YAML:")
    for name in models:
        print(f"  - {name}")

    # Get the models
    carriers = models["carriers"]
    flights = models["flights"]

    print("\n✓ Flights model has:")
    print(f"  Dimensions: {list(flights.dimensions)}")
    print(f"  Measures: {flights.measures}")

    # ============================================================================
    # STEP 3: Query the loaded models
    # ============================================================================
    print("\n" + "=" * 80)
    print("STEP 3: Query the loaded models")
    print("=" * 80)

    # Query 1: Basic aggregation
    print("\n" + "-" * 80)
    print("Query 1: Flight counts by origin")
    print("-" * 80)

    result = flights.group_by("origin").aggregate("flight_count").execute()
    print(result)

    # Query 2: Multiple dimensions and measures
    print("\n" + "-" * 80)
    print("Query 2: Distance metrics by carrier")
    print("-" * 80)

    result = (
        flights.group_by("carrier")
        .aggregate("flight_count", "avg_distance", "total_distance")
        .execute()
    )
    print(result)

    # Query 3: Using joined data
    print("\n" + "-" * 80)
    print("Query 3: Using joined carrier names")
    print("-" * 80)
    print("💡 The join was defined in the YAML file!")

    result = (
        flights.group_by("flights.origin", "carriers.name")
        .aggregate("flight_count", "avg_distance")
        .execute()
    )
    print(result)

    # Query 4: Carriers model
    print("\n" + "-" * 80)
    print("Query 4: Query the carriers model directly")
    print("-" * 80)

    result = carriers.group_by("name").aggregate("carrier_count").execute()
    print(result)

    print("\n" + "=" * 80)
    print("✅ Example completed successfully!")
    print("=" * 80)
    print("\n🎯 KEY TAKEAWAYS:")
    print("  ✓ Models can be defined declaratively in YAML")
    print("  ✓ YAML supports dimensions, measures, and joins")
    print("  ✓ Both simple and extended (with descriptions) formats work")
    print("  ✓ Time dimensions can be marked with metadata")
    print("  ✓ Once loaded, models work exactly like Python-defined models")
    print("\n📚 See yaml_example.yml for the model definitions")
    print()


if __name__ == "__main__":
    main()
