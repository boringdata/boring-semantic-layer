#!/usr/bin/env python3
"""Example demonstrating xorq profile usage with BSL.

This example shows:
1. How to create and save a xorq profile (one-time setup)
2. How to use profiles in BSL YAML configurations
3. How to use profiles directly in Python code
"""

import tempfile
from pathlib import Path

BASE_URL = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev"


def example_1_create_profile():
    """Example 1: Create and save a xorq profile (one-time setup)."""
    print("\n" + "=" * 70)
    print("Example 1: Creating and Saving a Xorq Profile")
    print("=" * 70)

    print("\n1. Create a connection with xorq:")
    try:
        import xorq.api as xo
    except ImportError:
        print("   ✗ xorq not installed. Install with: pip install 'boring-semantic-layer[xorq]'")
        return None, None

    # Create a DuckDB connection with a FILE (not :memory:) so it persists
    import tempfile
    db_file = Path(tempfile.gettempdir()) / "bsl_profile_example.db"

    con = xo.duckdb.connect(str(db_file))
    print(f"   ✓ Created connection: {con.name}")
    print(f"   ✓ Database file: {db_file}")

    # Load some data into it
    print("\n2. Load data into the connection:")
    flights = con.read_parquet(f"{BASE_URL}/flights.parquet")
    carriers = con.read_parquet(f"{BASE_URL}/carriers.parquet")

    # Register as tables
    con.create_table("flights_tbl", flights, overwrite=True)
    con.create_table("carriers_tbl", carriers, overwrite=True)
    print("   ✓ Loaded flights_tbl and carriers_tbl")

    # Check what's in the database
    print("\n3. List tables in connection:")
    tables = con.list_tables()
    print(f"   ✓ Tables: {tables}")

    # Save the profile for reuse
    print("\n4. Save the profile:")
    profile = con._profile
    saved_path = profile.save(alias="my_flights_db", clobber=True, check_secrets=False)
    print(f"   ✓ Profile saved as 'my_flights_db'")
    print(f"   ✓ Location: {saved_path}")

    print("\n5. Profile details:")
    print(f"   - Backend: {profile.con_name}")
    print(f"   - Parameters: {dict(profile.kwargs_tuple)}")
    print(f"   - Hash name: {profile.hash_name}")

    print("\n6. Profile is now saved and can be reused!")
    print("   You can use it in YAML files or load it in Python")
    print(f"   Note: Database file persists at {db_file}")

    return "my_flights_db", db_file


def example_2_use_profile_in_yaml(profile_name: str):
    """Example 2: Use a saved profile in a YAML configuration."""
    print("\n" + "=" * 70)
    print("Example 2: Using Profile in YAML Configuration")
    print("=" * 70)

    if not profile_name:
        print("   ✗ Profile not created, skipping this example")
        return

    print("\n1. Create YAML file using the profile:")

    # Create a temporary YAML file
    yaml_content = f"""
profile: {profile_name}

carriers:
  table: carriers_tbl
  description: "Airline carrier information"
  dimensions:
    code: _.code
    name: _.name
  measures:
    carrier_count: _.count()

flights:
  table: flights_tbl
  description: "Flight data"
  dimensions:
    origin: _.origin
    destination: _.destination
    carrier: _.carrier
  measures:
    flight_count: _.count()
    avg_distance: _.distance.mean()
  joins:
    carriers:
      model: carriers
      type: one
      left_on: carrier
      right_on: code
"""

    temp_yaml = Path(tempfile.gettempdir()) / "bsl_profile_example.yml"
    temp_yaml.write_text(yaml_content)
    print(f"   ✓ Created: {temp_yaml}")

    print("\n2. Load models from YAML:")
    from boring_semantic_layer.yaml import from_yaml

    try:
        models = from_yaml(str(temp_yaml))
        print(f"   ✓ Loaded {len(models)} models: {list(models.keys())}")

        print("\n3. Query the semantic model:")
        flights = models["flights"]

        # Simple aggregation
        result = (
            flights.group_by("carrier")
            .aggregate("flight_count", "avg_distance")
            .order_by(lambda t: t.flight_count.desc())
            .limit(5)
            .execute()
        )

        print("   ✓ Top 5 carriers by flight count:")
        print(result.to_string(index=False))

        print("\n4. Query with join:")
        result_with_names = (
            flights.group_by("carrier", "carriers.name")
            .aggregate("flight_count")
            .order_by(lambda t: t.flight_count.desc())
            .limit(5)
            .execute()
        )

        print("   ✓ Top 5 carriers with names:")
        print(result_with_names.to_string(index=False))

    except Exception as e:
        print(f"   ✗ Error: {e}")
        import traceback

        traceback.print_exc()


def example_3_use_profile_in_python(profile_name: str):
    """Example 3: Use a saved profile directly in Python."""
    print("\n" + "=" * 70)
    print("Example 3: Using Profile Directly in Python")
    print("=" * 70)

    if not profile_name:
        print("   ✗ Profile not created, skipping this example")
        return

    print("\n1. Load connection from profile:")
    from boring_semantic_layer.profile import load_profile, load_tables_from_profile

    try:
        con = load_profile(profile_name)
        print(f"   ✓ Loaded connection: {con.name}")
        print(f"   ✓ Available tables: {con.list_tables()}")

        print("\n2. Load specific tables from profile:")
        tables = load_tables_from_profile(profile_name, ["flights_tbl", "carriers_tbl"])
        print(f"   ✓ Loaded tables: {list(tables.keys())}")

        print("\n3. Create semantic model programmatically:")
        from boring_semantic_layer import to_semantic_table

        flights = (
            to_semantic_table(tables["flights_tbl"], name="flights")
            .with_measures(
                flight_count=lambda t: t.count(),
                total_distance=lambda t: t.distance.sum(),
                avg_distance=lambda t: t.distance.mean(),
            )
        )

        print("   ✓ Created semantic model")

        print("\n4. Query the model:")
        result = (
            flights.filter(lambda t: t.distance > 1000)
            .group_by("origin")
            .aggregate("flight_count", "avg_distance")
            .order_by(lambda t: t.flight_count.desc())
            .limit(5)
            .execute()
        )

        print("   ✓ Top 5 origins for long flights:")
        print(result.to_string(index=False))

    except Exception as e:
        print(f"   ✗ Error: {e}")
        import traceback

        traceback.print_exc()


def example_4_profile_benefits():
    """Example 4: Benefits of using profiles."""
    print("\n" + "=" * 70)
    print("Example 4: Benefits of Using Xorq Profiles")
    print("=" * 70)

    print("\n1. Security:")
    print("   ✓ Store sensitive credentials as environment variables")
    print("   ✓ Profile files only contain ${VAR_NAME} references")
    print("   ✓ Actual values are resolved at connection time")
    print("   ✓ Safe to commit profile configs to git")

    print("\n2. Reusability:")
    print("   ✓ Create profile once, use everywhere")
    print("   ✓ Share profiles across teams")
    print("   ✓ Use same profile in multiple YAML files")
    print("   ✓ Use profiles in notebooks, scripts, and applications")

    print("\n3. Consistency:")
    print("   ✓ All projects use same connection parameters")
    print("   ✓ Easy to update connection settings in one place")
    print("   ✓ No duplicate connection configuration")

    print("\n4. Organization:")
    print("   ✓ Profiles stored in ~/.config/xorq/profiles/")
    print("   ✓ Easy to list and manage profiles")
    print("   ✓ Supports multiple environments (dev, staging, prod)")

    print("\n5. Example profile with environment variables:")
    print("""
    # Save a Postgres profile with secure credentials
    import xorq.api as xo
    con = xo.postgres.connect(
        host='${POSTGRES_HOST}',
        port=5432,
        database='mydb',
        user='${POSTGRES_USER}',
        password='${POSTGRES_PASSWORD}'
    )
    con._profile.save(alias='prod_postgres')
    """)

    print("\n6. Then use it in BSL YAML:")
    print("""
    profile: prod_postgres

    my_model:
      table: my_table
      dimensions: {...}
      measures: {...}
    """)


def main():
    """Run all examples."""
    print("=" * 70)
    print("BSL + Xorq Profiles: Complete Example")
    print("=" * 70)
    print("\nDemonstrating:")
    print("  1. Creating and saving profiles (one-time setup)")
    print("  2. Using profiles in YAML configurations")
    print("  3. Using profiles directly in Python")
    print("  4. Benefits of the profile system")

    try:
        # Example 1: Create a profile
        profile_name, db_file = example_1_create_profile()

        # Example 2: Use it in YAML
        if profile_name:
            example_2_use_profile_in_yaml(profile_name)

        # Example 3: Use it in Python
        if profile_name:
            example_3_use_profile_in_python(profile_name)

        # Example 4: Benefits
        example_4_profile_benefits()

        # Cleanup
        if db_file and db_file.exists():
            print(f"\nNote: Test database file at {db_file}")
            print("Delete it manually if no longer needed")

        print("\n" + "=" * 70)
        print("✓ ALL EXAMPLES COMPLETED SUCCESSFULLY")
        print("=" * 70)
        print("\nKey Takeaways:")
        print("✓ Profiles provide secure, reusable database connections")
        print("✓ Create once with xorq, use everywhere in BSL")
        print("✓ Works seamlessly with YAML configurations")
        print("✓ Supports environment variables for secrets")
        print("✓ Better organization than inline connection configs")

    except Exception as e:
        print(f"\n✗ Example failed: {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
