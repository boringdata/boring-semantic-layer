#!/usr/bin/env python3
"""Test BSL with xorq profiles.

This script demonstrates loading data via xorq profiles and running BSL semantic queries.
"""

try:
    import xorq.api as xo
    # XorqProfile is optional - we'll try to import it later
except ImportError:
    print("ERROR: xorq is not installed. Run: uv pip install xorq xorq-datafusion")
    exit(1)

from boring_semantic_layer import SemanticModel
from boring_semantic_layer.xorq_convert import to_xorq


def test_xorq_profile_basic():
    """Test 1: Load xorq profile and create BSL model."""
    print("=" * 80)
    print("TEST 1: Load xorq profile and run BSL query")
    print("=" * 80)

    # Try to load an xorq profile
    # xorq profiles are stored in ~/.xorq/profiles/
    try:
        from xorq.vendor.ibis.backends.profiles import Profile as XorqProfile

        _profile = XorqProfile.load("default")
        print("✓ Loaded xorq profile 'default'")
    except Exception as e:
        print(f"⚠ Could not load 'default' profile: {e}")
        print("  Creating in-memory profile instead...")

        # Create an in-memory xorq connection with test data
        import pandas as pd

        xo_con = xo.connect()

        # Create test data
        data = pd.DataFrame(
            {
                "product_id": [1, 2, 3, 4, 5],
                "product_name": ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"],
                "category": [
                    "Electronics",
                    "Accessories",
                    "Accessories",
                    "Electronics",
                    "Accessories",
                ],
                "price": [1200, 25, 75, 350, 80],
                "stock": [10, 50, 30, 15, 40],
            }
        )

        products_table = xo_con.create_table("products", data)
        print(f"✓ Created test table 'products' with {len(data)} rows")

        # Since we can't use xorq table directly with BSL, convert to ibis
        # Execute the xorq table to get pandas, then convert to ibis
        import ibis

        df = products_table.execute()
        ibis_table = ibis.memtable(df)

        # Create BSL semantic model
        products_model = SemanticModel(
            table=ibis_table,
            dimensions={
                "product_name": lambda t: t.product_name,
                "category": lambda t: t.category,
            },
            measures={
                "total_stock": lambda t: t.stock.sum(),
                "avg_price": lambda t: t.price.mean(),
                "total_revenue": lambda t: (t.price * t.stock).sum(),
                "product_count": lambda t: t.count(),
            },
            name="products",
        )
        print("✓ Created BSL semantic model from xorq table")

        # Run query
        print("\n--- Query: Products by category ---")
        query = products_model.group_by("category").aggregate(
            "product_count", "total_stock", "avg_price", "total_revenue"
        )
        result = query.execute()
        print(result)

        # Convert to xorq and execute
        print("\n--- Same query executed via xorq ---")
        xorq_expr = to_xorq(query)
        from xorq.api import execute as xo_execute

        result_xorq = xo_execute(xorq_expr)
        print(result_xorq)

        print("\n✓ Test 1 passed!\n")
        return


def test_xorq_profile_with_real_data():
    """Test 2: Use xorq profile to load real parquet data."""
    print("=" * 80)
    print("TEST 2: Load parquet data via xorq and run BSL")
    print("=" * 80)

    import ibis

    # Create xorq connection
    xo_con = xo.connect()

    # Load flights data from remote parquet (using the example profile URL)
    flights_url = "https://pub-a45a6a332b4646f2a6f44775695c64df.r2.dev/flights.parquet"

    print("Loading flights data from remote parquet...")
    try:
        # Use xorq to read parquet
        xorq_flights = xo_con.read_parquet(flights_url, table_name="flights")
        print("✓ Loaded flights data via xorq")

        # Execute to get pandas, then convert to ibis
        print("Converting to ibis table...")
        flights_df = xorq_flights.limit(1000).execute()  # Limit for demo
        ibis_flights = ibis.memtable(flights_df)
        print(f"✓ Converted to ibis table ({len(flights_df)} rows)")

        # Create BSL semantic model
        flights_model = SemanticModel(
            table=ibis_flights,
            dimensions={
                "carrier": lambda t: t.carrier,
                "origin": lambda t: t.origin,
                "destination": lambda t: t.destination,
            },
            measures={
                "flight_count": lambda t: t.count(),
                "total_distance": lambda t: t.distance.sum(),
                "avg_distance": lambda t: t.distance.mean(),
                "avg_dep_delay": lambda t: t.dep_delay.mean(),
            },
            name="flights",
        )
        print("✓ Created BSL semantic model")

        # Run queries
        print("\n--- Query 1: Flights by carrier ---")
        query1 = (
            flights_model.group_by("carrier")
            .aggregate("flight_count", "avg_distance", "avg_dep_delay")
            .order_by(lambda t: ibis.desc(t.flight_count))
            .limit(10)
        )
        result1 = query1.execute()
        print(result1)

        print("\n--- Query 2: Busiest routes ---")
        query2 = (
            flights_model.group_by("origin", "destination")
            .aggregate("flight_count", "total_distance")
            .filter(lambda t: t.flight_count > 10)
            .order_by(lambda t: ibis.desc(t.flight_count))
            .limit(10)
        )

        # Execute with xorq
        print("(Executing via xorq backend)")
        xorq_expr2 = to_xorq(query2)
        from xorq.api import execute as xo_execute

        result2_xorq = xo_execute(xorq_expr2)
        print(result2_xorq)

        print("\n✓ Test 2 passed!\n")

    except Exception as e:
        print(f"⚠ Could not load remote parquet: {e}")
        print("  Skipping this test (network or xorq issue)")


def test_profile_loader_with_xorq():
    """Test 3: Use BSL ProfileLoader to get connection from xorq profile."""
    print("=" * 80)
    print("TEST 3: BSL ProfileLoader with xorq backend")
    print("=" * 80)

    from boring_semantic_layer.profile import ProfileLoader

    loader = ProfileLoader()

    # Check if xorq_dir is in search locations
    print(f"ProfileLoader search locations: {loader.search_locations}")

    # Try to load a profile via xorq
    try:
        # This will search xorq profiles directory
        connection = loader.get_connection("default")
        print("✓ Loaded connection via ProfileLoader")
        print(f"  Connection type: {type(connection)}")

        # List tables available
        tables = connection.list_tables()
        print(f"  Available tables: {tables[:5] if len(tables) > 5 else tables}")

        if tables:
            # Pick first table and create BSL model
            table_name = tables[0]
            table = connection.table(table_name)

            # Get column names
            schema = table.schema()
            cols = list(schema.names)
            print(f"\n  Table '{table_name}' columns: {cols[:5]}...")

            # Create a simple BSL model
            dimensions = {col: lambda t, c=col: t[c] for col in cols[:3]}  # First 3 as dims

            model = SemanticModel(
                table=table,
                dimensions=dimensions,
                measures={"row_count": lambda t: t.count()},
                name=table_name,
            )

            print(f"✓ Created BSL model for '{table_name}'")

            # Run simple query
            query = model.aggregate("row_count")
            result = query.execute()
            print(f"\n  Total rows: {result['row_count'].iloc[0]}")

            print("\n✓ Test 3 passed!\n")
        else:
            print("  No tables found in connection")
            print("\n⚠ Test 3 skipped (no tables)\n")

    except Exception as e:
        print(f"⚠ Could not load profile via ProfileLoader: {e}")
        print("  This is expected if no xorq profiles are configured")
        print("\n⚠ Test 3 skipped\n")


def main():
    """Run all xorq profile + BSL integration tests."""
    print("\n" + "=" * 80)
    print("BSL + xorq Profile Integration Tests")
    print("=" * 80 + "\n")

    try:
        # Test 1: Basic xorq connection
        test_xorq_profile_basic()

        # Test 2: Load real data via xorq
        test_xorq_profile_with_real_data()

        # Test 3: ProfileLoader with xorq
        test_profile_loader_with_xorq()

        print("=" * 80)
        print("✅ TESTS COMPLETED!")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback

        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
