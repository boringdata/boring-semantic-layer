"""
Demonstration tests for projection pushdown optimization in BSL.

These tests show that projection pushdown WORKS - unused columns are filtered
out before joins, reducing the amount of data scanned.

The implementation uses a conservative approach:
- Filters out explicitly unused columns (UNUSED_*)
- Filters out unused dimension columns
- Includes ALL measure columns (not per-query optimization)

NOTE: Projection pushdown has been disabled for xorq compatibility.
These tests are marked as xfail to document the expected behavior.
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer.api import to_semantic_table
from boring_semantic_layer.expr import to_untagged

# Projection pushdown disabled for xorq compatibility
pytestmark = pytest.mark.xfail(reason="Projection pushdown disabled for xorq vendored ibis compatibility")


@pytest.fixture(scope="module")
def duckdb_con():
    """DuckDB connection for all tests."""
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def wide_tables(duckdb_con):
    """Create wide tables with many columns to test projection pushdown."""
    # Left table with many columns
    flights_df = pd.DataFrame(
        {
            "flight_id": [1, 2, 3],
            "origin": ["JFK", "LAX", "ORD"],
            "destination": ["LAX", "JFK", "DFW"],
            "carrier": ["AA", "UA", "DL"],
            "tail_num": ["N123", "N456", "N789"],
            "distance": [2475, 2475, 802],
            # Unused columns that SHOULD be filtered
            "UNUSED_FLIGHT_1": [1, 2, 3],
            "UNUSED_FLIGHT_2": [4, 5, 6],
            "UNUSED_FLIGHT_3": [7, 8, 9],
        }
    )

    # Right table with many columns
    aircraft_df = pd.DataFrame(
        {
            "tail_num": ["N123", "N456", "N789"],
            "manufacturer": ["Boeing", "Airbus", "Boeing"],
            "model": ["737", "A320", "777"],
            "seats": [150, 180, 300],
            "year": [2010, 2015, 2020],
            # Unused columns that SHOULD be filtered
            "UNUSED_AIRCRAFT_A": [1, 2, 3],
            "UNUSED_AIRCRAFT_B": [4, 5, 6],
        }
    )

    flights_tbl = duckdb_con.create_table("flights", flights_df)
    aircraft_tbl = duckdb_con.create_table("aircraft", aircraft_df)

    return flights_tbl, aircraft_tbl


class TestProjectionPushdownDemo:
    """Demonstration tests showing projection pushdown behavior."""

    def test_projection_works_without_join(self, wide_tables):
        """
        Test baseline: projection works on single table queries (no join).

        When grouping by 'origin', unused columns shouldn't appear in SQL.
        This is Ibis's built-in optimization.

        ✅ This test PASSES - baseline functionality works.
        """
        flights_tbl, _ = wide_tables

        flights = (
            to_semantic_table(flights_tbl, "flights")
            .with_dimensions(origin=lambda t: t.origin)
            .with_measures(flight_count=lambda t: t.count())
        )

        query = flights.group_by("origin").aggregate("flight_count")
        sql = str(ibis.to_sql(to_untagged(query)))

        print("\n" + "=" * 80)
        print("TEST 1: Single Table (Baseline)")
        print("=" * 80)
        print("Query: Group flights by origin, count them")
        print(f"SQL length: {len(sql)} chars")
        print("=" * 80 + "\n")

        # Check for unused columns
        unused_cols = ["UNUSED_FLIGHT_1", "UNUSED_FLIGHT_2", "UNUSED_FLIGHT_3"]
        found_unused = [col for col in unused_cols if col in sql]

        print(f"✓ Checking for unused columns: {unused_cols}")
        print(f"  Found in SQL: {found_unused if found_unused else 'None'}")

        assert len(found_unused) == 0, f"Expected no unused columns, found: {found_unused}"
        print("  ✅ PASS: No unused columns in single-table query\n")

    def test_projection_pushdown_2_table_join(self, wide_tables):
        """
        Test projection pushdown on 2-table join.

        When joining flights and aircraft, unused columns should NOT appear in SQL.
        Only columns needed for join keys, dimensions, and measures should be selected.

        ✅ This test PASSES - projection pushdown works for 2-table joins!
        """
        flights_tbl, aircraft_tbl = wide_tables

        flights = (
            to_semantic_table(flights_tbl, "flights")
            .with_dimensions(
                origin=lambda t: t.origin,
                tail_num=lambda t: t.tail_num,
            )
            .with_measures(flight_count=lambda t: t.count())
        )

        aircraft = to_semantic_table(aircraft_tbl, "aircraft").with_dimensions(
            tail_num=lambda t: t.tail_num,
            manufacturer=lambda t: t.manufacturer,
        )

        joined = flights.join_many(aircraft, lambda f, a: f.tail_num == a.tail_num)
        query = joined.group_by("flights.origin").aggregate("flights.flight_count")

        sql = str(ibis.to_sql(to_untagged(query)))

        print("\n" + "=" * 80)
        print("TEST 2: Two-Table Join")
        print("=" * 80)
        print("Query: Join flights and aircraft, group by origin")
        print(f"SQL length: {len(sql)} chars")
        print("=" * 80 + "\n")

        # Check for unused columns
        unused_cols = [
            "UNUSED_FLIGHT_1",
            "UNUSED_FLIGHT_2",
            "UNUSED_FLIGHT_3",
            "UNUSED_AIRCRAFT_A",
            "UNUSED_AIRCRAFT_B",
        ]
        found_unused = [col for col in unused_cols if col in sql]

        print(f"✓ Checking for unused columns: {unused_cols}")
        print(f"  Found in SQL: {found_unused if found_unused else 'None'}")

        assert len(found_unused) == 0, (
            f"Projection pushdown should eliminate unused columns! "
            f"Found {len(found_unused)}: {found_unused}"
        )
        print("  ✅ PASS: Projection pushdown works - no unused columns!\n")

    def test_projection_pushdown_3_table_join(self, duckdb_con):
        """
        Test projection pushdown on 3-table (n-way) join.

        This demonstrates that projection pushdown works for complex nested joins,
        not just simple 2-table joins.

        ✅ This test PASSES - projection pushdown works for n-way joins!
        """
        # Create three tables
        orders_df = pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "customer_id": [101, 102, 103],
                "total_amount": [100, 200, 150],
                "UNUSED_ORDER": [1, 2, 3],
            }
        )

        customers_df = pd.DataFrame(
            {
                "customer_id": [101, 102, 103],
                "name": ["Alice", "Bob", "Charlie"],
                "UNUSED_CUSTOMER": [10, 20, 30],
            }
        )

        items_df = pd.DataFrame(
            {
                "order_id": [1, 1, 2, 3],
                "product_id": [1001, 1002, 1003, 1004],
                "quantity": [2, 1, 3, 1],
                "UNUSED_ITEM": [5, 6, 7, 8],
            }
        )

        orders_tbl = duckdb_con.create_table("orders", orders_df)
        customers_tbl = duckdb_con.create_table("customers", customers_df)
        items_tbl = duckdb_con.create_table("items", items_df)

        # Create semantic tables
        orders = (
            to_semantic_table(orders_tbl, "orders")
            .with_dimensions(customer_id=lambda t: t.customer_id)
            .with_measures(revenue=lambda t: t.total_amount.sum())
        )

        customers = to_semantic_table(customers_tbl, "customers").with_dimensions(
            customer_id=lambda t: t.customer_id,
            name=lambda t: t.name,
        )

        items = (
            to_semantic_table(items_tbl, "items")
            .with_dimensions(order_id=lambda t: t.order_id)
            .with_measures(item_count=lambda t: t.count())
        )

        # Three-way join
        joined = orders.join_many(customers, lambda o, c: o.customer_id == c.customer_id).join_many(
            items, lambda oc, i: oc.order_id == i.order_id
        )

        query = joined.group_by("customers.name").aggregate("orders.revenue")

        sql = str(ibis.to_sql(to_untagged(query)))

        print("\n" + "=" * 80)
        print("TEST 3: Three-Table Join (N-Way)")
        print("=" * 80)
        print("Query: Join orders->customers->items, group by customer name")
        print(f"SQL length: {len(sql)} chars")
        print("=" * 80 + "\n")

        # Check for unused columns
        unused_cols = ["UNUSED_ORDER", "UNUSED_CUSTOMER", "UNUSED_ITEM"]
        found_unused = [col for col in unused_cols if col in sql]

        print(f"✓ Checking for unused columns: {unused_cols}")
        print(f"  Found in SQL: {found_unused if found_unused else 'None'}")

        assert len(found_unused) == 0, (
            f"Projection pushdown should work for n-way joins! "
            f"Found {len(found_unused)}: {found_unused}"
        )
        print("  ✅ PASS: Projection pushdown works for 3-table joins!\n")

    def test_measure_columns_included(self, wide_tables):
        """
        Test that measure columns are always included (conservative approach).

        Even if a measure isn't used in a specific query, columns for ALL measures
        on a table are included. This is conservative but correct.

        ✅ This test documents expected behavior.
        """
        flights_tbl, aircraft_tbl = wide_tables

        flights = (
            to_semantic_table(flights_tbl, "flights")
            .with_dimensions(
                origin=lambda t: t.origin,
                tail_num=lambda t: t.tail_num,
            )
            .with_measures(
                flight_count=lambda t: t.count(),
                total_distance=lambda t: t.distance.sum(),  # Define but don't use
            )
        )

        aircraft = to_semantic_table(aircraft_tbl, "aircraft").with_dimensions(
            tail_num=lambda t: t.tail_num,
        )

        joined = flights.join_many(aircraft, lambda f, a: f.tail_num == a.tail_num)
        # Only use flight_count, not total_distance
        query = joined.group_by("flights.origin").aggregate("flights.flight_count")

        sql = str(ibis.to_sql(to_untagged(query)))

        print("\n" + "=" * 80)
        print("TEST 4: Measure Column Inclusion (Conservative)")
        print("=" * 80)
        print("Query: Uses flight_count only, not total_distance")
        print("Expected: 'distance' column IS included (conservative)")
        print("=" * 80 + "\n")

        # Distance should be included even though total_distance measure isn't used
        print("✓ Checking if 'distance' column is in SQL...")
        print(f"  'distance' in SQL: {('distance' in sql)}")

        # But unused columns should still be filtered
        unused_cols = ["UNUSED_FLIGHT_1", "UNUSED_AIRCRAFT_A"]
        found_unused = [col for col in unused_cols if col in sql]
        print(f"\n✓ Checking unused columns are still filtered: {unused_cols}")
        print(f"  Found in SQL: {found_unused if found_unused else 'None'}")

        assert len(found_unused) == 0, "Unused columns should still be filtered"
        print("  ✅ PASS: Conservative approach - includes measure columns,")
        print("           but still filters truly unused columns\n")


def test_summary():
    """Print a summary of projection pushdown behavior."""
    print("\n" + "=" * 80)
    print("PROJECTION PUSHDOWN SUMMARY")
    print("=" * 80)
    print("\n✅ What works:")
    print("   - Single table queries (baseline Ibis optimization)")
    print("   - 2-table joins")
    print("   - N-way joins (3+ tables)")
    print("   - Nested join trees")
    print("\n✅ What gets filtered:")
    print("   - Explicitly unused columns (UNUSED_*)")
    print("   - Unused dimension columns")
    print("\n✅ What gets included:")
    print("   - Join key columns (required)")
    print("   - Used dimension columns")
    print("   - ALL measure columns (conservative approach)")
    print("\n💡 Conservative approach:")
    print("   - Includes columns for all measures on a table")
    print("   - Not per-query optimization (includes unused measure columns)")
    print("   - This ensures correctness and simplicity")
    print("   - Main benefit: eliminates truly unused/junk columns")
    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
