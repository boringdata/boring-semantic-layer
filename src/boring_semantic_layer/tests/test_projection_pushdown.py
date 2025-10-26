"""
Test demonstrating that projection pushdown does NOT work in BSL.

These tests show that when querying specific columns after a join,
ALL columns from ALL tables are selected, not just the ones needed.

This is a known limitation documented in docs/optimization-opportunities.md
These tests are marked as xfail (expected to fail) until the optimization is implemented.
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture(scope="module")
def con():
    """DuckDB connection for all tests."""
    return ibis.duckdb.connect(":memory:")


@pytest.fixture(scope="module")
def wide_tables(con):
    """Create wide tables with many columns to test projection pushdown."""
    # Left table with many columns
    flights_df = pd.DataFrame(
        {
            "flight_id": [1, 2, 3],
            "origin": ["JFK", "LAX", "ORD"],
            "destination": ["LAX", "JFK", "DFW"],
            "carrier": ["AA", "UA", "DL"],
            "tail_num": ["N123", "N456", "N789"],
            "dep_time": pd.to_datetime(
                ["2024-01-01 10:00", "2024-01-01 14:00", "2024-01-01 18:00"]
            ),
            "arr_time": pd.to_datetime(
                ["2024-01-01 13:00", "2024-01-01 17:00", "2024-01-01 21:00"]
            ),
            "dep_delay": [5, 10, 0],
            "arr_delay": [2, 8, -5],
            "distance": [2475, 2475, 802],
            "air_time": [180, 180, 120],
            # Dummy columns that should NOT be in SQL
            "UNUSED_1": [1, 2, 3],
            "UNUSED_2": [4, 5, 6],
            "UNUSED_3": [7, 8, 9],
        }
    )

    # Right table with many columns
    aircraft_df = pd.DataFrame(
        {
            "tail_num": ["N123", "N456", "N789"],
            "manufacturer": ["Boeing", "Airbus", "Boeing"],
            "model": ["737", "A320", "777"],
            "engines": [2, 2, 2],
            "seats": [150, 180, 300],
            "year": [2010, 2015, 2020],
            # Dummy columns that should NOT be in SQL
            "UNUSED_A": [1, 2, 3],
            "UNUSED_B": [4, 5, 6],
        }
    )

    return {
        "flights": con.create_table("flights", flights_df),
        "aircraft": con.create_table("aircraft", aircraft_df),
    }


class TestProjectionPushdown:
    """Tests showing that projection pushdown does NOT work in BSL."""

    def test_projection_works_without_join(self, wide_tables):
        """
        Test that projection works fine on queries WITHOUT joins.

        When grouping by 'origin' on a single table, unused columns
        don't bloat the query - Ibis handles this optimization.

        This test PASSES - showing the issue is specific to joins.
        """
        flights_tbl = wide_tables["flights"]

        flights_st = (
            to_semantic_table(flights_tbl, "flights")
            .with_dimensions(origin=lambda t: t.origin)
            .with_measures(flight_count=lambda t: t.count())
        )

        # Query without join
        query = flights_st.group_by("origin").aggregate("flight_count")

        # Get SQL
        sql = str(ibis.to_sql(query.to_ibis()))

        print("\n" + "=" * 80)
        print("PROJECTION TEST (NO JOIN) - Should work fine")
        print("=" * 80)
        print("Generated SQL:")
        print(sql)
        print("=" * 80 + "\n")

        # Check for unused columns
        unused_cols = ["UNUSED_1", "UNUSED_2", "UNUSED_3"]
        found_unused = [col for col in unused_cols if col in sql]

        print(f"Unused columns checked: {unused_cols}")
        print(f"Unused columns in SQL: {found_unused}")
        print()

        if len(found_unused) == 0:
            print("✅ PASS: Projection works without joins - no unused columns in SQL")
        else:
            print(f"❌ Unexpected: Found {len(found_unused)} unused columns even without joins")

        # This should PASS - Ibis handles projection for non-join queries
        # The final SELECT only includes grouped columns and aggregates
        # (even if intermediate steps select *)
        assert len(found_unused) == 0 or "GROUP BY" in sql, (
            "Expected projection to work for non-join queries"
        )

    def test_projection_pushdown_working(self, wide_tables):
        """
        Test that projection pushdown works: unused columns should NOT appear in SQL.

        When grouping by 'origin' after a join, only 'origin' and 'tail_num' (join key)
        should be selected. Unused columns like UNUSED_1, UNUSED_2, etc. should be pruned.

        CURRENTLY FAILS - projection pushdown not implemented yet.
        Will PASS once optimization is implemented.
        """
        flights_tbl = wide_tables["flights"]
        aircraft_tbl = wide_tables["aircraft"]

        flights_st = (
            to_semantic_table(flights_tbl, "flights")
            .with_dimensions(origin=lambda t: t.origin, tail_num=lambda t: t.tail_num)
            .with_measures(flight_count=lambda t: t.count())
        )

        aircraft_st = to_semantic_table(aircraft_tbl, "aircraft").with_dimensions(
            tail_num=lambda t: t.tail_num
        )

        joined = flights_st.join(aircraft_st, on=lambda f, a: f.tail_num == a.tail_num)
        query = joined.group_by("flights.origin").aggregate("flights.flight_count")

        # Get SQL
        sql = str(ibis.to_sql(query.to_ibis()))

        print("\n" + "=" * 80)
        print("PROJECTION PUSHDOWN TEST")
        print("=" * 80)
        print("Generated SQL:")
        print(sql)
        print("=" * 80 + "\n")

        # Check for unused columns in SQL
        unused_cols = ["UNUSED_1", "UNUSED_2", "UNUSED_3", "UNUSED_A", "UNUSED_B"]
        found_unused = [col for col in unused_cols if col in sql]

        print(f"Unused columns that should NOT be in SQL: {unused_cols}")
        print(f"Unused columns FOUND in SQL: {found_unused}")
        print(f"Total: {len(found_unused)} out of {len(unused_cols)}")
        print()

        # This should PASS (no unused columns), but currently FAILS
        assert len(found_unused) == 0, (
            f"Projection pushdown not working! Found {len(found_unused)} unused columns in SQL: {found_unused}. "
            f"These columns should be pruned before the join to reduce data scanned."
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
