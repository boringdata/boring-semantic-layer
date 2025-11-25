"""Tests for projection pushdown optimization in query rewrites.

These tests verify that the query rewriter correctly pushes down column
projections to minimize the number of columns scanned, which is critical
for performance on column-oriented databases like BigQuery and Snowflake.

NOTE: Projection pushdown has been disabled for xorq compatibility.
These tests are marked as xfail to document the expected behavior.
"""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_ibis, to_semantic_table

# Projection pushdown disabled for xorq compatibility
pytestmark = pytest.mark.xfail(
    reason="Projection pushdown disabled for xorq vendored ibis compatibility"
)


@pytest.fixture
def duckdb_con():
    """Create a DuckDB connection for testing."""
    return ibis.duckdb.connect(":memory:")


@pytest.fixture
def wide_tables(duckdb_con):
    """Create wide tables with many unused columns to test projection pushdown."""
    # Create flights table with unused columns
    flights_df = pd.DataFrame(
        {
            "flight_id": [1, 2, 3, 4, 5],
            "origin": ["JFK", "LAX", "ORD", "JFK", "LAX"],
            "destination": ["LAX", "JFK", "LAX", "ORD", "ORD"],
            "tail_num": ["N123", "N456", "N789", "N123", "N456"],
            "distance": [2475, 2475, 1745, 740, 1745],
            # Many unused columns that should NOT appear in SQL
            "UNUSED_COL_1": [1, 2, 3, 4, 5],
            "UNUSED_COL_2": [10, 20, 30, 40, 50],
            "UNUSED_COL_3": [100, 200, 300, 400, 500],
            "UNUSED_COL_4": ["a", "b", "c", "d", "e"],
            "UNUSED_COL_5": ["x", "y", "z", "x", "y"],
        }
    )

    # Create aircraft table with unused columns
    aircraft_df = pd.DataFrame(
        {
            "tail_num": ["N123", "N456", "N789"],
            "manufacturer": ["Boeing", "Airbus", "Boeing"],
            "model": ["737", "A320", "787"],
            # Unused columns that should NOT appear in SQL
            "UNUSED_AIRCRAFT_A": [1, 2, 3],
            "UNUSED_AIRCRAFT_B": [4, 5, 6],
            "UNUSED_AIRCRAFT_C": [7, 8, 9],
            "UNUSED_AIRCRAFT_D": ["p", "q", "r"],
        }
    )

    flights_tbl = duckdb_con.create_table("flights", flights_df)
    aircraft_tbl = duckdb_con.create_table("aircraft", aircraft_df)

    return flights_tbl, aircraft_tbl


def test_projection_pushdown_simple_aggregate(wide_tables):
    """Test that unused columns are not selected in simple aggregation queries."""
    flights_tbl, _ = wide_tables

    # Create semantic table with only a few dimensions and measures
    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(flight_count=lambda t: t.count())
    )

    # Query: group by origin, count flights
    # Should only need: flights.origin (for grouping)
    # Should NOT need: UNUSED_COL_1, UNUSED_COL_2, etc.
    query = flights.group_by("origin").aggregate("flight_count")

    # Get generated SQL
    sql = str(ibis.to_sql(to_ibis(query)))

    # Check that unused columns are NOT in the SQL
    unused_cols = [
        "UNUSED_COL_1",
        "UNUSED_COL_2",
        "UNUSED_COL_3",
        "UNUSED_COL_4",
        "UNUSED_COL_5",
    ]

    for col in unused_cols:
        assert col not in sql, f"Column {col} should not appear in SQL but was found"


def test_projection_pushdown_after_join(wide_tables):
    """Test that projection pushdown works after joins.

    This is the most important test case - after a join, we should only
    select the columns needed for:
    1. Join keys
    2. Dimensions in the final query
    3. Columns needed for measures
    """
    flights_tbl, aircraft_tbl = wide_tables

    # Create semantic tables
    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            origin=lambda t: t.origin,
            tail_num=lambda t: t.tail_num,
        )
        .with_measures(flight_count=lambda t: t.count())
    )

    aircraft = to_semantic_table(aircraft_tbl, name="aircraft").with_dimensions(
        tail_num=lambda t: t.tail_num,
        manufacturer=lambda t: t.manufacturer,
    )

    # Join flights with aircraft
    joined = flights.join(aircraft, lambda f, a: f.tail_num == a.tail_num, how="left")

    # Query: group by origin, count flights
    # Should ONLY need:
    #   - flights.origin (for grouping)
    #   - flights.tail_num (join key)
    #   - aircraft.tail_num (join key)
    # Should NOT need any UNUSED columns
    query = joined.group_by("flights.origin").aggregate("flight_count")

    # Get generated SQL
    sql = str(ibis.to_sql(to_ibis(query)))

    # Check that unused columns from BOTH tables are NOT in SQL
    unused_cols = [
        # From flights table
        "UNUSED_COL_1",
        "UNUSED_COL_2",
        "UNUSED_COL_3",
        "UNUSED_COL_4",
        "UNUSED_COL_5",
        # From aircraft table
        "UNUSED_AIRCRAFT_A",
        "UNUSED_AIRCRAFT_B",
        "UNUSED_AIRCRAFT_C",
        "UNUSED_AIRCRAFT_D",
    ]

    found_unused = [col for col in unused_cols if col in sql]

    assert len(found_unused) == 0, (
        f"Projection pushdown failed: {len(found_unused)} unused columns found in SQL: "
        f"{found_unused}. These columns should have been filtered out."
    )


def test_projection_pushdown_with_dimension_from_right_table(wide_tables):
    """Test projection pushdown when using dimension from the right table."""
    flights_tbl, aircraft_tbl = wide_tables

    # Create semantic tables
    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(tail_num=lambda t: t.tail_num)
        .with_measures(flight_count=lambda t: t.count())
    )

    aircraft = to_semantic_table(aircraft_tbl, name="aircraft").with_dimensions(
        tail_num=lambda t: t.tail_num,
        manufacturer=lambda t: t.manufacturer,
    )

    # Join flights with aircraft
    joined = flights.join(aircraft, lambda f, a: f.tail_num == a.tail_num, how="left")

    # Query: group by manufacturer (from aircraft table), count flights
    # Should need:
    #   - flights.tail_num (join key)
    #   - aircraft.tail_num (join key)
    #   - aircraft.manufacturer (for grouping)
    # Should NOT need unused columns
    query = joined.group_by("aircraft.manufacturer").aggregate("flight_count")

    sql = str(ibis.to_sql(to_ibis(query)))

    # Check unused columns are not present
    unused_cols = [
        "UNUSED_COL_1",
        "UNUSED_COL_2",
        "UNUSED_COL_3",
        "UNUSED_AIRCRAFT_A",
        "UNUSED_AIRCRAFT_B",
        "UNUSED_AIRCRAFT_C",
    ]

    for col in unused_cols:
        assert col not in sql, f"Column {col} should not appear in SQL"


def test_projection_pushdown_with_measure_referencing_columns(wide_tables):
    """Test that columns referenced in measures ARE included in the SQL."""
    flights_tbl, _ = wide_tables

    # Create semantic table with measure that references a specific column
    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),  # References 'distance' column
        )
    )

    # Query using the distance measure
    query = flights.group_by("origin").aggregate("total_distance")

    sql = str(ibis.to_sql(to_ibis(query)))

    # 'distance' column SHOULD be in SQL (needed for measure)
    assert "distance" in sql, "Column 'distance' should be in SQL (needed for measure)"

    # Unused columns should NOT be in SQL
    unused_cols = ["UNUSED_COL_1", "UNUSED_COL_2", "UNUSED_COL_3"]
    for col in unused_cols:
        assert col not in sql, f"Column {col} should not appear in SQL"


def test_projection_pushdown_counts_columns(wide_tables):
    """Test that projection pushdown significantly reduces column count."""
    flights_tbl, aircraft_tbl = wide_tables

    # Create semantic tables
    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            origin=lambda t: t.origin,
            tail_num=lambda t: t.tail_num,
        )
        .with_measures(flight_count=lambda t: t.count())
    )

    aircraft = to_semantic_table(aircraft_tbl, name="aircraft").with_dimensions(
        tail_num=lambda t: t.tail_num,
        manufacturer=lambda t: t.manufacturer,
    )

    # Join and query
    joined = flights.join(aircraft, lambda f, a: f.tail_num == a.tail_num, how="left")
    query = joined.group_by("flights.origin").aggregate("flight_count")

    sql = str(ibis.to_sql(to_ibis(query)))

    # Count SELECT clauses to estimate column reduction
    # Original tables have: 10 (flights) + 7 (aircraft) = 17 total columns
    # We should only need: origin, flights.tail_num, aircraft.tail_num = 3 columns
    # That's a reduction of ~82%

    # Simple heuristic: count commas in SELECT clause (approximate)
    # Extract text between SELECT and FROM
    select_start = sql.upper().find("SELECT")
    from_start = sql.upper().find("FROM", select_start)
    if select_start != -1 and from_start != -1:
        select_clause = sql[select_start:from_start]
        # Count columns (commas + 1, excluding nested parentheses)
        # This is approximate but good enough for the test
        num_columns_selected = select_clause.count(",") + 1

        # Should be significantly less than the 17 total columns
        # Allow some flexibility for join keys and internal columns
        assert num_columns_selected < 10, (
            f"Expected < 10 columns in SELECT, but found ~{num_columns_selected}. "
            "Projection pushdown may not be working optimally."
        )


def test_projection_pushdown_multiple_dimensions(wide_tables):
    """Test projection pushdown when selecting multiple dimensions."""
    flights_tbl, aircraft_tbl = wide_tables

    # Create semantic tables
    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            origin=lambda t: t.origin,
            destination=lambda t: t.destination,
            tail_num=lambda t: t.tail_num,
        )
        .with_measures(flight_count=lambda t: t.count())
    )

    aircraft = to_semantic_table(aircraft_tbl, name="aircraft").with_dimensions(
        tail_num=lambda t: t.tail_num,
        manufacturer=lambda t: t.manufacturer,
        model=lambda t: t.model,
    )

    # Join tables
    joined = flights.join(aircraft, lambda f, a: f.tail_num == a.tail_num, how="left")

    # Query using dimensions from both tables
    query = joined.group_by("flights.origin", "aircraft.manufacturer").aggregate("flight_count")

    sql = str(ibis.to_sql(to_ibis(query)))

    # Should need: origin, tail_num (join keys), manufacturer
    # Should NOT need: destination, model, or any UNUSED columns
    unused_cols = [
        "UNUSED_COL_1",
        "UNUSED_COL_2",
        "UNUSED_COL_3",
        "UNUSED_AIRCRAFT_A",
        "UNUSED_AIRCRAFT_B",
    ]

    for col in unused_cols:
        assert col not in sql, f"Column {col} should not appear in SQL"

    # Also verify we're not selecting unnecessary dimensions
    # 'destination' and 'model' should not be in the SQL since they're not used
    assert "destination" not in sql or sql.count("destination") == 0, (
        "Column 'destination' is not used and should not be selected"
    )
    assert "model" not in sql or sql.count("model") == 0, (
        "Column 'model' is not used and should not be selected"
    )


def test_projection_pushdown_three_way_join_all_notations(duckdb_con):
    """Test projection pushdown with three-way join using all notation styles.

    This test verifies projection pushdown works with n-way joins using top-down
    requirement propagation:
    1. Join predicates with raw column access (e.g., oc.order_id)
    2. Bracket notation with prefixed columns (e.g., t["orders.revenue"])
    3. String notation with prefixes in group_by (e.g., "customers.segment")
    4. Calculated measures with prefixed measure references
    5. Projection pushdown eliminates unused columns from all tables in nested joins
    """
    # Create three tables with unused columns
    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4],
            "customer_id": [101, 102, 101, 103],
            "total_amount": [100, 200, 150, 300],
            "UNUSED_ORDER_1": [1, 2, 3, 4],
            "UNUSED_ORDER_2": ["a", "b", "c", "d"],
        }
    )

    customers_df = pd.DataFrame(
        {
            "customer_id": [101, 102, 103],
            "customer_name": ["Alice", "Bob", "Charlie"],
            "segment": ["Premium", "Standard", "Premium"],
            "UNUSED_CUSTOMER_1": [10, 20, 30],
            "UNUSED_CUSTOMER_2": ["x", "y", "z"],
        }
    )

    items_df = pd.DataFrame(
        {
            "order_id": [1, 1, 2, 3, 3, 4],
            "product_id": [1001, 1002, 1001, 1003, 1004, 1001],
            "quantity": [2, 1, 3, 1, 2, 5],
            "UNUSED_ITEM_1": [5, 6, 7, 8, 9, 10],
            "UNUSED_ITEM_2": ["p", "q", "r", "s", "t", "u"],
        }
    )

    orders_tbl = duckdb_con.create_table("orders", orders_df)
    customers_tbl = duckdb_con.create_table("customers", customers_df)
    items_tbl = duckdb_con.create_table("items", items_df)

    # Create semantic tables
    orders = (
        to_semantic_table(orders_tbl, name="orders")
        .with_dimensions(
            order_id=lambda t: t.order_id,
            customer_id=lambda t: t.customer_id,
        )
        .with_measures(
            revenue=lambda t: t.total_amount.sum(),
            order_count=lambda t: t.count(),
        )
    )

    customers = (
        to_semantic_table(customers_tbl, name="customers")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,
            customer_name=lambda t: t.customer_name,
            segment=lambda t: t.segment,
        )
        .with_measures(customer_count=lambda t: t.count())
    )

    items = (
        to_semantic_table(items_tbl, name="items")
        .with_dimensions(
            order_id=lambda t: t.order_id,
            product_id=lambda t: t.product_id,
        )
        .with_measures(
            total_quantity=lambda t: t.quantity.sum(),
            item_count=lambda t: t.count(),
        )
    )

    # Three-way join using raw column access in predicates
    joined = orders.join(customers, lambda o, c: o.customer_id == c.customer_id, how="left").join(
        items, lambda oc, i: oc.order_id == i.order_id, how="left"
    )

    # Test 1: Use bracket notation with prefixes in calculated measures
    result1 = (
        joined.with_measures(
            # Bracket notation for accessing prefixed measures
            combined_metric=lambda t: (
                t["orders.revenue"] + t["items.total_quantity"] + t["customers.customer_count"]
            ),
        )
        .group_by("customers.segment")  # String notation with prefix
        .aggregate("combined_metric")
    )

    sql1 = str(ibis.to_sql(to_ibis(result1)))

    # Test 2: Use dot notation for unprefixed dimension access
    result2 = (
        joined.with_measures(
            # Mix of bracket notation and operations
            revenue_per_item=lambda t: t["orders.revenue"] / t["items.total_quantity"],
        )
        .group_by("customers.customer_name")  # String notation
        .aggregate("revenue_per_item")
    )

    sql2 = str(ibis.to_sql(to_ibis(result2)))

    # Test 3: Simple query with minimal columns
    result3 = joined.group_by("customers.segment").aggregate("orders.order_count")

    sql3 = str(ibis.to_sql(to_ibis(result3)))

    # Verify projection pushdown works across all queries
    unused_cols = [
        "UNUSED_ORDER_1",
        "UNUSED_ORDER_2",
        "UNUSED_CUSTOMER_1",
        "UNUSED_CUSTOMER_2",
        "UNUSED_ITEM_1",
        "UNUSED_ITEM_2",
    ]

    for sql in [sql1, sql2, sql3]:
        for col in unused_cols:
            assert col not in sql, f"Column {col} should not appear in SQL but was found"

    # Verify that dimension columns that are not used are filtered
    # customer_name is a dimension but not used in sql3 (only segment is used)
    assert "customer_name" not in sql3, "customer_name dimension not used in this query"
    assert "product_id" not in sql3, "product_id dimension not used in this query"

    # Note: Measure columns are currently included for ALL measures defined on a table,
    # not just the measures used in the specific query. This is correct (doesn't break queries)
    # but not maximally optimal. Future optimization could track which measures are actually
    # referenced and only include columns for those specific measures.
    #
    # Current behavior for sql3 (only uses orders.order_count which is count()):
    # - Includes total_amount (for orders.revenue measure, even though not used)
    # - Includes quantity (for items.total_quantity measure, even though not used)
    # This is acceptable - it's a conservative approach that ensures correctness.
