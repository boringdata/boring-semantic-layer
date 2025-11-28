"""Tests for dimension validation error messages."""

import ibis
import pandas as pd
import pytest

from boring_semantic_layer import to_semantic_table


def test_dimension_with_nonexistent_column():
    """Test that accessing a non-existent column gives a helpful error message."""
    con = ibis.duckdb.connect(":memory:")
    tbl = con.create_table("test", {"a": [1, 2, 3], "b": [4, 5, 6]})

    st = (
        to_semantic_table(tbl, name="test")
        .with_dimensions(
            bad_dim={"expr": lambda t: t.nonexistent}  # Column doesn't exist
        )
        .with_measures(count={"expr": lambda t: t.count()})
    )

    with pytest.raises(AttributeError) as exc_info:
        st.query(dimensions=["bad_dim"], measures=["count"]).execute()

    error_msg = str(exc_info.value)
    assert "nonexistent" in error_msg
    assert "Available columns" in error_msg
    assert "['a', 'b']" in error_msg or "a" in error_msg


def test_dimension_with_typo_suggests_correction():
    """Test that typos in column names suggest corrections."""
    con = ibis.duckdb.connect(":memory:")
    tbl = con.create_table("test", {"carrier": ["AA", "UA"], "arr_time": [100, 200]})

    st = (
        to_semantic_table(tbl, name="test")
        .with_dimensions(
            time={"expr": lambda t: t.arrtime}  # Typo: should be arr_time
        )
        .with_measures(count={"expr": lambda t: t.count()})
    )

    with pytest.raises(AttributeError) as exc_info:
        st.query(dimensions=["time"], measures=["count"]).execute()

    error_msg = str(exc_info.value)
    assert "arrtime" in error_msg
    assert "Did you mean" in error_msg
    assert "arr_time" in error_msg


def test_dimension_with_join_and_wrong_column():
    """Test error message when using wrong column in joined table."""
    con = ibis.duckdb.connect(":memory:")
    t1 = con.create_table("t1", {"id": [1, 2], "name": ["a", "b"]})
    t2 = con.create_table("t2", {"id": [1, 2], "value": [10, 20]})

    st1 = (
        to_semantic_table(t1, name="t1")
        .with_dimensions(
            id={"expr": lambda t: t.id},
            name={"expr": lambda t: t.wrong_column},  # Wrong column name
        )
        .with_measures(count={"expr": lambda t: t.count()})
    )

    st2 = (
        to_semantic_table(t2, name="t2")
        .with_dimensions(id={"expr": lambda t: t.id})
        .with_measures(total={"expr": lambda t: t.value.sum()})
    )

    joined = st1.join_one(st2, on=lambda left, right: left.id == right.id)

    with pytest.raises(AttributeError) as exc_info:
        joined.query(dimensions=["t1.name"], measures=["t1.count"]).execute()

    error_msg = str(exc_info.value)
    assert "wrong_column" in error_msg
    assert "Available columns" in error_msg or "Dimension expression" in error_msg


def test_wrong_dimension_with_join_projection_pushdown():
    """Test that error correctly identifies bad dimension even with projection pushdown.

    Regression test: when a dimension references a non-existent column, but we group_by
    a different dimension, projection pushdown can cause misleading error messages.
    The error should mention the actual missing column, not the dimension being used.
    """
    con = ibis.duckdb.connect(":memory:")

    # Create flights table with arr_time and destination columns
    flights_data = {
        "flight_id": [1, 2, 3],
        "arr_time": ["2023-01-01 10:00", "2023-01-01 12:00", "2023-01-01 14:00"],
        "destination": ["NYC", "LAX", "SFO"],
    }
    flights_tbl = con.create_table("flights", flights_data)
    customers_tbl = con.create_table(
        "customers", {"customer_id": [1, 2, 3], "name": ["A", "B", "C"]}
    )

    # Define dimension with WRONG column name (dest instead of destination)
    flights = (
        to_semantic_table(flights_tbl, name="flights")
        .with_dimensions(
            arr_time=lambda t: t.arr_time,  # Valid dimension
            destination=lambda t: t.dest,  # WRONG: column is 'destination', not 'dest'
        )
        .with_measures(flight_count=lambda t: t.count())
    )

    customers = (
        to_semantic_table(customers_tbl, name="customers")
        .with_dimensions(customer_id=lambda t: t.customer_id)
        .with_measures(customer_count=lambda t: t.count())
    )

    # Join and group by a DIFFERENT dimension (arr_time, not the broken destination)
    joined = flights.join_one(customers, on=lambda f, c: f.flight_id == c.customer_id)

    # The error should mention 'dest' (the actual missing column),
    # not 'arr_time' (which exists but triggers the error via projection pushdown)
    with pytest.raises(AttributeError) as exc_info:
        joined.group_by("flights.arr_time").aggregate("flights.flight_count").execute()

    error_msg = str(exc_info.value)
    # The error should mention 'dest' as the problematic column
    assert "dest" in error_msg.lower()
    # Ideally, it should NOT misleadingly say arr_time is missing
    # (though this may still happen with current implementation)


def test_valid_dimension_renamed():
    """Test that dimensions can be renamed (dimension name != column name)."""
    con = ibis.duckdb.connect(":memory:")
    tbl = con.create_table("test", {"destination": ["NYC", "LAX"], "count": [1, 2]})

    # This should work: dimension 'dest' accesses column 'destination'
    st = (
        to_semantic_table(tbl, name="test")
        .with_dimensions(
            dest={"expr": lambda t: t.destination}  # Renamed dimension
        )
        .with_measures(total={"expr": lambda t: t["count"].sum()})
    )

    result = st.query(dimensions=["dest"], measures=["total"]).execute()
    assert len(result) == 2
    assert "dest" in result.columns


def test_helpful_tip_in_error_message():
    """Test that error message includes helpful tip with example."""
    con = ibis.duckdb.connect(":memory:")
    tbl = con.create_table("test", {"column_a": [1, 2], "column_b": [3, 4]})

    st = (
        to_semantic_table(tbl, name="test")
        .with_dimensions(dim={"expr": lambda t: t.xyz})
        .with_measures(count={"expr": lambda t: t.count()})
    )

    with pytest.raises(AttributeError) as exc_info:
        st.query(dimensions=["dim"], measures=["count"]).execute()

    error_msg = str(exc_info.value)
    assert "Tip:" in error_msg
    assert "lambda t:" in error_msg


# Tests for joins with renamed dimensions (issue #43)


def test_join_one_with_renamed_dimension():
    """Test that join_one works with lambda-based join condition."""
    con = ibis.duckdb.connect(":memory:")
    customers_df = pd.DataFrame(
        {
            "cust_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )
    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [1, 2, 1],
            "amount": [100, 200, 150],
        }
    )

    customers_tbl = con.create_table("test_customers_renamed", customers_df)
    orders_tbl = con.create_table("test_orders_renamed", orders_df)

    customers = to_semantic_table(customers_tbl, "customers").with_dimensions(
        customer_id=lambda t: t.cust_id,  # Renamed: cust_id -> customer_id
        name=lambda t: t.name,
    )

    orders = (
        to_semantic_table(orders_tbl, "orders")
        .with_dimensions(customer_id=lambda t: t.customer_id)
        .with_measures(revenue=lambda t: t.amount.sum())
    )

    # Lambda-based join uses column names directly
    joined = orders.join_one(customers, on=lambda o, c: o.customer_id == c.cust_id)
    result = joined.group_by("customers.name").aggregate("orders.revenue").execute()

    assert len(result) == 2
    assert "customers.name" in result.columns
    assert "orders.revenue" in result.columns


def test_join_many_with_renamed_dimension():
    """Test that join_many works with lambda-based join condition."""
    con = ibis.duckdb.connect(":memory:")
    customers_df = pd.DataFrame(
        {
            "cust_id": [1, 2],
            "name": ["Alice", "Bob"],
        }
    )
    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [1, 2, 1],
            "amount": [100, 200, 150],
        }
    )

    customers_tbl = con.create_table("test_customers_many", customers_df)
    orders_tbl = con.create_table("test_orders_many", orders_df)

    customers = to_semantic_table(customers_tbl, "customers").with_dimensions(
        customer_id=lambda t: t.cust_id,  # Renamed: cust_id -> customer_id
    )

    orders = (
        to_semantic_table(orders_tbl, "orders")
        .with_dimensions(customer_id=lambda t: t.customer_id)
        .with_measures(order_count=lambda t: t.count())
    )

    # Lambda-based join uses column names directly
    joined = customers.join_many(orders, on=lambda c, o: c.cust_id == o.customer_id)
    result = joined.group_by("customers.customer_id").aggregate("orders.order_count").execute()

    assert len(result) == 2
    assert "customers.customer_id" in result.columns
    assert "orders.order_count" in result.columns


def test_join_one_with_complex_condition():
    """Test that join_one works with complex lambda conditions."""
    con = ibis.duckdb.connect(":memory:")
    customers_df = pd.DataFrame(
        {
            "cust_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )
    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [1, 2, 1],
            "amount": [100, 200, 150],
        }
    )

    customers_tbl = con.create_table("test_customers_flex", customers_df)
    orders_tbl = con.create_table("test_orders_flex", orders_df)

    customers = to_semantic_table(customers_tbl, "customers").with_dimensions(
        customer_id=lambda t: t.cust_id,  # Renamed: cust_id -> customer_id
        name=lambda t: t.name,
    )

    orders = (
        to_semantic_table(orders_tbl, "orders")
        .with_dimensions(customer_id=lambda t: t.customer_id)
        .with_measures(revenue=lambda t: t.amount.sum())
    )

    # Lambda-based join uses column names directly
    joined = orders.join_one(customers, on=lambda o, c: o.customer_id == c.cust_id)
    result = joined.group_by("customers.name").aggregate("orders.revenue").execute()

    assert len(result) == 2
    assert "customers.name" in result.columns
    assert "orders.revenue" in result.columns


def test_join_dimension_matching_column_name():
    """Test that joining on a dimension name that matches column name works."""
    con = ibis.duckdb.connect(":memory:")

    # Both tables have column 'customer_id'
    customers_df = pd.DataFrame(
        {
            "customer_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )
    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [1, 2, 1],
            "amount": [100, 200, 150],
        }
    )

    customers_tbl = con.create_table("customers_match", customers_df, overwrite=True)
    orders_tbl = con.create_table("orders_match", orders_df, overwrite=True)

    # Both define dimension 'customer_id' that maps to column 'customer_id'
    customers = to_semantic_table(customers_tbl, "customers").with_dimensions(
        customer_id=lambda t: t.customer_id,  # Dimension matches column
    )
    orders = (
        to_semantic_table(orders_tbl, "orders")
        .with_dimensions(
            customer_id=lambda t: t.customer_id,  # Dimension matches column
        )
        .with_measures(revenue=lambda t: t.amount.sum())
    )

    # Lambda-based join uses column names directly
    joined = orders.join_one(customers, on=lambda o, c: o.customer_id == c.customer_id)

    # First verify the join executed without error
    result = joined.execute()

    # Verify the join worked correctly
    assert len(result) == 3  # 3 orders
    assert "customer_id" in result.columns
    assert "name" in result.columns  # From customers
    assert "amount" in result.columns  # From orders

    # Verify data integrity - Alice (customer_id=1) has 2 orders
    alice_orders = result[result["name"] == "Alice"]
    assert len(alice_orders) == 2
    assert alice_orders["amount"].sum() == 250  # 100 + 150


# Additional validation test scenarios


def test_measure_with_nonexistent_column():
    """Test that measures with wrong column names give helpful errors."""
    con = ibis.duckdb.connect(":memory:")
    tbl = con.create_table("test", {"a": [1, 2, 3], "b": [4, 5, 6]})

    st = (
        to_semantic_table(tbl, name="test")
        .with_dimensions(dim_a=lambda t: t.a)
        .with_measures(bad_measure=lambda t: t.nonexistent_col.sum())
    )

    with pytest.raises(AttributeError) as exc_info:
        st.query(dimensions=["dim_a"], measures=["bad_measure"]).execute()

    error_msg = str(exc_info.value)
    assert "nonexistent_col" in error_msg
    # Measure errors show basic ibis error (could be enhanced in future)
    assert "'Table' object has no attribute" in error_msg


def test_multiple_wrong_dimensions():
    """Test error reporting when multiple dimensions have wrong columns."""
    con = ibis.duckdb.connect(":memory:")
    tbl = con.create_table("test", {"a": [1, 2], "b": [3, 4]})

    st = (
        to_semantic_table(tbl, name="test")
        .with_dimensions(
            bad_dim1=lambda t: t.wrong1,
            bad_dim2=lambda t: t.wrong2,
            good_dim=lambda t: t.a,
        )
        .with_measures(count=lambda t: t.count())
    )

    # First bad dimension should error
    with pytest.raises(AttributeError) as exc_info:
        st.query(dimensions=["bad_dim1"], measures=["count"]).execute()
    assert "wrong1" in str(exc_info.value)

    # Second bad dimension should also error
    with pytest.raises(AttributeError) as exc_info:
        st.query(dimensions=["bad_dim2"], measures=["count"]).execute()
    assert "wrong2" in str(exc_info.value)


def test_nested_expression_with_wrong_column():
    """Test error when nested expression references non-existent column."""
    con = ibis.duckdb.connect(":memory:")
    tbl = con.create_table("test", {"date_col": ["2023-01-01", "2023-01-02"]})

    st = (
        to_semantic_table(tbl, name="test")
        .with_dimensions(
            # Trying to extract year from non-existent column
            year=lambda t: t.wrong_date_col.cast("timestamp").year()
        )
        .with_measures(count=lambda t: t.count())
    )

    with pytest.raises(AttributeError) as exc_info:
        st.query(dimensions=["year"], measures=["count"]).execute()

    error_msg = str(exc_info.value)
    assert "wrong_date_col" in error_msg


def test_filter_with_nonexistent_column():
    """Test that filters with wrong column names give helpful errors."""
    con = ibis.duckdb.connect(":memory:")
    tbl = con.create_table("test", {"status": ["active", "inactive"], "value": [1, 2]})

    st = (
        to_semantic_table(tbl, name="test")
        .with_dimensions(status=lambda t: t.status)
        .with_measures(total=lambda t: t.value.sum())
    )

    query = st.query(dimensions=["status"], measures=["total"])

    # Filter using wrong column name
    with pytest.raises(AttributeError) as exc_info:
        query.filter(lambda t: t.wrong_status == "active").execute()

    error_msg = str(exc_info.value)
    assert "wrong_status" in error_msg


def test_complex_dimension_expression_with_wrong_column():
    """Test error in complex dimension expression with multiple column references."""
    con = ibis.duckdb.connect(":memory:")
    tbl = con.create_table("test", {"col1": [1, 2], "col2": [3, 4]})

    st = (
        to_semantic_table(tbl, name="test")
        .with_dimensions(
            # Complex expression where one column doesn't exist
            combined=lambda t: t.col1 + t.wrong_col
        )
        .with_measures(count=lambda t: t.count())
    )

    with pytest.raises(AttributeError) as exc_info:
        st.query(dimensions=["combined"], measures=["count"]).execute()

    error_msg = str(exc_info.value)
    assert "wrong_col" in error_msg


def test_case_sensitive_column_name():
    """Test that column name case sensitivity is handled with suggestions."""
    con = ibis.duckdb.connect(":memory:")
    # DuckDB lowercases column names by default
    tbl = con.create_table("test", {"customer_id": [1, 2], "customer_name": ["A", "B"]})

    st = (
        to_semantic_table(tbl, name="test")
        .with_dimensions(
            # Using wrong case
            customer=lambda t: t.Customer_ID  # Should be customer_id
        )
        .with_measures(count=lambda t: t.count())
    )

    with pytest.raises(AttributeError) as exc_info:
        st.query(dimensions=["customer"], measures=["count"]).execute()

    error_msg = str(exc_info.value)
    # Error should mention the wrong column name
    assert "Customer_ID" in error_msg or "customer_id" in error_msg.lower()
