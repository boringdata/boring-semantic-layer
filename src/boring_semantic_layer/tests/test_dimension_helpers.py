"""Tests for entity_dimension and time_dimension helper functions."""

import ibis

from boring_semantic_layer import entity_dimension, time_dimension, to_semantic_table


def test_entity_dimension_basic():
    """Test that entity_dimension creates a dimension with is_entity=True."""
    con = ibis.duckdb.connect(":memory:")
    data = ibis.memtable({"business_id": [1, 2, 3], "value": [100, 200, 300]})
    tbl = con.create_table("test", data)

    # Create semantic table with entity dimension
    st = to_semantic_table(tbl, name="test_table").with_dimensions(
        business_id=entity_dimension(lambda t: t.business_id),
    )

    # Check that the dimension is marked as entity
    json_def = st.json_definition
    assert "business_id" in json_def["dimensions"]
    assert "business_id" in json_def["entity_dimensions"]
    assert json_def["dimensions"]["business_id"]["is_entity"] is True


def test_entity_dimension_with_description():
    """Test that entity_dimension accepts and stores description."""
    con = ibis.duckdb.connect(":memory:")
    data = ibis.memtable({"user_id": [1, 2, 3], "value": [100, 200, 300]})
    tbl = con.create_table("test", data)

    # Create semantic table with entity dimension and description
    st = to_semantic_table(tbl, name="test_table").with_dimensions(
        user_id=entity_dimension(lambda t: t.user_id, "User identifier"),
    )

    # Check that description is stored
    json_def = st.json_definition
    assert json_def["dimensions"]["user_id"]["description"] == "User identifier"
    assert json_def["dimensions"]["user_id"]["is_entity"] is True


def test_time_dimension_basic():
    """Test that time_dimension creates a dimension with is_event_timestamp=True."""
    con = ibis.duckdb.connect(":memory:")
    data = ibis.memtable(
        {
            "statement_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "value": [100, 200, 300],
        }
    )
    tbl = con.create_table("test", data)

    # Create semantic table with time dimension
    st = to_semantic_table(tbl, name="test_table").with_dimensions(
        statement_date=time_dimension(lambda t: t.statement_date),
    )

    # Check that the dimension is marked as event timestamp
    json_def = st.json_definition
    assert "statement_date" in json_def["dimensions"]
    assert "statement_date" in json_def["event_timestamp"]
    assert json_def["dimensions"]["statement_date"]["is_event_timestamp"] is True


def test_time_dimension_with_grain():
    """Test that time_dimension with grain sets is_time_dimension and smallest_time_grain."""
    con = ibis.duckdb.connect(":memory:")
    data = ibis.memtable(
        {
            "statement_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "value": [100, 200, 300],
        }
    )
    tbl = con.create_table("test", data)

    # Create semantic table with time dimension and grain
    st = to_semantic_table(tbl, name="test_table").with_dimensions(
        statement_date=time_dimension(
            lambda t: t.statement_date,
            description="Statement date for balance features",
            smallest_time_grain="TIME_GRAIN_DAY",
        ),
    )

    # Check that grain is stored
    json_def = st.json_definition
    assert json_def["dimensions"]["statement_date"]["is_event_timestamp"] is True
    assert json_def["dimensions"]["statement_date"]["smallest_time_grain"] == "TIME_GRAIN_DAY"
    assert json_def["dimensions"]["statement_date"]["description"] == "Statement date for balance features"
    # Should be in both event_timestamp and time_dimensions
    assert "statement_date" in json_def["event_timestamp"]
    assert "statement_date" in json_def["time_dimensions"]


def test_combined_entity_and_time_dimensions():
    """Test using both entity_dimension and time_dimension together."""
    con = ibis.duckdb.connect(":memory:")
    data = ibis.memtable(
        {
            "business_id": [1, 2, 3],
            "statement_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "balance": [1000.0, 2000.0, 3000.0],
        }
    )
    tbl = con.create_table("balance", data)

    # Create semantic table with both entity and time dimensions
    st = (
        to_semantic_table(tbl, name="balance_features_90d")
        .with_dimensions(
            business_id=entity_dimension(lambda t: t.business_id),
            statement_date=time_dimension(
                lambda t: t.statement_date,
                smallest_time_grain="TIME_GRAIN_DAY",
            ),
        )
        .with_measures(
            total_balance=lambda t: t.balance.sum(),
        )
    )

    # Check JSON definition
    json_def = st.json_definition

    # Check entity dimension
    assert "business_id" in json_def["entity_dimensions"]
    assert json_def["dimensions"]["business_id"]["is_entity"] is True

    # Check event timestamp dimension
    assert "statement_date" in json_def["event_timestamp"]
    assert json_def["dimensions"]["statement_date"]["is_event_timestamp"] is True
    assert json_def["dimensions"]["statement_date"]["smallest_time_grain"] == "TIME_GRAIN_DAY"

    # Check measures
    assert "total_balance" in json_def["measures"]


def test_multiple_entity_dimensions():
    """Test that multiple entity dimensions can be defined."""
    con = ibis.duckdb.connect(":memory:")
    data = ibis.memtable(
        {
            "business_id": [1, 2, 3],
            "user_id": [101, 102, 103],
            "value": [100, 200, 300],
        }
    )
    tbl = con.create_table("test", data)

    # Create semantic table with multiple entity dimensions
    st = to_semantic_table(tbl, name="test_table").with_dimensions(
        business_id=entity_dimension(lambda t: t.business_id, "Business identifier"),
        user_id=entity_dimension(lambda t: t.user_id, "User identifier"),
    )

    # Check that both are in entity_dimensions
    json_def = st.json_definition
    assert "business_id" in json_def["entity_dimensions"]
    assert "user_id" in json_def["entity_dimensions"]
    assert json_def["dimensions"]["business_id"]["is_entity"] is True
    assert json_def["dimensions"]["user_id"]["is_entity"] is True


def test_regular_dimension_vs_entity_dimension():
    """Test that regular dimensions don't have is_entity flag."""
    con = ibis.duckdb.connect(":memory:")
    data = ibis.memtable(
        {
            "business_id": [1, 2, 3],
            "category": ["A", "B", "C"],
            "value": [100, 200, 300],
        }
    )
    tbl = con.create_table("test", data)

    # Create semantic table with both entity and regular dimensions
    st = to_semantic_table(tbl, name="test_table").with_dimensions(
        business_id=entity_dimension(lambda t: t.business_id),
        category=lambda t: t.category,  # Regular dimension
    )

    # Check entity dimension
    json_def = st.json_definition
    assert json_def["dimensions"]["business_id"]["is_entity"] is True
    assert "business_id" in json_def["entity_dimensions"]

    # Check regular dimension
    assert "category" in json_def["dimensions"]
    assert "is_entity" not in json_def["dimensions"]["category"]
    assert "category" not in json_def["entity_dimensions"]
