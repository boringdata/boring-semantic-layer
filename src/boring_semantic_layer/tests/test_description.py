"""Tests for description parameter on semantic tables."""

import ibis

from boring_semantic_layer import to_semantic_table


def test_to_semantic_table_with_description():
    """Test that to_semantic_table accepts and stores description parameter."""
    con = ibis.duckdb.connect(":memory:")
    data = ibis.memtable({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    tbl = con.create_table("test", data)

    # Create semantic table with description
    st = to_semantic_table(tbl, name="test_table", description="This is a test table")

    # Check that description is stored in json_definition
    assert st.json_definition["description"] == "This is a test table"
    assert st.json_definition["name"] == "test_table"
    assert st.description == "This is a test table"


def test_to_semantic_table_without_description():
    """Test that to_semantic_table works without description (backward compatibility)."""
    con = ibis.duckdb.connect(":memory:")
    data = ibis.memtable({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    tbl = con.create_table("test", data)

    # Create semantic table without description
    st = to_semantic_table(tbl, name="test_table")

    # Check that description is not present in json_definition
    assert "description" not in st.json_definition
    assert st.json_definition["name"] == "test_table"
    assert st.description is None


def test_semantic_table_description_with_dimensions_and_measures():
    """Test description works alongside dimensions and measures."""
    con = ibis.duckdb.connect(":memory:")
    data = ibis.memtable({"origin": ["JFK", "LAX"], "distance": [100, 200]})
    tbl = con.create_table("flights", data)

    # Create semantic table with description, dimensions, and measures
    st = (
        to_semantic_table(tbl, name="flights", description="Flight data")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(total_distance=lambda t: t.distance.sum())
    )

    # Check everything is present
    json_def = st.json_definition
    assert json_def["description"] == "Flight data"
    assert json_def["name"] == "flights"
    assert "origin" in json_def["dimensions"]
    assert "total_distance" in json_def["measures"]


def test_description_preserved_through_with_dimensions():
    """Test that description is preserved when adding dimensions."""
    con = ibis.duckdb.connect(":memory:")
    data = ibis.memtable({"a": [1, 2], "b": ["x", "y"]})
    tbl = con.create_table("test", data)

    st = to_semantic_table(tbl, name="test", description="Original description")
    st_with_dims = st.with_dimensions(a=lambda t: t.a)

    assert st_with_dims.description == "Original description"
    assert st_with_dims.json_definition["description"] == "Original description"


def test_description_preserved_through_with_measures():
    """Test that description is preserved when adding measures."""
    con = ibis.duckdb.connect(":memory:")
    data = ibis.memtable({"a": [1, 2], "b": ["x", "y"]})
    tbl = con.create_table("test", data)

    st = to_semantic_table(tbl, name="test", description="Original description")
    st_with_meas = st.with_measures(count=lambda t: t.count())

    assert st_with_meas.description == "Original description"
    assert st_with_meas.json_definition["description"] == "Original description"
