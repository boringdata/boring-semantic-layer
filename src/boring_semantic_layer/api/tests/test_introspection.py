"""
Test introspection capabilities of SemanticTable.

Tests the .dims and .measures properties that allow inspecting
what dimensions and measures are available on a semantic table.
"""
import pandas as pd
import ibis
import pytest

from boring_semantic_layer.api import to_semantic_table


def test_empty_semantic_table():
    """Test that an empty semantic table has no dims or measures."""
    con = ibis.duckdb.connect(":memory:")
    df = pd.DataFrame({"col1": [1, 2, 3]})
    tbl = con.create_table("test", df)

    st = to_semantic_table(tbl, "test")

    assert st.dims == []
    assert st.measures == []


def test_dims_property():
    """Test that dims property returns list of dimension names."""
    con = ibis.duckdb.connect(":memory:")
    df = pd.DataFrame({"carrier": ["AA", "UA"], "date": ["2024-01-01", "2024-01-02"]})
    tbl = con.create_table("flights", df)

    st = to_semantic_table(tbl, "flights").with_dimensions(
        carrier=lambda t: t.carrier,
        year=lambda t: t.date[:4],
        month=lambda t: t.date[5:7]
    )

    assert set(st.dims) == {"carrier", "year", "month"}


def test_measures_property_base_only():
    """Test measures property with only base measures."""
    con = ibis.duckdb.connect(":memory:")
    df = pd.DataFrame({"distance": [100, 200, 300]})
    tbl = con.create_table("flights", df)

    st = to_semantic_table(tbl, "flights").with_measures(
        flight_count=lambda t: t.count(),
        total_distance=lambda t: t.distance.sum(),
        avg_distance=lambda t: t.distance.mean()
    )

    assert set(st.measures) == {"flight_count", "total_distance", "avg_distance"}


def test_measures_property_calculated_only():
    """Test measures property with only calculated measures."""
    con = ibis.duckdb.connect(":memory:")
    df = pd.DataFrame({"distance": [100, 200, 300]})
    tbl = con.create_table("flights", df)

    st = (
        to_semantic_table(tbl, "flights")
        .with_measures(flight_count=lambda t: t.count())
        .with_measures(
            # These reference existing measures, so they're calculated measures
            doubled=lambda t: t.flight_count * 2,
            pct=lambda t: t.flight_count / t.all(t.flight_count)
        )
    )

    # Should have all three measures
    assert set(st.measures) == {"flight_count", "doubled", "pct"}


def test_measures_property_mixed():
    """Test measures property with both base and calculated measures."""
    con = ibis.duckdb.connect(":memory:")
    df = pd.DataFrame({"distance": [100, 200, 300]})
    tbl = con.create_table("flights", df)

    st = (
        to_semantic_table(tbl, "flights")
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum()
        )
        .with_measures(
            avg_distance_per_flight=lambda t: t.total_distance / t.flight_count,
            pct=lambda t: t.flight_count / t.all(t.flight_count)
        )
    )

    assert set(st.measures) == {
        "flight_count",
        "total_distance",
        "avg_distance_per_flight",
        "pct"
    }


def test_dims_and_measures_together():
    """Test that both dims and measures can be inspected together."""
    con = ibis.duckdb.connect(":memory:")
    df = pd.DataFrame({
        "carrier": ["AA", "UA"],
        "distance": [100, 200]
    })
    tbl = con.create_table("flights", df)

    st = (
        to_semantic_table(tbl, "flights")
        .with_dimensions(carrier=lambda t: t.carrier)
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum()
        )
    )

    assert st.dims == ["carrier"]
    assert set(st.measures) == {"flight_count", "total_distance"}


def test_dims_after_join():
    """Test that dims property works correctly after joins with prefixing."""
    con = ibis.duckdb.connect(":memory:")
    flights_df = pd.DataFrame({"carrier": ["AA", "UA"]})
    carriers_df = pd.DataFrame({"code": ["AA", "UA"], "name": ["American", "United"]})

    f_tbl = con.create_table("flights", flights_df)
    c_tbl = con.create_table("carriers", carriers_df)

    flights_st = to_semantic_table(f_tbl, "flights").with_dimensions(
        carrier=lambda t: t.carrier
    )
    carriers_st = to_semantic_table(c_tbl, "carriers").with_dimensions(
        code=lambda t: t.code,
        name=lambda t: t.name
    )

    joined = flights_st.join(carriers_st, on=lambda f, c: f.carrier == c.code)

    # After join, dimensions should be prefixed
    assert set(joined.dims) == {"flights__carrier", "carriers__code", "carriers__name"}


def test_measures_after_join():
    """Test that measures property works correctly after joins with prefixing."""
    con = ibis.duckdb.connect(":memory:")
    flights_df = pd.DataFrame({"carrier": ["AA", "UA"]})
    carriers_df = pd.DataFrame({"code": ["AA", "UA"]})

    f_tbl = con.create_table("flights", flights_df)
    c_tbl = con.create_table("carriers", carriers_df)

    flights_st = to_semantic_table(f_tbl, "flights").with_measures(
        flight_count=lambda t: t.count()
    )
    carriers_st = to_semantic_table(c_tbl, "carriers").with_measures(
        carrier_count=lambda t: t.count()
    )

    joined = flights_st.join(carriers_st, on=lambda f, c: f.carrier == c.code)

    # After join, measures should be prefixed
    assert set(joined.measures) == {"flights__flight_count", "carriers__carrier_count"}


def test_measures_after_aggregate():
    """Test that measures property is empty after aggregation (measures become columns)."""
    con = ibis.duckdb.connect(":memory:")
    df = pd.DataFrame({"carrier": ["AA", "AA", "UA"]})
    tbl = con.create_table("flights", df)

    st = to_semantic_table(tbl, "flights").with_measures(
        flight_count=lambda t: t.count()
    )

    # Before aggregation, we have measures
    assert st.measures == ["flight_count"]

    # After aggregation, measures are materialized as columns
    aggregated = st.group_by("carrier").aggregate("flight_count")
    assert aggregated.measures == []  # No semantic measures anymore


def test_chaining_maintains_introspection():
    """Test that dims and measures are updated correctly through method chaining."""
    con = ibis.duckdb.connect(":memory:")
    df = pd.DataFrame({"carrier": ["AA", "UA"], "distance": [100, 200]})
    tbl = con.create_table("flights", df)

    st = to_semantic_table(tbl, "flights")
    assert st.dims == []
    assert st.measures == []

    st = st.with_dimensions(carrier=lambda t: t.carrier)
    assert st.dims == ["carrier"]
    assert st.measures == []

    st = st.with_measures(flight_count=lambda t: t.count())
    assert st.dims == ["carrier"]
    assert st.measures == ["flight_count"]

    st = st.with_measures(total_distance=lambda t: t.distance.sum())
    assert st.dims == ["carrier"]
    assert set(st.measures) == {"flight_count", "total_distance"}


def test_introspection_with_inline_measures():
    """Test that inline measures defined in aggregate() are included in introspection."""
    con = ibis.duckdb.connect(":memory:")
    df = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    tbl = con.create_table("flights", df)

    st = (
        to_semantic_table(tbl, "flights")
        .with_measures(flight_count=lambda t: t.count())
    )

    # Initially only flight_count
    assert st.measures == ["flight_count"]

    # After defining inline measures, they should be included
    # Note: We need to define them via with_measures, not in aggregate()
    # because aggregate() is terminal (returns ibis expression after materialization)
    st = st.with_measures(
        total_distance=lambda t: t.distance.sum(),
        pct=lambda t: t.flight_count / t.all(t.flight_count)
    )

    assert set(st.measures) == {"flight_count", "total_distance", "pct"}


def test_introspection_preserves_definition_order():
    """Test that dims preserve definition order."""
    con = ibis.duckdb.connect(":memory:")
    df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
    tbl = con.create_table("test", df)

    st = to_semantic_table(tbl, "test").with_dimensions(
        dim_a=lambda t: t.a,
        dim_b=lambda t: t.b,
        dim_c=lambda t: t.c
    )

    # Dict keys preserve insertion order in Python 3.7+
    assert st.dims == ["dim_a", "dim_b", "dim_c"]
