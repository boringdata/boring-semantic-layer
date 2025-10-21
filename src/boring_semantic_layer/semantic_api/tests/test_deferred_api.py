"""
Test Ibis deferred API support (using _) for measure and dimension definitions.

The deferred API allows writing expressions without explicitly using lambda:
- Instead of: lambda t: t.distance.sum()
- You can use: _.distance.sum()
"""

import pandas as pd
import ibis
from ibis import _
import pytest

from boring_semantic_layer.semantic_api import to_semantic_table


def test_deferred_in_with_measures():
    """Test using deferred expressions in with_measures()."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    f_tbl = con.create_table("flights", flights)

    # Define measures using deferred API
    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_measures(
            flight_count=_.count(),  # No lambda!
            total_distance=_.distance.sum(),
        )
    )

    df = flights_st.group_by("carrier").aggregate("flight_count", "total_distance").execute()
    assert df.flight_count.sum() == 3
    assert df.total_distance.sum() == 600


def test_deferred_in_with_dimensions():
    """Test using deferred expressions in with_dimensions()."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame(
        {"carrier": ["AA", "AA", "UA"], "dep_time": pd.date_range("2024-01-01", periods=3)}
    )
    f_tbl = con.create_table("flights", flights)

    # Define dimensions using deferred API
    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_dimensions(
            dep_month=_.dep_time.truncate("M"),  # No lambda!
        )
        .with_measures(flight_count=_.count())
    )

    df = flights_st.group_by("dep_month").aggregate("flight_count").execute()
    assert len(df) > 0


def test_deferred_in_filter():
    """Test using deferred expressions in filter()."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    f_tbl = con.create_table("flights", flights)

    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_measures(flight_count=_.count())
        .filter(_.distance > 150)  # No lambda!
    )

    df = flights_st.group_by("carrier").aggregate("flight_count").execute()
    assert df.flight_count.sum() == 2  # Only 2 flights with distance > 150


def test_deferred_in_mutate():
    """Test using deferred expressions in mutate()."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"]})
    f_tbl = con.create_table("flights", flights)

    result = (
        to_semantic_table(f_tbl, "flights")
        .with_measures(flight_count=_.count())
        .group_by("carrier")
        .aggregate("flight_count")
        .mutate(double_count=_.flight_count * 2)  # No lambda!
    )

    df = result.execute()
    assert all(df.double_count == df.flight_count * 2)


def test_deferred_in_inline_aggregate():
    """Test using deferred expressions in inline aggregate() definitions."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    f_tbl = con.create_table("flights", flights)

    flights_st = to_semantic_table(f_tbl, "flights")

    # Define measures inline using deferred API
    df = (
        flights_st.group_by("carrier")
        .aggregate(
            flight_count=_.count(),  # No lambda!
            total_distance=_.distance.sum(),
            avg_distance=_.distance.mean(),
        )
        .execute()
    )

    assert df.flight_count.sum() == 3
    assert df.total_distance.sum() == 600
    # Mean of per-carrier averages: (150 + 300) / 2 = 225
    assert pytest.approx(df.avg_distance.mean()) == 225


def test_mixed_deferred_and_lambda():
    """Test mixing deferred expressions and lambdas in the same query."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    f_tbl = con.create_table("flights", flights)

    flights_st = (
        to_semantic_table(f_tbl, "flights")
        # Mix deferred and lambda
        .with_measures(
            flight_count=_.count(),  # Deferred
            total_distance=lambda t: t.distance.sum(),  # Lambda
        )
        .with_measures(
            # Reference existing measures - must use lambda for t.all()
            pct=lambda t: t.flight_count / t.all(t.flight_count)
        )
    )

    df = flights_st.group_by("carrier").aggregate("pct").execute()
    assert pytest.approx(df.pct.sum()) == 1.0


def test_deferred_with_complex_expression():
    """Test deferred API with complex expressions."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame(
        {"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300], "delay": [10, 20, 30]}
    )
    f_tbl = con.create_table("flights", flights)

    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_measures(
            # Complex deferred expression
            total_delay_distance=_.distance.sum() + _.delay.sum(),
        )
    )

    df = flights_st.group_by("carrier").aggregate("total_delay_distance").execute()
    assert df.total_delay_distance.sum() == 660  # (100+200+300) + (10+20+30)


def test_deferred_with_conditional():
    """Test deferred API with conditional expressions."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    f_tbl = con.create_table("flights", flights)

    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_measures(
            # Conditional with deferred
            long_flight_count=(_.distance > 150).sum(),
        )
    )

    df = flights_st.group_by("carrier").aggregate("long_flight_count").execute()
    assert df.long_flight_count.sum() == 2


def test_deferred_reference_to_measure_not_supported():
    """
    Test that deferred expressions cannot reference measures directly.

    This is a known limitation - deferred expressions resolve against the table,
    not the MeasureScope, so they can't access measure references.
    For measure references, you must use lambdas with t.measure_name or t.all().
    """
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"]})
    f_tbl = con.create_table("flights", flights)

    flights_st = to_semantic_table(f_tbl, "flights").with_measures(
        flight_count=_.count()
    )

    # This should raise an error because deferred can't access measures
    # We need to use lambda for measure references
    with pytest.raises(Exception):  # Will raise AttributeError or similar
        flights_st.with_measures(
            pct=_.flight_count / _.all(_.flight_count)  # This won't work!
        )


def test_deferred_documentation_example():
    """Example showing when to use deferred vs lambda."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    f_tbl = con.create_table("flights", flights)

    flights_st = (
        to_semantic_table(f_tbl, "flights")
        # Use deferred for simple column operations
        .with_measures(
            flight_count=_.count(),
            total_distance=_.distance.sum(),
            avg_distance=_.distance.mean(),
        )
        # Use lambda for measure references and t.all()
        .with_measures(
            pct_of_flights=lambda t: t.flight_count / t.all(t.flight_count),
            pct_of_distance=lambda t: t.total_distance / t.all(t.total_distance),
        )
    )

    df = flights_st.group_by("carrier").aggregate("pct_of_flights", "pct_of_distance").execute()
    assert pytest.approx(df.pct_of_flights.sum()) == 1.0
    assert pytest.approx(df.pct_of_distance.sum()) == 1.0
