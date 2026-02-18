"""
Test Ibis deferred API support (using _) for measure and dimension definitions.

The deferred API allows writing expressions without explicitly using lambda:
- Instead of: lambda t: t.distance.sum()
- You can use: _.distance.sum()
"""

import ibis
import pandas as pd
import pytest
from ibis import _

from boring_semantic_layer import to_semantic_table


def test_deferred_in_with_measures():
    """Test using deferred expressions in with_measures()."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    f_tbl = con.create_table("flights", flights)

    # Define measures using deferred API
    flights_st = to_semantic_table(f_tbl, "flights").with_measures(
        flight_count=_.count(),  # No lambda!
        total_distance=_.distance.sum(),
    )

    df = flights_st.group_by("carrier").aggregate("flight_count", "total_distance").execute()
    assert df.flight_count.sum() == 3
    assert df.total_distance.sum() == 600


def test_deferred_in_with_dimensions():
    """Test using deferred expressions in with_dimensions()."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame(
        {
            "carrier": ["AA", "AA", "UA"],
            "dep_time": pd.date_range("2024-01-01", periods=3),
        },
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
            pct=lambda t: t.flight_count / t.all(t.flight_count),
        )
    )

    df = flights_st.group_by("carrier").aggregate("pct").execute()
    assert pytest.approx(df.pct.sum()) == 1.0


def test_deferred_with_complex_expression():
    """Test deferred API with complex expressions.

    For complex expressions, define base measures first, then combine them.
    """
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame(
        {
            "carrier": ["AA", "AA", "UA"],
            "distance": [100, 200, 300],
            "delay": [10, 20, 30],
        },
    )
    f_tbl = con.create_table("flights", flights)

    # Define base measures with _, then combine them
    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_measures(
            total_distance=_.distance.sum(),
            total_delay=_.delay.sum(),
        )
        .with_measures(
            # Combine the base measures
            total_delay_distance=lambda t: t.total_distance + t.total_delay,
        )
    )

    df = flights_st.group_by("carrier").aggregate("total_delay_distance").execute()
    assert df.total_delay_distance.sum() == 660  # (100+200+300) + (10+20+30)


def test_deferred_with_conditional():
    """Test deferred API with conditional expressions."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    f_tbl = con.create_table("flights", flights)

    flights_st = to_semantic_table(f_tbl, "flights").with_measures(
        # Conditional with deferred
        long_flight_count=(_.distance > 150).sum(),
    )

    df = flights_st.group_by("carrier").aggregate("long_flight_count").execute()
    assert df.long_flight_count.sum() == 2


def test_deferred_reference_to_measure_now_supported():
    """
    Test that deferred expressions CAN reference measures directly!

    Deferred expressions now resolve against MeasureScope, which means
    _.measure_name returns a MeasureRef, just like lambda t: t.measure_name.
    """
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"]})
    f_tbl = con.create_table("flights", flights)

    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_measures(
            flight_count=_.count(),  # Define base measure with deferred
        )
        .with_measures(
            # Reference measure using deferred!
            double_count=_.flight_count * 2,
        )
    )

    df = flights_st.group_by("carrier").aggregate("flight_count", "double_count").execute()
    assert all(df.double_count == df.flight_count * 2)


def test_deferred_with_measure_references_and_operations():
    """Test deferred expressions with measure references and arithmetic."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    f_tbl = con.create_table("flights", flights)

    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_measures(
            flight_count=_.count(),
            total_distance=_.distance.sum(),
        )
        .with_measures(
            # Use deferred to reference measures and do math!
            avg_distance_per_flight=_.total_distance / _.flight_count,
        )
    )

    df = flights_st.group_by("carrier").aggregate("avg_distance_per_flight").execute()
    # AA carrier: (100 + 200) / 2 = 150
    # UA carrier: 300 / 1 = 300
    assert pytest.approx(df[df.carrier == "AA"]["avg_distance_per_flight"].iloc[0]) == 150
    assert pytest.approx(df[df.carrier == "UA"]["avg_distance_per_flight"].iloc[0]) == 300


def test_deferred_documentation_example():
    """Example showing deferred can now do everything lambdas can do."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
    f_tbl = con.create_table("flights", flights)

    flights_st = (
        to_semantic_table(f_tbl, "flights")
        # Use deferred for everything - column operations AND measure references!
        .with_measures(
            flight_count=_.count(),
            total_distance=_.distance.sum(),
            avg_distance=_.distance.mean(),
        )
        # Deferred can now reference measures too!
        .with_measures(
            # However, t.all() still requires lambda because it's a method on MeasureScope
            pct_of_flights=lambda t: t.flight_count / t.all(t.flight_count),
            # But measure references work with deferred
            distance_per_flight=_.total_distance / _.flight_count,
        )
    )

    df = flights_st.group_by("carrier").aggregate("pct_of_flights", "distance_per_flight").execute()
    assert pytest.approx(df.pct_of_flights.sum()) == 1.0
    assert len(df) == 2  # 2 carriers


def test_deferred_bracket_notation_for_measures():
    """Test that deferred works with bracket notation for measures."""
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"]})
    f_tbl = con.create_table("flights", flights)

    # Note: Deferred doesn't support bracket notation directly (_["col"] doesn't work in Ibis)
    # But we can still use it in lambdas
    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_measures(
            flight_count=_.count(),
        )
        .with_measures(
            # Mix deferred and lambda with bracket notation
            double=lambda t: t["flight_count"] * 2,
        )
    )

    df = flights_st.group_by("carrier").aggregate("flight_count", "double").execute()
    assert all(df.double == df.flight_count * 2)


def test_deferred_comprehensive_workflow():
    """
    Comprehensive test using deferred API in ALL applicable methods in a single workflow:
    - with_dimensions() ✓
    - with_measures() ✓
    - filter() ✓
    - aggregate() with inline measures ✓

    This verifies deferred works everywhere applicable.
    Note: mutate() with deferred is tested separately in test_deferred_in_mutate()
    """
    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame(
        {
            "carrier": ["AA", "AA", "UA", "UA", "DL"],
            "distance": [100, 200, 300, 400, 150],
            "delay": [10, 20, 30, 40, 5],
            "dep_time": pd.date_range("2024-01-01", periods=5),
        },
    )
    f_tbl = con.create_table("flights", flights)

    # Use deferred in with_dimensions()
    flights_st = (
        to_semantic_table(f_tbl, "flights")
        .with_dimensions(
            dep_month=_.dep_time.truncate("M"),  # Deferred!
            dep_year=_.dep_time.truncate("Y"),  # Deferred!
        )
        # Use deferred in with_measures()
        .with_measures(
            flight_count=_.count(),  # Deferred!
            total_distance=_.distance.sum(),  # Deferred!
            avg_delay=_.delay.mean(),  # Deferred!
        )
        # Use deferred to define calculated measures
        .with_measures(
            avg_distance_per_flight=_.total_distance / _.flight_count,  # Deferred measure refs!
        )
        # Use deferred in filter()
        .filter(_.distance > 100)  # Deferred! (filters out the 100 distance flight)
    )

    # Use deferred in aggregate() inline
    df = (
        flights_st.group_by("carrier")
        .aggregate(
            "flight_count",
            "total_distance",
            "avg_distance_per_flight",
            # Define new measure inline with deferred
            long_flights=(_.distance > 200).sum(),  # Deferred! (count flights > 200)
        )
        .execute()
    )

    # Verify results
    assert len(df) == 3  # AA, UA, DL (after filter)
    assert df.flight_count.sum() == 4  # 5 flights - 1 filtered = 4
    assert df.total_distance.sum() == 1050  # 200+300+400+150 (100 filtered out)
    # AA has 1 flight (200) - 0 long, UA has 2 flights (300, 400) - 2 long, DL has 1 flight (150) - 0 long
    assert df.long_flights.sum() == 2


def test_aggregation_expr_method_chaining():
    """Test that AggregationExpr supports method chaining for post-aggregation operations.

    This allows patterns like t.time.max().epoch_seconds() when defining base measures.
    """
    con = ibis.duckdb.connect(":memory:")
    events = pd.DataFrame(
        {
            "session_id": [1, 1, 2],
            "event_time": pd.to_datetime(
                ["2023-01-01 10:00", "2023-01-01 10:10", "2023-01-01 11:00"]
            ),
        },
    )
    events_tbl = con.create_table("events", events)

    # Method chaining works when defining base measures
    events_st = (
        to_semantic_table(events_tbl, "events")
        .with_measures(
            # ✅ Method chaining: t.event_time.max().epoch_seconds()
            max_time_epoch=lambda t: t.event_time.max().epoch_seconds(),
            min_time_epoch=lambda t: t.event_time.min().epoch_seconds(),
        )
        .with_measures(
            # Combine base measures
            duration_seconds=lambda t: t.max_time_epoch - t.min_time_epoch,
        )
    )

    df = events_st.group_by("session_id").aggregate("duration_seconds").execute()

    # Session 1: 10 minutes = 600 seconds
    assert df[df.session_id == 1]["duration_seconds"].values[0] == 600
    # Session 2: 0 seconds (single event)
    assert df[df.session_id == 2]["duration_seconds"].values[0] == 0


# ---------------------------------------------------------------------------
# Tests for Deferred support in group_by() and aggregate() positional args
# ---------------------------------------------------------------------------


class TestDeferredGroupBy:
    """Test using _.dimension in group_by() instead of string names."""

    def test_group_by_single_deferred(self):
        con = ibis.duckdb.connect(":memory:")
        flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
        f_tbl = con.create_table("flights", flights)

        flights_st = to_semantic_table(f_tbl, "flights").with_measures(
            flight_count=_.count(),
        )
        df = flights_st.group_by(_.carrier).aggregate("flight_count").execute()
        assert df.flight_count.sum() == 3
        assert set(df.carrier) == {"AA", "UA"}

    def test_group_by_multiple_deferred(self):
        con = ibis.duckdb.connect(":memory:")
        flights = pd.DataFrame(
            {"carrier": ["AA", "AA", "UA"], "origin": ["JFK", "LAX", "JFK"], "distance": [100, 200, 300]}
        )
        f_tbl = con.create_table("flights", flights)

        flights_st = to_semantic_table(f_tbl, "flights").with_measures(
            flight_count=_.count(),
        )
        df = flights_st.group_by(_.carrier, _.origin).aggregate("flight_count").execute()
        assert df.flight_count.sum() == 3
        assert len(df) == 3

    def test_group_by_mixed_str_and_deferred(self):
        con = ibis.duckdb.connect(":memory:")
        flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "origin": ["JFK", "LAX", "JFK"]})
        f_tbl = con.create_table("flights", flights)

        flights_st = to_semantic_table(f_tbl, "flights").with_measures(
            flight_count=_.count(),
        )
        df = flights_st.group_by("carrier", _.origin).aggregate("flight_count").execute()
        assert len(df) == 3

    def test_group_by_rejects_complex_deferred(self):
        con = ibis.duckdb.connect(":memory:")
        flights = pd.DataFrame({"carrier": ["AA"], "distance": [100]})
        f_tbl = con.create_table("flights", flights)

        flights_st = to_semantic_table(f_tbl, "flights")
        with pytest.raises(TypeError, match="simple Deferred"):
            flights_st.group_by(_.distance.sum())


class TestDeferredAggregate:
    """Test using _.measure_name in aggregate() positional args."""

    def test_aggregate_single_deferred_measure(self):
        con = ibis.duckdb.connect(":memory:")
        flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
        f_tbl = con.create_table("flights", flights)

        flights_st = to_semantic_table(f_tbl, "flights").with_measures(
            flight_count=_.count(),
        )
        df = flights_st.group_by("carrier").aggregate(_.flight_count).execute()
        assert df.flight_count.sum() == 3

    def test_aggregate_multiple_deferred_measures(self):
        con = ibis.duckdb.connect(":memory:")
        flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
        f_tbl = con.create_table("flights", flights)

        flights_st = to_semantic_table(f_tbl, "flights").with_measures(
            flight_count=_.count(),
            total_distance=_.distance.sum(),
        )
        df = flights_st.group_by("carrier").aggregate(_.flight_count, _.total_distance).execute()
        assert df.flight_count.sum() == 3
        assert df.total_distance.sum() == 600

    def test_aggregate_mixed_str_and_deferred(self):
        con = ibis.duckdb.connect(":memory:")
        flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
        f_tbl = con.create_table("flights", flights)

        flights_st = to_semantic_table(f_tbl, "flights").with_measures(
            flight_count=_.count(),
            total_distance=_.distance.sum(),
        )
        df = flights_st.group_by("carrier").aggregate("flight_count", _.total_distance).execute()
        assert df.flight_count.sum() == 3
        assert df.total_distance.sum() == 600

    def test_aggregate_without_group_by_with_deferred(self):
        con = ibis.duckdb.connect(":memory:")
        flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
        f_tbl = con.create_table("flights", flights)

        flights_st = to_semantic_table(f_tbl, "flights").with_measures(
            flight_count=_.count(),
        )
        df = flights_st.aggregate(_.flight_count).execute()
        assert df.flight_count.iloc[0] == 3


class TestDeferredEndToEnd:
    """End-to-end tests using Deferred in both group_by and aggregate."""

    def test_full_deferred_workflow(self):
        """The target API: group_by(_.dim).aggregate(_.measure)."""
        con = ibis.duckdb.connect(":memory:")
        flights = pd.DataFrame({"carrier": ["AA", "AA", "UA"], "distance": [100, 200, 300]})
        f_tbl = con.create_table("flights", flights)

        flights_st = to_semantic_table(f_tbl, "flights").with_measures(
            flight_count=_.count(),
            total_distance=_.distance.sum(),
        )
        df = flights_st.group_by(_.carrier).aggregate(_.flight_count, _.total_distance).execute()
        assert df.flight_count.sum() == 3
        assert df.total_distance.sum() == 600
        assert set(df.carrier) == {"AA", "UA"}


class TestNormalizeToName:
    """Unit tests for the _normalize_to_name helper."""

    def test_string_passthrough(self):
        from boring_semantic_layer.ops import _normalize_to_name

        assert _normalize_to_name("origin") == "origin"

    def test_simple_deferred(self):
        from boring_semantic_layer.ops import _normalize_to_name

        assert _normalize_to_name(_.origin) == "origin"
        assert _normalize_to_name(_.flight_count) == "flight_count"

    def test_complex_deferred_raises(self):
        from boring_semantic_layer.ops import _normalize_to_name

        with pytest.raises(TypeError):
            _normalize_to_name(_.distance.sum())

    def test_invalid_type_raises(self):
        from boring_semantic_layer.ops import _normalize_to_name

        with pytest.raises(TypeError):
            _normalize_to_name(42)
