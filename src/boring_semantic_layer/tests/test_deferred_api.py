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


# ---------------------------------------------------------------------------
# Tests for _normalize_join_predicate and string/Deferred join shorthands
# ---------------------------------------------------------------------------


class TestNormalizeJoinPredicate:
    """Unit tests for the _normalize_join_predicate helper."""

    def test_string_produces_callable(self):
        from boring_semantic_layer.ops import _normalize_join_predicate

        pred = _normalize_join_predicate("customer_id")
        assert callable(pred)

    def test_deferred_produces_callable(self):
        from boring_semantic_layer.ops import _normalize_join_predicate

        pred = _normalize_join_predicate(_.customer_id)
        assert callable(pred)

    def test_list_of_strings_produces_callable(self):
        from boring_semantic_layer.ops import _normalize_join_predicate

        pred = _normalize_join_predicate(["customer_id", "region"])
        assert callable(pred)

    def test_list_of_deferred_produces_callable(self):
        from boring_semantic_layer.ops import _normalize_join_predicate

        pred = _normalize_join_predicate([_.customer_id, _.region])
        assert callable(pred)

    def test_mixed_list_produces_callable(self):
        from boring_semantic_layer.ops import _normalize_join_predicate

        pred = _normalize_join_predicate([_.customer_id, "region"])
        assert callable(pred)

    def test_callable_passthrough(self):
        from boring_semantic_layer.ops import _normalize_join_predicate

        fn = lambda l, r: l.x == r.x
        assert _normalize_join_predicate(fn) is fn

    def test_none_passthrough(self):
        from boring_semantic_layer.ops import _normalize_join_predicate

        assert _normalize_join_predicate(None) is None

    def test_invalid_type_raises(self):
        from boring_semantic_layer.ops import _normalize_join_predicate

        with pytest.raises(TypeError, match="join `on`"):
            _normalize_join_predicate(42)

    def test_complex_deferred_raises(self):
        from boring_semantic_layer.ops import _normalize_join_predicate

        with pytest.raises(TypeError, match="simple Deferred"):
            _normalize_join_predicate(_.distance.sum())


class TestJoinStringShorthand:
    """Integration tests for string/Deferred/list join predicates."""

    @pytest.fixture()
    def models(self):
        con = ibis.duckdb.connect(":memory:")
        orders = pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "customer_id": [10, 20, 10],
                "amount": [100, 200, 300],
            }
        )
        customers = pd.DataFrame(
            {
                "customer_id": [10, 20],
                "name": ["Alice", "Bob"],
            }
        )
        orders_tbl = con.create_table("orders", orders)
        customers_tbl = con.create_table("customers", customers)

        orders_st = (
            to_semantic_table(orders_tbl, "orders")
            .with_dimensions(
                order_id=lambda t: t.order_id,
                customer_id=lambda t: t.customer_id,
            )
            .with_measures(
                total_amount=_.amount.sum(),
                order_count=_.count(),
            )
        )
        customers_st = (
            to_semantic_table(customers_tbl, "customers")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                name=lambda t: t.name,
            )
            .with_measures(
                customer_count=_.count(),
            )
        )
        return orders_st, customers_st

    def test_join_one_string(self, models):
        orders_st, customers_st = models
        joined = orders_st.join_one(customers_st, on="customer_id")
        df = joined.group_by("customers.name").aggregate("orders.total_amount").execute()
        assert len(df) == 2
        assert df["orders.total_amount"].sum() == 600

    def test_join_one_deferred(self, models):
        orders_st, customers_st = models
        joined = orders_st.join_one(customers_st, on=_.customer_id)
        df = joined.group_by("customers.name").aggregate("orders.total_amount").execute()
        assert len(df) == 2
        assert df["orders.total_amount"].sum() == 600

    def test_join_one_lambda_still_works(self, models):
        orders_st, customers_st = models
        joined = orders_st.join_one(
            customers_st, on=lambda o, c: o.customer_id == c.customer_id
        )
        df = joined.group_by("customers.name").aggregate("orders.total_amount").execute()
        assert len(df) == 2
        assert df["orders.total_amount"].sum() == 600

    def test_join_many_string(self, models):
        orders_st, customers_st = models
        joined = customers_st.join_many(orders_st, on="customer_id")
        df = joined.group_by("customers.name").aggregate("orders.total_amount").execute()
        assert len(df) == 2
        assert df["orders.total_amount"].sum() == 600

    def test_join_many_deferred(self, models):
        orders_st, customers_st = models
        joined = customers_st.join_many(orders_st, on=_.customer_id)
        df = joined.group_by("customers.name").aggregate("orders.total_amount").execute()
        assert len(df) == 2
        assert df["orders.total_amount"].sum() == 600

    def test_join_one_list_of_strings(self):
        """Test compound equi-join with list of strings."""
        con = ibis.duckdb.connect(":memory:")
        left = pd.DataFrame(
            {"customer_id": [1, 2, 1], "region": ["US", "EU", "US"], "amount": [10, 20, 30]}
        )
        right = pd.DataFrame(
            {"customer_id": [1, 2], "region": ["US", "EU"], "label": ["a", "b"]}
        )
        left_tbl = con.create_table("left_t", left)
        right_tbl = con.create_table("right_t", right)

        left_st = (
            to_semantic_table(left_tbl, "left")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                region=lambda t: t.region,
            )
            .with_measures(total=_.amount.sum())
        )
        right_st = (
            to_semantic_table(right_tbl, "right")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                region=lambda t: t.region,
                label=lambda t: t.label,
            )
        )

        joined = left_st.join_one(right_st, on=["customer_id", "region"])
        df = joined.group_by("right.label").aggregate("left.total").execute()
        assert len(df) == 2
        assert df["left.total"].sum() == 60

    def test_join_one_list_of_deferred(self):
        """Test compound equi-join with list of Deferred."""
        con = ibis.duckdb.connect(":memory:")
        left = pd.DataFrame(
            {"customer_id": [1, 2, 1], "region": ["US", "EU", "US"], "amount": [10, 20, 30]}
        )
        right = pd.DataFrame(
            {"customer_id": [1, 2], "region": ["US", "EU"], "label": ["a", "b"]}
        )
        left_tbl = con.create_table("left_t", left)
        right_tbl = con.create_table("right_t", right)

        left_st = (
            to_semantic_table(left_tbl, "left")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                region=lambda t: t.region,
            )
            .with_measures(total=_.amount.sum())
        )
        right_st = (
            to_semantic_table(right_tbl, "right")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                region=lambda t: t.region,
                label=lambda t: t.label,
            )
        )

        joined = left_st.join_one(right_st, on=[_.customer_id, _.region])
        df = joined.group_by("right.label").aggregate("left.total").execute()
        assert len(df) == 2
        assert df["left.total"].sum() == 60

    def test_join_one_mixed_list(self):
        """Test compound equi-join with mixed list of string and Deferred."""
        con = ibis.duckdb.connect(":memory:")
        left = pd.DataFrame(
            {"customer_id": [1, 2, 1], "region": ["US", "EU", "US"], "amount": [10, 20, 30]}
        )
        right = pd.DataFrame(
            {"customer_id": [1, 2], "region": ["US", "EU"], "label": ["a", "b"]}
        )
        left_tbl = con.create_table("left_t", left)
        right_tbl = con.create_table("right_t", right)

        left_st = (
            to_semantic_table(left_tbl, "left")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                region=lambda t: t.region,
            )
            .with_measures(total=_.amount.sum())
        )
        right_st = (
            to_semantic_table(right_tbl, "right")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                region=lambda t: t.region,
                label=lambda t: t.label,
            )
        )

        joined = left_st.join_one(right_st, on=[_.customer_id, "region"])
        df = joined.group_by("right.label").aggregate("left.total").execute()
        assert len(df) == 2
        assert df["left.total"].sum() == 60


# ---------------------------------------------------------------------------
# Snowflake / snow-star schema tests with string & Deferred join shorthands
# ---------------------------------------------------------------------------


class TestSnowflakeSchema:
    """Multi-tier snowflake schema joins using string/Deferred shorthands.

    Schema modeled:

        sales (fact)
          |-- join_one --> customers (dim)
          |                  |-- join_one --> regions (dim)
          |-- join_one --> products (dim)
          |                  |-- join_one --> categories (dim)
          |-- join_one --> stores (dim)
    """

    @pytest.fixture()
    def snowflake(self):
        con = ibis.duckdb.connect(":memory:")

        # -- fact table --
        sales = con.create_table(
            "sales",
            pd.DataFrame(
                {
                    "sale_id": [1, 2, 3, 4, 5, 6],
                    "customer_id": [1, 2, 1, 3, 2, 3],
                    "product_id": [10, 10, 20, 20, 30, 30],
                    "store_id": [100, 100, 200, 200, 100, 200],
                    "amount": [50, 80, 120, 90, 60, 110],
                }
            ),
        )

        # -- first-level dims --
        customers = con.create_table(
            "customers",
            pd.DataFrame(
                {
                    "customer_id": [1, 2, 3],
                    "customer_name": ["Alice", "Bob", "Carol"],
                    "region_id": [1, 1, 2],
                }
            ),
        )
        products = con.create_table(
            "products",
            pd.DataFrame(
                {
                    "product_id": [10, 20, 30],
                    "product_name": ["Widget", "Gadget", "Gizmo"],
                    "category_id": [1, 1, 2],
                }
            ),
        )
        stores = con.create_table(
            "stores",
            pd.DataFrame(
                {
                    "store_id": [100, 200],
                    "store_name": ["Downtown", "Mall"],
                }
            ),
        )

        # -- second-level dims (snowflake arms) --
        regions = con.create_table(
            "regions",
            pd.DataFrame(
                {
                    "region_id": [1, 2],
                    "region_name": ["North", "South"],
                }
            ),
        )
        categories = con.create_table(
            "categories",
            pd.DataFrame(
                {
                    "category_id": [1, 2],
                    "category_name": ["Electronics", "Accessories"],
                }
            ),
        )

        # -- semantic models --
        sales_st = (
            to_semantic_table(sales, "sales")
            .with_dimensions(
                customer_id=lambda t: t.customer_id,
                product_id=lambda t: t.product_id,
                store_id=lambda t: t.store_id,
            )
            .with_measures(
                total_sales=_.amount.sum(),
                sale_count=_.count(),
            )
        )
        customers_st = to_semantic_table(customers, "customers").with_dimensions(
            customer_id=lambda t: t.customer_id,
            customer_name=lambda t: t.customer_name,
            region_id=lambda t: t.region_id,
        )
        products_st = to_semantic_table(products, "products").with_dimensions(
            product_id=lambda t: t.product_id,
            product_name=lambda t: t.product_name,
            category_id=lambda t: t.category_id,
        )
        stores_st = to_semantic_table(stores, "stores").with_dimensions(
            store_id=lambda t: t.store_id,
            store_name=lambda t: t.store_name,
        )
        regions_st = to_semantic_table(regions, "regions").with_dimensions(
            region_id=lambda t: t.region_id,
            region_name=lambda t: t.region_name,
        )
        categories_st = to_semantic_table(categories, "categories").with_dimensions(
            category_id=lambda t: t.category_id,
            category_name=lambda t: t.category_name,
        )

        return {
            "sales": sales_st,
            "customers": customers_st,
            "products": products_st,
            "stores": stores_st,
            "regions": regions_st,
            "categories": categories_st,
        }

    # -- star layer: fact + first-level dims --

    def test_three_arm_star_string(self, snowflake):
        """Fact joined to three dimension tables using string shorthand."""
        joined = (
            snowflake["sales"]
            .join_one(snowflake["customers"], on="customer_id")
            .join_one(snowflake["products"], on="product_id")
            .join_one(snowflake["stores"], on="store_id")
        )

        df = (
            joined
            .group_by("customers.customer_name")
            .aggregate("sales.total_sales")
            .execute()
        )
        assert set(df["customers.customer_name"]) == {"Alice", "Bob", "Carol"}
        assert df["sales.total_sales"].sum() == 510  # 50+80+120+90+60+110

    def test_three_arm_star_deferred(self, snowflake):
        """Same star join using Deferred shorthand."""
        joined = (
            snowflake["sales"]
            .join_one(snowflake["customers"], on=_.customer_id)
            .join_one(snowflake["products"], on=_.product_id)
            .join_one(snowflake["stores"], on=_.store_id)
        )

        df = (
            joined
            .group_by("stores.store_name")
            .aggregate("sales.total_sales", "sales.sale_count")
            .execute()
        )
        assert set(df["stores.store_name"]) == {"Downtown", "Mall"}
        assert df["sales.sale_count"].sum() == 6

    def test_star_group_by_two_dims(self, snowflake):
        """Group by dimensions from two different arms of the star."""
        joined = (
            snowflake["sales"]
            .join_one(snowflake["customers"], on="customer_id")
            .join_one(snowflake["products"], on="product_id")
        )

        df = (
            joined
            .group_by("customers.customer_name", "products.product_name")
            .aggregate("sales.total_sales")
            .execute()
        )
        # Alice bought Widget(50) and Gadget(120)
        alice_widget = df[
            (df["customers.customer_name"] == "Alice")
            & (df["products.product_name"] == "Widget")
        ]
        assert alice_widget["sales.total_sales"].iloc[0] == 50

    # -- snowflake layer: chain through second-level dims --

    def test_snowflake_customer_region_chain(self, snowflake):
        """sales -> customers -> regions (two-hop snowflake arm)."""
        joined = (
            snowflake["sales"]
            .join_one(snowflake["customers"], on="customer_id")
            .join_one(snowflake["regions"], on="region_id")
        )

        df = (
            joined
            .group_by("regions.region_name")
            .aggregate("sales.total_sales")
            .execute()
        )
        # North: Alice(1,2) + Bob(1,2) = customers 1,2 → sales 50+80+120+60 = 310
        # South: Carol(3) → sales 90+110 = 200
        assert set(df["regions.region_name"]) == {"North", "South"}
        north = df[df["regions.region_name"] == "North"]["sales.total_sales"].iloc[0]
        south = df[df["regions.region_name"] == "South"]["sales.total_sales"].iloc[0]
        assert north == 310
        assert south == 200

    def test_snowflake_product_category_chain(self, snowflake):
        """sales -> products -> categories (two-hop snowflake arm)."""
        joined = (
            snowflake["sales"]
            .join_one(snowflake["products"], on=_.product_id)
            .join_one(snowflake["categories"], on=_.category_id)
        )

        df = (
            joined
            .group_by("categories.category_name")
            .aggregate("sales.total_sales")
            .execute()
        )
        # Electronics (Widget 10, Gadget 20): sales 50+80+120+90 = 340
        # Accessories (Gizmo 30): sales 60+110 = 170
        assert set(df["categories.category_name"]) == {"Electronics", "Accessories"}
        electronics = df[df["categories.category_name"] == "Electronics"]["sales.total_sales"].iloc[0]
        accessories = df[df["categories.category_name"] == "Accessories"]["sales.total_sales"].iloc[0]
        assert electronics == 340
        assert accessories == 170

    def test_full_snowflake_five_tables(self, snowflake):
        """Full snowflake: fact + 2 dim arms each extended one level deeper.

        sales -> customers -> regions
              -> products  -> categories
        """
        joined = (
            snowflake["sales"]
            .join_one(snowflake["customers"], on="customer_id")
            .join_one(snowflake["regions"], on="region_id")
            .join_one(snowflake["products"], on="product_id")
            .join_one(snowflake["categories"], on="category_id")
        )

        df = (
            joined
            .group_by("regions.region_name", "categories.category_name")
            .aggregate("sales.total_sales", "sales.sale_count")
            .execute()
        )
        # 4 combos: North/Electronics, North/Accessories, South/Electronics, South/Accessories
        assert len(df) == 4
        assert df["sales.total_sales"].sum() == 510
        assert df["sales.sale_count"].sum() == 6

    def test_full_snowflake_six_tables(self, snowflake):
        """All six tables: fact + 3 first-level dims + 2 second-level dims."""
        joined = (
            snowflake["sales"]
            .join_one(snowflake["customers"], on=_.customer_id)
            .join_one(snowflake["regions"], on=_.region_id)
            .join_one(snowflake["products"], on=_.product_id)
            .join_one(snowflake["categories"], on=_.category_id)
            .join_one(snowflake["stores"], on=_.store_id)
        )

        df = (
            joined
            .group_by("stores.store_name", "regions.region_name")
            .aggregate("sales.total_sales")
            .execute()
        )
        # Not all store/region combos have sales (Downtown/South has none)
        assert len(df) == 3
        assert df["sales.total_sales"].sum() == 510

    def test_snowflake_with_filter(self, snowflake):
        """Snowflake join with filter on a second-level dimension."""
        joined = (
            snowflake["sales"]
            .join_one(snowflake["products"], on="product_id")
            .join_one(snowflake["categories"], on="category_id")
            .filter(lambda t: t.categories.category_name == "Electronics")
        )

        df = joined.group_by("products.product_name").aggregate("sales.total_sales").execute()
        # Only Widget and Gadget are Electronics
        assert set(df["products.product_name"]) == {"Widget", "Gadget"}
        assert df["sales.total_sales"].sum() == 340

    def test_snowflake_mixed_shorthand_styles(self, snowflake):
        """Mix string, Deferred, and lambda across a single chain."""
        joined = (
            snowflake["sales"]
            .join_one(snowflake["customers"], on="customer_id")            # string
            .join_one(snowflake["regions"], on=_.region_id)                # Deferred
            .join_one(snowflake["products"],                               # lambda
                      on=lambda l, r: l.product_id == r.product_id)
        )

        df = (
            joined
            .group_by("regions.region_name", "products.product_name")
            .aggregate("sales.sale_count")
            .execute()
        )
        assert df["sales.sale_count"].sum() == 6


# ---------------------------------------------------------------------------
# Deeply nested joins with multi-level measures — order_by, mutate, ambiguity
# ---------------------------------------------------------------------------


class TestDeeplyNestedJoins:
    """Test short-name resolution, order_by, mutate, and ambiguity detection
    on deeply nested (3+ level) join trees with measures at every level.

    Schema:
        continents  (level 3)
          |-- join_many --> countries  (level 2)
          |                   |-- join_many --> cities  (level 1)
          |                   |                  |-- join_many --> shops (fact, level 0)
          |                   |-- measures: country_count, total_area
          |-- measures: continent_count

        shops has measures: shop_count, total_revenue, avg_revenue
        cities has measures: city_count, total_population
        countries has measures: country_count, total_area
        continents has measures: continent_count
    """

    @pytest.fixture()
    def deep_model(self):
        con = ibis.duckdb.connect(":memory:")

        continents = con.create_table(
            "continents",
            pd.DataFrame(
                {
                    "continent_id": [1, 2],
                    "continent_name": ["Europe", "Asia"],
                }
            ),
        )
        countries = con.create_table(
            "countries",
            pd.DataFrame(
                {
                    "country_id": [10, 20, 30],
                    "country_name": ["France", "Germany", "Japan"],
                    "continent_id": [1, 1, 2],
                    "area": [643_801, 357_022, 377_975],
                }
            ),
        )
        cities = con.create_table(
            "cities",
            pd.DataFrame(
                {
                    "city_id": [100, 200, 300, 400],
                    "city_name": ["Paris", "Berlin", "Tokyo", "Osaka"],
                    "country_id": [10, 20, 30, 30],
                    "population": [2_161_000, 3_645_000, 13_960_000, 2_753_000],
                }
            ),
        )
        shops = con.create_table(
            "shops",
            pd.DataFrame(
                {
                    "shop_id": [1, 2, 3, 4, 5, 6],
                    "city_id": [100, 100, 200, 300, 300, 400],
                    "revenue": [500, 300, 700, 1200, 800, 600],
                }
            ),
        )

        continents_st = (
            to_semantic_table(continents, "continents")
            .with_dimensions(
                continent_id=lambda t: t.continent_id,
                continent_name=lambda t: t.continent_name,
            )
            .with_measures(continent_count=_.count())
        )
        countries_st = (
            to_semantic_table(countries, "countries")
            .with_dimensions(
                country_id=lambda t: t.country_id,
                country_name=lambda t: t.country_name,
                continent_id=lambda t: t.continent_id,
            )
            .with_measures(
                country_count=_.count(),
                total_area=_.area.sum(),
            )
        )
        cities_st = (
            to_semantic_table(cities, "cities")
            .with_dimensions(
                city_id=lambda t: t.city_id,
                city_name=lambda t: t.city_name,
                country_id=lambda t: t.country_id,
            )
            .with_measures(
                city_count=_.count(),
                total_population=_.population.sum(),
            )
        )
        shops_st = (
            to_semantic_table(shops, "shops")
            .with_dimensions(
                shop_id=lambda t: t.shop_id,
                city_id=lambda t: t.city_id,
            )
            .with_measures(
                shop_count=_.count(),
                total_revenue=_.revenue.sum(),
                avg_revenue=_.revenue.mean(),
            )
        )

        return {
            "continents": continents_st,
            "countries": countries_st,
            "cities": cities_st,
            "shops": shops_st,
        }

    def _build_full_chain(self, m):
        """Build 4-level join: continents -> countries -> cities -> shops."""
        return (
            m["continents"]
            .join_many(m["countries"], on="continent_id")
            .join_many(m["cities"], on="country_id")
            .join_many(m["shops"], on="city_id")
        )

    # -- basic multi-level aggregation --

    def test_measures_from_all_four_levels(self, deep_model):
        """Aggregate measures from every level of the join tree."""
        joined = self._build_full_chain(deep_model)
        df = (
            joined.group_by("continents.continent_name")
            .aggregate(
                "continents.continent_count",
                "countries.country_count",
                "cities.city_count",
                "shops.shop_count",
                "shops.total_revenue",
            )
            .execute()
        )
        assert set(df["continents.continent_name"]) == {"Europe", "Asia"}
        assert df["shops.total_revenue"].sum() == 4100  # 500+300+700+1200+800+600
        assert df["shops.shop_count"].sum() == 6

    def test_measures_from_intermediate_levels(self, deep_model):
        """Aggregate measures from level-2 (countries) and level-1 (cities).

        Per-source pre-aggregation ensures intermediate measures are computed
        on their own raw tables before joining, preventing fan-out inflation.
        """
        joined = self._build_full_chain(deep_model)
        df = (
            joined.group_by("continents.continent_name")
            .aggregate(
                "countries.total_area",
                "cities.total_population",
            )
            .execute()
        )
        europe = df[df["continents.continent_name"] == "Europe"]
        asia = df[df["continents.continent_name"] == "Asia"]
        # Europe: France(643801) + Germany(357022) = 1000823
        assert europe["countries.total_area"].iloc[0] == 1_000_823
        # Asia: Japan(377975)
        assert asia["countries.total_area"].iloc[0] == 377_975
        # Europe population: Paris(2161000) + Berlin(3645000) = 5806000
        assert europe["cities.total_population"].iloc[0] == 5_806_000

    # -- order_by with short names on deeply nested joins --

    def test_order_by_fqdn_lambda(self, deep_model):
        """order_by(lambda t: t["shops.total_revenue"].desc()) uses FQDN."""
        joined = self._build_full_chain(deep_model)
        df = (
            joined.group_by("continents.continent_name")
            .aggregate("shops.total_revenue")
            .order_by(lambda t: t["shops.total_revenue"].desc())
            .execute()
        )
        # Asia has higher revenue (1200+800+600=2600) vs Europe (500+300+700=1500)
        assert df["continents.continent_name"].iloc[0] == "Asia"

    def test_order_by_fqdn_deferred(self, deep_model):
        """order_by(_["shops.total_revenue"].desc()) uses FQDN."""
        joined = self._build_full_chain(deep_model)
        df = (
            joined.group_by("continents.continent_name")
            .aggregate("shops.total_revenue")
            .order_by(_["shops.total_revenue"].desc())
            .execute()
        )
        assert df["continents.continent_name"].iloc[0] == "Asia"

    def test_order_by_full_prefixed_name(self, deep_model):
        """order_by with full dot-prefixed column name."""
        joined = self._build_full_chain(deep_model)
        df = (
            joined.group_by("continents.continent_name")
            .aggregate("shops.total_revenue")
            .order_by(lambda t: t["shops.total_revenue"].desc())
            .execute()
        )
        assert df["continents.continent_name"].iloc[0] == "Asia"

    def test_order_by_intermediate_measure(self, deep_model):
        """order_by using a measure from an intermediate level (countries) with FQDN."""
        joined = self._build_full_chain(deep_model)
        df = (
            joined.group_by("continents.continent_name")
            .aggregate("countries.total_area", "shops.total_revenue")
            .order_by(lambda t: t["countries.total_area"].desc())
            .execute()
        )
        # Europe total_area (1644624) > Asia (1133925) due to fan-out
        assert df["continents.continent_name"].iloc[0] == "Europe"

    # -- mutate with short names on deeply nested joins --

    def test_mutate_fqdn_lambda(self, deep_model):
        """mutate(lambda t: ...) uses FQDN bracket notation."""
        joined = self._build_full_chain(deep_model)
        df = (
            joined.group_by("continents.continent_name")
            .aggregate("shops.total_revenue", "shops.shop_count")
            .mutate(revenue_per_shop=lambda t: t["shops.total_revenue"] / t["shops.shop_count"])
            .execute()
        )
        assert "revenue_per_shop" in df.columns
        # Europe: 1500/3 = 500, Asia: 2600/3 ≈ 866.67
        europe = df[df["continents.continent_name"] == "Europe"]
        assert pytest.approx(europe["revenue_per_shop"].iloc[0]) == 500.0

    def test_mutate_fqdn_bracket(self, deep_model):
        """mutate with FQDN bracket notation lambda."""
        joined = self._build_full_chain(deep_model)
        df = (
            joined.group_by("continents.continent_name")
            .aggregate("shops.total_revenue", "shops.shop_count")
            .mutate(
                revenue_per_shop=lambda t: t["shops.total_revenue"] / t["shops.shop_count"]
            )
            .execute()
        )
        assert "revenue_per_shop" in df.columns
        asia = df[df["continents.continent_name"] == "Asia"]
        assert pytest.approx(asia["revenue_per_shop"].iloc[0], rel=0.01) == 866.67

    def test_mutate_cross_level_measures(self, deep_model):
        """mutate combining measures from different levels with FQDN."""
        joined = self._build_full_chain(deep_model)
        df = (
            joined.group_by("continents.continent_name")
            .aggregate(
                "shops.total_revenue",
                "cities.total_population",
            )
            .mutate(
                revenue_per_capita=lambda t: t["shops.total_revenue"] / t["cities.total_population"],
            )
            .execute()
        )
        assert "revenue_per_capita" in df.columns
        assert all(df["revenue_per_capita"] > 0)

    # -- pipeline: aggregate -> mutate -> order_by --

    def test_full_pipeline_deeply_nested(self, deep_model):
        """Full pipeline: aggregate measures from 4 levels -> mutate -> order_by with FQDN.

        With pre-aggregation, city_count is correct (Europe=2, Asia=2).
        Asia: 2600/2 = 1300, Europe: 1500/2 = 750.
        """
        joined = self._build_full_chain(deep_model)
        df = (
            joined.group_by("continents.continent_name")
            .aggregate(
                "shops.total_revenue",
                "cities.city_count",
                "countries.country_count",
            )
            .mutate(
                revenue_per_city=lambda t: t["shops.total_revenue"] / t["cities.city_count"],
            )
            .order_by(lambda t: t.revenue_per_city.desc())
            .execute()
        )
        # Asia: 2600/2 = 1300, Europe: 1500/2 = 750
        assert df["continents.continent_name"].iloc[0] == "Asia"
        assert pytest.approx(df["revenue_per_city"].iloc[0], rel=0.01) == 1300.0

    # -- ambiguity detection --

    def test_short_name_order_by_raises(self, deep_model):
        """Short name in order_by raises after join — use FQDN bracket notation."""
        con = ibis.duckdb.connect(":memory:")
        left = con.create_table(
            "left_t",
            pd.DataFrame({"key": [1, 2], "left_val": [10, 20]}),
        )
        right = con.create_table(
            "right_t",
            pd.DataFrame({"key": [1, 2], "right_val": [30, 40]}),
        )
        left_st = (
            to_semantic_table(left, "left")
            .with_dimensions(key=lambda t: t.key)
            .with_measures(total=_.left_val.sum())
        )
        right_st = (
            to_semantic_table(right, "right")
            .with_dimensions(key=lambda t: t.key)
            .with_measures(total=_.right_val.sum())
        )

        joined = left_st.join_one(right_st, on="key")
        agg = joined.aggregate("left.total", "right.total")

        with pytest.raises(AttributeError, match="no attribute 'total'"):
            agg.order_by(lambda t: t.total.desc()).execute()

    def test_short_name_mutate_raises(self, deep_model):
        """Short names always fail after join in mutate — FQDN required."""
        con = ibis.duckdb.connect(":memory:")
        left = con.create_table(
            "left_t2",
            pd.DataFrame({"key": [1, 2], "left_val": [10, 20]}),
        )
        right = con.create_table(
            "right_t2",
            pd.DataFrame({"key": [1, 2], "right_val": [30, 40]}),
        )
        left_st = (
            to_semantic_table(left, "left")
            .with_dimensions(key=lambda t: t.key)
            .with_measures(total=_.left_val.sum())
        )
        right_st = (
            to_semantic_table(right, "right")
            .with_dimensions(key=lambda t: t.key)
            .with_measures(total=_.right_val.sum())
        )

        joined = left_st.join_one(right_st, on="key")
        agg = joined.aggregate("left.total", "right.total")

        with pytest.raises(Exception):
            agg.mutate(doubled=lambda t: t.total * 2).execute()

    def test_ambiguous_resolved_with_full_prefix(self, deep_model):
        """Full dot-prefixed names work even when short names are ambiguous."""
        con = ibis.duckdb.connect(":memory:")
        # Use distinct raw column names to avoid ibis column collision in join
        left = con.create_table(
            "left_t3",
            pd.DataFrame({"key": [1, 2], "left_val": [10, 20]}),
        )
        right = con.create_table(
            "right_t3",
            pd.DataFrame({"key": [1, 2], "right_val": [30, 40]}),
        )
        left_st = (
            to_semantic_table(left, "left")
            .with_dimensions(key=lambda t: t.key)
            .with_measures(total=_.left_val.sum())
        )
        right_st = (
            to_semantic_table(right, "right")
            .with_dimensions(key=lambda t: t.key)
            .with_measures(total=_.right_val.sum())
        )

        joined = left_st.join_one(right_st, on="key")
        df = (
            joined.aggregate("left.total", "right.total")
            .mutate(combined=lambda t: t["left.total"] + t["right.total"])
            .order_by(lambda t: t["left.total"].desc())
            .execute()
        )
        assert "combined" in df.columns
        # left.total = 10+20 = 30, right.total = 30+40 = 70, combined = 100
        assert df["combined"].sum() == 100

    # -- group_by at different levels --

    def test_group_by_level2_aggregate_level0(self, deep_model):
        """Group by level-2 dim, aggregate level-0 measure with FQDN."""
        joined = self._build_full_chain(deep_model)
        df = (
            joined.group_by("countries.country_name")
            .aggregate("shops.total_revenue", "shops.shop_count")
            .order_by(lambda t: t["shops.total_revenue"].desc())
            .execute()
        )
        # Japan: Tokyo(1200+800) + Osaka(600) = 2600
        # France: Paris(500+300) = 800
        # Germany: Berlin(700) = 700
        assert df["countries.country_name"].iloc[0] == "Japan"
        assert df["shops.total_revenue"].iloc[0] == 2600

    def test_group_by_level1_aggregate_level0(self, deep_model):
        """Group by level-1 dim, aggregate level-0 measure with FQDN."""
        joined = self._build_full_chain(deep_model)
        df = (
            joined.group_by("cities.city_name")
            .aggregate("shops.total_revenue")
            .order_by(lambda t: t["shops.total_revenue"].desc())
            .execute()
        )
        # Tokyo: 1200+800 = 2000
        assert df["cities.city_name"].iloc[0] == "Tokyo"
        assert df["shops.total_revenue"].iloc[0] == 2000
