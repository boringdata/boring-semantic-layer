from __future__ import annotations

import ibis
import pytest

from boring_semantic_layer import to_semantic_table


@pytest.fixture
def flights_data():
    con = ibis.duckdb.connect(":memory:")
    data = {
        "origin": ["JFK", "LAX", "SFO"],
        "destination": ["LAX", "JFK", "NYC"],
        "distance": [100, 200, 300],
    }
    return con.create_table("flights", data)


def test_dimension_serialization(flights_data):
    from boring_semantic_layer.xorq_convert import serialize_dimensions

    flights = to_semantic_table(flights_data, name="flights").with_dimensions(
        origin=lambda t: t.origin,
        destination=lambda t: t.destination,
    )

    op = flights.op()
    dims = op.get_dimensions()

    result = serialize_dimensions(dims)
    assert result
    dim_metadata = result.unwrap()

    assert "origin" in dim_metadata
    assert "expr_struct" in dim_metadata["origin"] or "expr" in dim_metadata["origin"]

    assert "destination" in dim_metadata
    assert "expr_struct" in dim_metadata["destination"] or "expr" in dim_metadata["destination"]


def test_measure_serialization(flights_data):
    from boring_semantic_layer.xorq_convert import serialize_measures

    flights = to_semantic_table(flights_data, name="flights").with_measures(
        avg_distance=lambda t: t.distance.mean(),
        total_distance=lambda t: t.distance.sum(),
    )

    op = flights.op()
    measures = op.get_measures()

    result = serialize_measures(measures)
    assert result
    meas_metadata = result.unwrap()

    assert "avg_distance" in meas_metadata
    assert "expr_struct" in meas_metadata["avg_distance"]

    assert "total_distance" in meas_metadata
    assert "expr_struct" in meas_metadata["total_distance"]


def test_to_tagged_with_string_metadata(flights_data):
    from boring_semantic_layer.xorq_convert import to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(
            origin=lambda t: t.origin,
            destination=lambda t: t.destination,
        )
        .with_measures(
            avg_distance=lambda t: t.distance.mean(),
            total_distance=lambda t: t.distance.sum(),
        )
    )

    tagged_expr = to_tagged(flights)

    op = tagged_expr.op()
    metadata = dict(op.metadata)

    # metadata is stored as nested tuples, convert to dict
    dims = dict(metadata["dimensions"])
    # each dimension value is also a tuple of key-value pairs
    origin_dim = dict(dims["origin"])
    assert "expr_struct" in origin_dim or "expr" in origin_dim

    destination_dim = dict(dims["destination"])
    assert "expr_struct" in destination_dim or "expr" in destination_dim

    # measures are also stored as nested tuples
    meas = dict(metadata["measures"])
    avg_distance_meas = dict(meas["avg_distance"])
    assert "expr_struct" in avg_distance_meas

    total_distance_meas = dict(meas["total_distance"])
    assert "expr_struct" in total_distance_meas


def test_to_tagged_instance_method(flights_data):
    """SemanticTable.to_tagged() instance method works the same as the module-level function."""
    from boring_semantic_layer.xorq_convert import from_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(avg_distance=lambda t: t.distance.mean())
    )

    tagged_expr = flights.to_tagged()

    op = tagged_expr.op()
    metadata = dict(op.metadata)
    dims = dict(metadata["dimensions"])
    origin_dim = dict(dims["origin"])
    assert "expr_struct" in origin_dim or "expr" in origin_dim

    meas = dict(metadata["measures"])
    avg_distance_meas = dict(meas["avg_distance"])
    assert "expr_struct" in avg_distance_meas

    # Verify round-trip works
    reconstructed = from_tagged(tagged_expr)
    result = reconstructed.group_by("origin").aggregate("avg_distance").execute()
    assert len(result) > 0
    assert "origin" in result.columns
    assert "avg_distance" in result.columns


def test_from_tagged_deserialization(flights_data):
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(avg_distance=lambda t: t.distance.mean())
    )

    tagged_expr = to_tagged(flights)
    reconstructed = from_tagged(tagged_expr)

    result = reconstructed.group_by("origin").aggregate("avg_distance").execute()

    assert len(result) > 0
    assert "origin" in result.columns
    assert "avg_distance" in result.columns


def test_serialize_entity_dimensions(flights_data):
    from boring_semantic_layer import entity_dimension
    from boring_semantic_layer.xorq_convert import serialize_dimensions

    flights = to_semantic_table(flights_data, name="flights").with_dimensions(
        origin=entity_dimension(lambda t: t.origin, "Origin airport"),
        destination=lambda t: t.destination,
    )

    op = flights.op()
    dims = op.get_dimensions()

    result = serialize_dimensions(dims)
    assert result
    dim_metadata = result.unwrap()

    # Entity dimension should have is_entity flag
    assert "origin" in dim_metadata
    assert dim_metadata["origin"]["is_entity"] is True
    assert dim_metadata["origin"]["description"] == "Origin airport"
    assert "expr_struct" in dim_metadata["origin"] or "expr" in dim_metadata["origin"]

    # Regular dimension should not have is_entity flag
    assert "destination" in dim_metadata
    assert dim_metadata["destination"]["is_entity"] is False


def test_serialize_event_timestamp_dimensions(flights_data):
    from boring_semantic_layer import time_dimension
    from boring_semantic_layer.xorq_convert import serialize_dimensions

    con = ibis.duckdb.connect(":memory:")
    data = {
        "origin": ["JFK", "LAX"],
        "arr_time": ["2024-01-01", "2024-01-02"],
        "distance": [100, 200],
    }
    tbl = con.create_table("flights", data)

    flights = to_semantic_table(tbl, name="flights").with_dimensions(
        arr_time=time_dimension(
            lambda t: t.arr_time,
            "Arrival time",
            smallest_time_grain="TIME_GRAIN_DAY"
        ),
        origin=lambda t: t.origin,
    )

    op = flights.op()
    dims = op.get_dimensions()

    result = serialize_dimensions(dims)
    assert result
    dim_metadata = result.unwrap()

    # Event timestamp dimension should have flags
    assert "arr_time" in dim_metadata
    assert dim_metadata["arr_time"]["is_event_timestamp"] is True
    assert dim_metadata["arr_time"]["is_time_dimension"] is True
    assert dim_metadata["arr_time"]["smallest_time_grain"] == "TIME_GRAIN_DAY"
    assert dim_metadata["arr_time"]["description"] == "Arrival time"

    # Regular dimension should not have flags
    assert "origin" in dim_metadata
    assert dim_metadata["origin"]["is_event_timestamp"] is False
    assert dim_metadata["origin"]["is_time_dimension"] is False


def test_entity_dimension_roundtrip(flights_data):
    from boring_semantic_layer import entity_dimension
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(
            origin=entity_dimension(lambda t: t.origin, "Origin airport"),
            destination=lambda t: t.destination,
        )
        .with_measures(avg_distance=lambda t: t.distance.mean())
    )

    # Serialize and deserialize
    tagged_expr = to_tagged(flights)
    reconstructed = from_tagged(tagged_expr)

    # Verify entity dimension metadata is preserved
    dims = reconstructed.get_dimensions()
    assert "origin" in dims
    assert dims["origin"].is_entity is True
    assert dims["origin"].description == "Origin airport"

    assert "destination" in dims
    assert dims["destination"].is_entity is False

    # Verify it still works
    result = reconstructed.group_by("origin").aggregate("avg_distance").execute()
    assert len(result) > 0


def test_event_timestamp_roundtrip(flights_data):
    from boring_semantic_layer import time_dimension
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    data = {
        "origin": ["JFK", "LAX", "SFO"],
        "arr_time": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "distance": [100, 200, 300],
    }
    tbl = con.create_table("flights", data)

    flights = (
        to_semantic_table(tbl, name="flights")
        .with_dimensions(
            arr_time=time_dimension(
                lambda t: t.arr_time,
                "Arrival time",
                smallest_time_grain="TIME_GRAIN_DAY"
            ),
            origin=lambda t: t.origin,
        )
        .with_measures(total_distance=lambda t: t.distance.sum())
    )

    # Serialize and deserialize
    tagged_expr = to_tagged(flights)
    reconstructed = from_tagged(tagged_expr)

    # Verify event timestamp metadata is preserved
    dims = reconstructed.get_dimensions()
    assert "arr_time" in dims
    assert dims["arr_time"].is_event_timestamp is True
    assert dims["arr_time"].is_time_dimension is True
    assert dims["arr_time"].smallest_time_grain == "TIME_GRAIN_DAY"
    assert dims["arr_time"].description == "Arrival time"

    assert "origin" in dims
    assert dims["origin"].is_event_timestamp is False

    # Verify it still works
    result = reconstructed.group_by("arr_time").aggregate("total_distance").execute()
    assert len(result) > 0


def test_entity_and_event_timestamp_roundtrip(flights_data):
    from boring_semantic_layer import entity_dimension, time_dimension
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    data = {
        "business_id": [1, 2, 3],
        "statement_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "balance": [1000, 2000, 3000],
    }
    tbl = con.create_table("balance", data)

    balance = (
        to_semantic_table(tbl, name="balance_features")
        .with_dimensions(
            business_id=entity_dimension(lambda t: t.business_id, "Business identifier"),
            statement_date=time_dimension(
                lambda t: t.statement_date,
                "Statement date",
                smallest_time_grain="TIME_GRAIN_DAY"
            ),
        )
        .with_measures(total_balance=lambda t: t.balance.sum())
    )

    # Serialize and deserialize
    tagged_expr = to_tagged(balance)
    reconstructed = from_tagged(tagged_expr)

    # Verify entity dimension
    dims = reconstructed.get_dimensions()
    assert dims["business_id"].is_entity is True
    assert dims["business_id"].description == "Business identifier"

    # Verify event timestamp
    assert dims["statement_date"].is_event_timestamp is True
    assert dims["statement_date"].is_time_dimension is True
    assert dims["statement_date"].smallest_time_grain == "TIME_GRAIN_DAY"

    # Verify json_definition contains both
    json_def = reconstructed.json_definition
    assert "entity_dimensions" in json_def
    assert "business_id" in json_def["entity_dimensions"]
    assert "event_timestamp" in json_def
    assert "statement_date" in json_def["event_timestamp"]

    # Verify it still works
    result = reconstructed.group_by("business_id", "statement_date").aggregate("total_balance").execute()
    assert len(result) > 0


def test_case_expr_measure_serialization(flights_data):
    """Case expression measures should serialize via source extraction."""
    import xorq.api as xo

    from boring_semantic_layer.xorq_convert import serialize_measures

    flights = to_semantic_table(flights_data, name="flights").with_measures(
        short_flight_count=lambda t: xo.case().when(t.distance < 200, 1).else_(0).end().sum(),
    )

    op = flights.op()
    measures = op.get_measures()
    result = serialize_measures(measures)
    assert result
    meas_metadata = result.unwrap()

    assert "short_flight_count" in meas_metadata
    assert "expr_struct" in meas_metadata["short_flight_count"]

def test_case_expr_tagged_roundtrip(flights_data):
    """Case expression measures should survive to_tagged → from_tagged."""
    import xorq.api as xo

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(
            short_flight_count=lambda t: xo.case().when(t.distance < 200, 1).else_(0).end().sum(),
        )
    )

    tagged_expr = to_tagged(flights)
    reconstructed = from_tagged(tagged_expr)

    result = reconstructed.group_by("origin").aggregate("short_flight_count").execute()
    assert len(result) > 0
    assert "origin" in result.columns
    assert "short_flight_count" in result.columns


def test_ifelse_measure_serialization(flights_data):
    """xo.ifelse measures should serialize via source extraction."""
    import xorq.api as xo

    from boring_semantic_layer.xorq_convert import serialize_measures

    flights = to_semantic_table(flights_data, name="flights").with_measures(
        short_flight_count=lambda t: xo.ifelse(t.distance < 200, 1, 0).sum(),
    )

    op = flights.op()
    measures = op.get_measures()
    result = serialize_measures(measures)
    assert result
    meas_metadata = result.unwrap()

    assert "short_flight_count" in meas_metadata
    assert "expr_struct" in meas_metadata["short_flight_count"]

def test_ifelse_tagged_roundtrip(flights_data):
    """xo.ifelse measures should survive to_tagged → from_tagged."""
    import xorq.api as xo

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(
            short_flight_count=lambda t: xo.ifelse(t.distance < 200, 1, 0).sum(),
        )
    )

    tagged_expr = to_tagged(flights)
    reconstructed = from_tagged(tagged_expr)

    result = reconstructed.group_by("origin").aggregate("short_flight_count").execute()
    assert len(result) > 0
    assert "origin" in result.columns
    assert "short_flight_count" in result.columns


# --- Structured resolver serialization tests ---


def test_serialize_resolver_simple_attr():
    """_.distance round-trips through structured serialization."""
    from boring_semantic_layer.utils import deserialize_resolver, serialize_resolver

    from xorq.vendor.ibis import _
    from xorq.vendor.ibis.common.deferred import Deferred

    d = _.distance
    data = serialize_resolver(d._resolver)

    assert data == ("attr", ("var", "_"), ("just", "distance"))

    r = deserialize_resolver(data)
    d2 = Deferred(r)
    assert repr(d2) == repr(d)


def test_serialize_resolver_method_call():
    """_.distance.mean() round-trips through structured serialization."""
    from boring_semantic_layer.utils import deserialize_resolver, serialize_resolver

    from xorq.vendor.ibis import _
    from xorq.vendor.ibis.common.deferred import Deferred

    d = _.distance.mean()
    data = serialize_resolver(d._resolver)

    assert data[0] == "call"
    assert data[1][0] == "attr"  # func is an Attr

    r = deserialize_resolver(data)
    d2 = Deferred(r)
    assert repr(d2) == "_.distance.mean()"


def test_serialize_resolver_case_expr():
    """case().when(_.distance < 200, 1).else_(0).end().sum() round-trips."""
    import xorq.api as xo

    from boring_semantic_layer.utils import expr_to_structured, structured_to_expr
    from xorq.common.utils.ibis_utils import from_ibis
    from xorq.vendor.ibis import _

    fn = lambda t: xo.case().when(t.distance < 200, 1).else_(0).end().sum()
    result = expr_to_structured(fn)
    assert result.value_or(None) is not None

    data = result.unwrap()
    assert data[0] == "call"

    deferred = structured_to_expr(data).unwrap()

    # Verify with xorq table
    con = ibis.duckdb.connect(":memory:")
    t = from_ibis(con.create_table("test", {"distance": [100, 200, 300]}))
    resolved = deferred.resolve(t)
    assert resolved.execute() == 1


def test_serialize_resolver_ifelse():
    """xo.ifelse(_.distance < 200, 1, 0).sum() round-trips."""
    import xorq.api as xo

    from boring_semantic_layer.utils import expr_to_structured, structured_to_expr
    from xorq.common.utils.ibis_utils import from_ibis
    from xorq.vendor.ibis import _

    fn = lambda t: xo.ifelse(t.distance < 200, 1, 0).sum()
    result = expr_to_structured(fn)
    assert result.value_or(None) is not None

    data = result.unwrap()
    deferred = structured_to_expr(data).unwrap()

    # Verify with xorq table
    con = ibis.duckdb.connect(":memory:")
    t = from_ibis(con.create_table("test", {"distance": [100, 200, 300]}))
    resolved = deferred.resolve(t)
    assert resolved.execute() == 1


def test_structured_serialization_in_measures(flights_data):
    """Measures serialize with both expr and expr_struct."""
    import xorq.api as xo

    from boring_semantic_layer.xorq_convert import serialize_measures

    flights = to_semantic_table(flights_data, name="flights").with_measures(
        short_flight_count=lambda t: xo.case().when(t.distance < 200, 1).else_(0).end().sum(),
        avg_distance=lambda t: t.distance.mean(),
    )

    op = flights.op()
    measures = op.get_measures()
    result = serialize_measures(measures)
    meas_metadata = result.unwrap()

    assert "expr_struct" in meas_metadata["short_flight_count"]
    assert "expr_struct" in meas_metadata["avg_distance"]

def test_structured_tagged_roundtrip_case(flights_data):
    """Full to_tagged -> from_tagged with case expression using structured format."""
    import xorq.api as xo

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(
            short_flight_count=lambda t: xo.case().when(t.distance < 200, 1).else_(0).end().sum(),
            avg_distance=lambda t: t.distance.mean(),
        )
    )

    tagged_expr = to_tagged(flights)

    # Verify expr_struct is in the tag metadata
    op = tagged_expr.op()
    metadata = dict(op.metadata)
    meas = dict(metadata["measures"])
    short_flight = dict(meas["short_flight_count"])
    assert "expr_struct" in short_flight
    # Reconstruct and verify
    reconstructed = from_tagged(tagged_expr)

    result = reconstructed.group_by("origin").aggregate("short_flight_count", "avg_distance").execute()
    assert len(result) > 0
    assert "origin" in result.columns
    assert "short_flight_count" in result.columns
    assert "avg_distance" in result.columns


def test_structured_tagged_roundtrip_ifelse(flights_data):
    """Full to_tagged -> from_tagged with ifelse expression using structured format."""
    import xorq.api as xo

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(
            short_flight_count=lambda t: xo.ifelse(t.distance < 200, 1, 0).sum(),
        )
    )

    tagged_expr = to_tagged(flights)
    reconstructed = from_tagged(tagged_expr)

    result = reconstructed.group_by("origin").aggregate("short_flight_count").execute()
    assert len(result) > 0
    assert "origin" in result.columns
    assert "short_flight_count" in result.columns


# --- Additional round-trip tests for structured resolver serialization ---


def _make_resolver_roundtrip(fn):
    """Helper: serialize a lambda -> structured tuple -> Deferred, return Deferred."""
    from boring_semantic_layer.utils import expr_to_structured, structured_to_expr

    data = expr_to_structured(fn).unwrap()
    return structured_to_expr(data).unwrap()


@pytest.fixture
def xorq_flights():
    """A xorq table with flights data for resolver round-trip tests."""
    from xorq.common.utils.ibis_utils import from_ibis

    con = ibis.duckdb.connect(":memory:")
    data = {
        "origin": ["JFK", "LAX", "SFO", "JFK", "LAX"],
        "destination": ["LAX", "JFK", "NYC", "SFO", "SFO"],
        "distance": [100, 200, 300, 150, 250],
        "dep_delay": [10, -5, 0, 20, -3],
        "carrier": ["AA", "UA", "AA", "UA", "AA"],
    }
    return from_ibis(con.create_table("flights", data))


def test_resolver_roundtrip_all_aggregations(xorq_flights):
    """All standard aggregations round-trip: count, sum, mean, max, min."""
    fns = {
        "count": lambda t: t.distance.count(),
        "sum": lambda t: t.distance.sum(),
        "mean": lambda t: t.distance.mean(),
        "max": lambda t: t.distance.max(),
        "min": lambda t: t.distance.min(),
    }

    for name, fn in fns.items():
        deferred = _make_resolver_roundtrip(fn)
        original = fn(xorq_flights)
        reconstructed = deferred.resolve(xorq_flights)
        assert original.execute() == reconstructed.execute(), f"{name} mismatch"


def test_resolver_roundtrip_nunique(xorq_flights):
    """nunique() round-trips through structured serialization."""
    fn = lambda t: t.carrier.nunique()
    deferred = _make_resolver_roundtrip(fn)
    assert fn(xorq_flights).execute() == deferred.resolve(xorq_flights).execute()


def test_resolver_roundtrip_binary_arithmetic(xorq_flights):
    """Arithmetic binary operators: +, -, *, / on deferred expressions."""
    fn_add = lambda t: t.distance.sum() + t.dep_delay.sum()
    fn_sub = lambda t: t.distance.sum() - t.dep_delay.sum()
    fn_mul = lambda t: t.distance.sum() * 2
    fn_div = lambda t: t.distance.sum() / t.distance.count()

    for label, fn in [("add", fn_add), ("sub", fn_sub), ("mul", fn_mul), ("div", fn_div)]:
        deferred = _make_resolver_roundtrip(fn)
        assert fn(xorq_flights).execute() == deferred.resolve(xorq_flights).execute(), f"{label} mismatch"


def test_resolver_roundtrip_comparison_operators(xorq_flights):
    """Comparison binary operators: <, <=, >, >=, ==, != produce correct deferred."""
    from boring_semantic_layer.utils import expr_to_structured, structured_to_expr

    comparisons = {
        "lt": lambda t: (t.distance < 200).sum(),
        "le": lambda t: (t.distance <= 200).sum(),
        "gt": lambda t: (t.distance > 200).sum(),
        "ge": lambda t: (t.distance >= 200).sum(),
        "eq": lambda t: (t.distance == 200).sum(),
        "ne": lambda t: (t.distance != 200).sum(),
    }

    for label, fn in comparisons.items():
        deferred = _make_resolver_roundtrip(fn)
        assert fn(xorq_flights).execute() == deferred.resolve(xorq_flights).execute(), f"{label} mismatch"


def test_resolver_roundtrip_unary_neg(xorq_flights):
    """Unary negation round-trips."""
    fn = lambda t: (-t.dep_delay).sum()
    deferred = _make_resolver_roundtrip(fn)
    assert fn(xorq_flights).execute() == deferred.resolve(xorq_flights).execute()


def test_resolver_roundtrip_chained_methods(xorq_flights):
    """Chained method calls like .cast() round-trip."""
    fn = lambda t: t.distance.cast("float64").mean()
    deferred = _make_resolver_roundtrip(fn)
    assert fn(xorq_flights).execute() == deferred.resolve(xorq_flights).execute()


def test_resolver_roundtrip_boolean_cast_sum(xorq_flights):
    """Boolean cast to int then sum: (condition).cast('int').sum() round-trips."""
    fn = lambda t: (t.distance > 150).cast("int32").sum()
    deferred = _make_resolver_roundtrip(fn)
    assert fn(xorq_flights).execute() == deferred.resolve(xorq_flights).execute()


def test_resolver_roundtrip_desc(xorq_flights):
    """The .desc() method call on a column round-trips."""
    from boring_semantic_layer.utils import expr_to_structured, structured_to_expr

    fn = lambda t: t.distance.desc()
    data = expr_to_structured(fn).unwrap()
    assert data[0] == "call"
    # Verify the structure can be deserialized
    deferred = structured_to_expr(data).unwrap()
    assert repr(deferred) == "_.distance.desc()"


def test_resolver_roundtrip_multi_when_case(xorq_flights):
    """Multi-when case expression round-trips."""
    import xorq.api as xo

    fn = lambda t: (
        xo.case()
        .when(t.distance < 150, 1)
        .when(t.distance < 250, 2)
        .else_(3)
        .end()
        .sum()
    )
    deferred = _make_resolver_roundtrip(fn)
    assert fn(xorq_flights).execute() == deferred.resolve(xorq_flights).execute()


def test_resolver_roundtrip_case_with_string_result(xorq_flights):
    """Case expression with string results round-trips."""
    import xorq.api as xo

    fn = lambda t: xo.case().when(t.distance < 200, "short").else_("long").end()
    deferred = _make_resolver_roundtrip(fn)
    # Resolve and check it produces a valid column
    result = deferred.resolve(xorq_flights)
    assert result is not None


def test_tagged_roundtrip_multiple_measure_types(flights_data):
    """to_tagged -> from_tagged with mix of simple, case, and arithmetic measures."""
    import xorq.api as xo

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(
            flight_count=lambda t: t.count(),
            total_distance=lambda t: t.distance.sum(),
            avg_distance=lambda t: t.distance.mean(),
            short_flight_count=lambda t: xo.case().when(t.distance < 200, 1).else_(0).end().sum(),
        )
    )

    tagged = to_tagged(flights)
    reconstructed = from_tagged(tagged)

    result = (
        reconstructed
        .group_by("origin")
        .aggregate("flight_count", "total_distance", "avg_distance", "short_flight_count")
        .execute()
    )
    assert len(result) > 0
    assert set(result.columns) == {"origin", "flight_count", "total_distance", "avg_distance", "short_flight_count"}


def test_tagged_roundtrip_filter_predicate(flights_data):
    """Filter predicates survive to_tagged -> from_tagged."""
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(total_distance=lambda t: t.distance.sum())
        .filter(lambda t: t.distance > 100)
    )

    tagged = to_tagged(flights)
    reconstructed = from_tagged(tagged)

    result = reconstructed.group_by("origin").aggregate("total_distance").execute()
    assert len(result) > 0
    # Only rows with distance > 100 (200, 300) should be included
    assert result["total_distance"].sum() == 500


def test_tagged_roundtrip_mutate_arithmetic(flights_data):
    """Mutate with arithmetic expression survives to_tagged -> from_tagged."""
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(
            total_distance=lambda t: t.distance.sum(),
            flight_count=lambda t: t.count(),
        )
    )

    result_original = (
        flights
        .group_by("origin")
        .aggregate("total_distance", "flight_count")
        .mutate(avg_distance_per_flight=lambda t: t.total_distance / t.flight_count)
        .execute()
    )

    tagged = to_tagged(
        flights
        .group_by("origin")
        .aggregate("total_distance", "flight_count")
        .mutate(avg_distance_per_flight=lambda t: t.total_distance / t.flight_count)
    )
    reconstructed = from_tagged(tagged)
    result_reconstructed = reconstructed.execute()

    assert len(result_reconstructed) > 0
    assert "avg_distance_per_flight" in result_reconstructed.columns


def test_tagged_roundtrip_order_by(flights_data):
    """Order by expression survives to_tagged -> from_tagged."""
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(total_distance=lambda t: t.distance.sum())
    )

    tagged = to_tagged(
        flights
        .group_by("origin")
        .aggregate("total_distance")
        .order_by(lambda t: t.total_distance.desc())
    )
    reconstructed = from_tagged(tagged)
    result = reconstructed.execute()

    assert len(result) > 0
    assert "total_distance" in result.columns
    # Should be in descending order
    distances = result["total_distance"].tolist()
    assert distances == sorted(distances, reverse=True)


def test_tagged_roundtrip_with_limit(flights_data):
    """Limit survives to_tagged -> from_tagged."""
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(total_distance=lambda t: t.distance.sum())
    )

    tagged = to_tagged(
        flights
        .group_by("origin")
        .aggregate("total_distance")
        .limit(2)
    )
    reconstructed = from_tagged(tagged)
    result = reconstructed.execute()

    assert len(result) == 2


def test_tagged_roundtrip_full_pipeline(flights_data):
    """Full pipeline: filter -> group_by -> aggregate -> mutate -> order_by -> limit."""
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(
            total_distance=lambda t: t.distance.sum(),
            flight_count=lambda t: t.count(),
        )
        .filter(lambda t: t.distance >= 100)
    )

    pipeline = (
        flights
        .group_by("origin")
        .aggregate("total_distance", "flight_count")
        .mutate(avg_dist=lambda t: t.total_distance / t.flight_count)
        .order_by(lambda t: t.avg_dist.desc())
        .limit(2)
    )

    tagged = to_tagged(pipeline)
    reconstructed = from_tagged(tagged)
    result = reconstructed.execute()

    assert len(result) <= 2
    assert "avg_dist" in result.columns
    assert "origin" in result.columns


def test_tagged_roundtrip_case_multi_when(flights_data):
    """Multi-when case expression measure survives full to_tagged -> from_tagged."""
    import xorq.api as xo

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    data = {
        "origin": ["JFK", "LAX", "SFO", "JFK", "LAX"],
        "distance": [50, 150, 250, 350, 450],
    }
    tbl = con.create_table("flights2", data)

    flights = (
        to_semantic_table(tbl, name="flights2")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(
            bucket_sum=lambda t: (
                xo.case()
                .when(t.distance < 100, 1)
                .when(t.distance < 200, 2)
                .when(t.distance < 300, 3)
                .else_(4)
                .end()
                .sum()
            ),
        )
    )

    tagged = to_tagged(flights)
    reconstructed = from_tagged(tagged)

    result = reconstructed.group_by("origin").aggregate("bucket_sum").execute()
    assert len(result) > 0
    assert "bucket_sum" in result.columns
    # JFK: 50->1, 350->4 = 5; LAX: 150->2, 450->4 = 6; SFO: 250->3
    total = result["bucket_sum"].sum()
    assert total == 14


def test_tagged_roundtrip_multiple_dimensions(flights_data):
    """Multiple dimensions round-trip correctly."""
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(
            origin=lambda t: t.origin,
            destination=lambda t: t.destination,
        )
        .with_measures(total_distance=lambda t: t.distance.sum())
    )

    tagged = to_tagged(flights)
    reconstructed = from_tagged(tagged)

    dims = reconstructed.get_dimensions()
    assert "origin" in dims
    assert "destination" in dims

    result = (
        reconstructed
        .group_by("origin", "destination")
        .aggregate("total_distance")
        .execute()
    )
    assert len(result) > 0
    assert "origin" in result.columns
    assert "destination" in result.columns


# --- Advanced modeling round-trip tests ---


def test_tagged_roundtrip_deferred_underscore_measures():
    """Measures defined using the ibis _ deferred API round-trip correctly."""
    from ibis import _

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    data = {
        "origin": ["JFK", "LAX", "SFO", "JFK", "LAX"],
        "distance": [100, 200, 300, 150, 250],
    }
    tbl = con.create_table("flights_deferred", data)

    flights = (
        to_semantic_table(tbl, name="flights_deferred")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(
            total_distance=_.distance.sum(),
            avg_distance=_.distance.mean(),
            flight_count=_.count(),
        )
    )

    tagged = to_tagged(flights)
    reconstructed = from_tagged(tagged)

    result = (
        reconstructed
        .group_by("origin")
        .aggregate("total_distance", "avg_distance", "flight_count")
        .order_by("origin")
        .execute()
    )

    assert len(result) > 0
    assert set(result.columns) == {"origin", "total_distance", "avg_distance", "flight_count"}

    # Verify computed values
    jfk = result[result["origin"] == "JFK"].iloc[0]
    assert jfk["total_distance"] == 250  # 100 + 150
    assert jfk["flight_count"] == 2


def test_tagged_roundtrip_composite_deferred_measure():
    """Composite deferred measure (ratio of two aggregations) round-trips."""
    from ibis import _

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    data = {
        "origin": ["JFK", "LAX", "SFO", "JFK", "LAX"],
        "distance": [100, 200, 300, 150, 250],
    }
    tbl = con.create_table("flights_composite", data)

    flights = (
        to_semantic_table(tbl, name="flights_composite")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(
            total_distance=_.distance.sum(),
            flight_count=_.distance.count(),
        )
    )

    tagged = to_tagged(flights)
    reconstructed = from_tagged(tagged)

    result = (
        reconstructed
        .group_by("origin")
        .aggregate("total_distance", "flight_count")
        .mutate(avg_distance=lambda t: t.total_distance / t.flight_count)
        .order_by("origin")
        .execute()
    )

    assert len(result) > 0
    assert "avg_distance" in result.columns

    jfk = result[result["origin"] == "JFK"].iloc[0]
    assert pytest.approx(jfk["avg_distance"]) == 125.0  # (100+150)/2


def test_tagged_roundtrip_boolean_condition_measure():
    """Measure that sums a boolean condition round-trips with value verification."""
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    data = {
        "origin": ["JFK", "LAX", "SFO", "JFK", "LAX"],
        "distance": [100, 200, 300, 150, 250],
    }
    tbl = con.create_table("flights_bool", data)

    flights = (
        to_semantic_table(tbl, name="flights_bool")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(
            short_flights=lambda t: (t.distance < 200).sum(),
            total_flights=lambda t: t.count(),
        )
    )

    tagged = to_tagged(flights)
    reconstructed = from_tagged(tagged)

    result = (
        reconstructed
        .group_by("origin")
        .aggregate("short_flights", "total_flights")
        .order_by("origin")
        .execute()
    )

    assert len(result) > 0
    jfk = result[result["origin"] == "JFK"].iloc[0]
    assert jfk["short_flights"] == 2  # 100 < 200 and 150 < 200
    assert jfk["total_flights"] == 2


def test_tagged_roundtrip_case_with_value_verification(flights_data):
    """Case expression measure round-trips with correct computed values."""
    import xorq.api as xo

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(
            distance_bucket=lambda t: (
                xo.case()
                .when(t.distance < 150, "short")
                .when(t.distance < 250, "medium")
                .else_("long")
                .end()
            ),
            short_count=lambda t: xo.case().when(t.distance < 200, 1).else_(0).end().sum(),
        )
    )

    tagged = to_tagged(flights)
    reconstructed = from_tagged(tagged)

    result = (
        reconstructed
        .group_by("origin")
        .aggregate("short_count")
        .order_by("origin")
        .execute()
    )

    assert len(result) > 0
    # flights_data: JFK->100, LAX->200, SFO->300
    # short_count: JFK->1 (100<200), LAX->0 (200 not <200), SFO->0 (300 not <200)
    jfk = result[result["origin"] == "JFK"].iloc[0]
    assert jfk["short_count"] == 1

    lax = result[result["origin"] == "LAX"].iloc[0]
    assert lax["short_count"] == 0


def test_tagged_roundtrip_percent_of_total():
    """Percent-of-total pattern survives to_tagged -> from_tagged with correct values."""
    import pandas as pd

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({"carrier": ["AA", "AA", "UA", "DL", "DL", "DL"]})
    carriers = pd.DataFrame(
        {"code": ["AA", "UA", "DL"], "nickname": ["American", "United", "Delta"]},
    )
    f_tbl = con.create_table("flights_pct", flights)
    c_tbl = con.create_table("carriers_pct", carriers)

    flights_st = to_semantic_table(f_tbl, "flights_pct").with_measures(
        flight_count=lambda t: t.count(),
    )
    carriers_st = to_semantic_table(c_tbl, "carriers_pct").with_dimensions(
        code=lambda t: t.code,
        nickname=lambda t: t.nickname,
    )

    joined = (
        flights_st.join_many(carriers_st, lambda f, c: f.carrier == c.code)
        .with_dimensions(nickname=lambda t: t.nickname)
        .with_measures(
            percent_of_total=lambda t: t["flights_pct.flight_count"] / t.all(t["flights_pct.flight_count"]),
        )
    )

    tagged = to_tagged(joined)
    reconstructed = from_tagged(tagged)

    df = (
        reconstructed
        .group_by("nickname")
        .aggregate("percent_of_total")
        .order_by("nickname")
        .execute()
    )

    expected = {"American": 2 / 6, "Delta": 3 / 6, "United": 1 / 6}
    got = dict(zip(df.nickname, df.percent_of_total, strict=False))
    for k, v in expected.items():
        assert pytest.approx(v) == got[k]
    assert pytest.approx(sum(got.values())) == 1.0


def test_tagged_roundtrip_join_with_multiple_measures():
    """Joined semantic tables with multiple measures survive round-trip."""
    import pandas as pd

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({
        "carrier": ["AA", "AA", "UA", "DL", "DL", "DL"],
        "distance": [100, 200, 300, 400, 500, 600],
    })
    carriers = pd.DataFrame(
        {"code": ["AA", "UA", "DL"], "nickname": ["American", "United", "Delta"]},
    )
    f_tbl = con.create_table("flights_join", flights)
    c_tbl = con.create_table("carriers_join", carriers)

    flights_st = to_semantic_table(f_tbl, "flights_join").with_measures(
        flight_count=lambda t: t.count(),
        total_distance=lambda t: t.distance.sum(),
    )
    carriers_st = to_semantic_table(c_tbl, "carriers_join").with_dimensions(
        code=lambda t: t.code,
        nickname=lambda t: t.nickname,
    )

    joined = (
        flights_st.join_many(carriers_st, lambda f, c: f.carrier == c.code)
        .with_dimensions(nickname=lambda t: t.nickname)
    )

    tagged = to_tagged(joined)
    reconstructed = from_tagged(tagged)

    df = (
        reconstructed
        .group_by("nickname")
        .aggregate("flights_join.flight_count", "flights_join.total_distance")
        .order_by("nickname")
        .execute()
    )

    assert len(df) == 3
    got = dict(zip(df.nickname, df["flights_join.flight_count"], strict=False))
    assert got["American"] == 2
    assert got["United"] == 1
    assert got["Delta"] == 3

    got_dist = dict(zip(df.nickname, df["flights_join.total_distance"], strict=False))
    assert got_dist["American"] == 300  # 100+200
    assert got_dist["United"] == 300
    assert got_dist["Delta"] == 1500  # 400+500+600


def test_tagged_roundtrip_filter_aggregate_mutate_pipeline():
    """Complex pipeline: filter -> aggregate -> mutate with value verification."""
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    data = {
        "origin": ["JFK", "JFK", "LAX", "LAX", "SFO"],
        "distance": [100, 400, 200, 300, 500],
    }
    tbl = con.create_table("flights_pipeline", data)

    flights = (
        to_semantic_table(tbl, name="flights_pipeline")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(
            total_distance=lambda t: t.distance.sum(),
            flight_count=lambda t: t.count(),
        )
        .filter(lambda t: t.distance >= 200)
    )

    pipeline = (
        flights
        .group_by("origin")
        .aggregate("total_distance", "flight_count")
        .mutate(avg_dist=lambda t: t.total_distance / t.flight_count)
        .order_by(lambda t: t.avg_dist.desc())
    )

    tagged = to_tagged(pipeline)
    reconstructed = from_tagged(tagged)
    result = reconstructed.execute()

    assert len(result) > 0
    assert "avg_dist" in result.columns

    # After filtering >= 200: JFK->400, LAX->200,300, SFO->500
    # avg_dist: JFK->400, LAX->250, SFO->500
    # Ordered desc: SFO(500), JFK(400), LAX(250)
    assert result.iloc[0]["origin"] == "SFO"
    assert pytest.approx(result.iloc[0]["avg_dist"]) == 500.0


def test_tagged_roundtrip_ifelse_with_value_verification(flights_data):
    """ifelse measure round-trips with correct computed values."""
    import xorq.api as xo

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    flights = (
        to_semantic_table(flights_data, name="flights")
        .with_dimensions(origin=lambda t: t.origin)
        .with_measures(
            short_flag_sum=lambda t: xo.ifelse(t.distance < 200, 1, 0).sum(),
        )
    )

    tagged = to_tagged(flights)
    reconstructed = from_tagged(tagged)

    result = (
        reconstructed
        .group_by("origin")
        .aggregate("short_flag_sum")
        .order_by("origin")
        .execute()
    )

    # flights_data: JFK->100, LAX->200, SFO->300 (1 row each)
    assert len(result) == 3
    jfk = result[result["origin"] == "JFK"].iloc[0]
    assert jfk["short_flag_sum"] == 1  # 100 < 200

    lax = result[result["origin"] == "LAX"].iloc[0]
    assert lax["short_flag_sum"] == 0  # 200 is NOT < 200


def test_tagged_roundtrip_join_cross():
    """Cross join (no predicate) metadata survives round-trip."""
    import pandas as pd

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    colors = pd.DataFrame({"color": ["red", "blue"]})
    sizes = pd.DataFrame({"size": ["S", "M", "L"]})
    c_tbl = con.create_table("colors_cross", colors)
    s_tbl = con.create_table("sizes_cross", sizes)

    colors_st = to_semantic_table(c_tbl, "colors_cross").with_dimensions(
        color=lambda t: t.color,
    )
    sizes_st = to_semantic_table(s_tbl, "sizes_cross").with_dimensions(
        size=lambda t: t.size,
    )

    joined = colors_st.join_cross(sizes_st)
    tagged = to_tagged(joined)
    reconstructed = from_tagged(tagged)

    op = reconstructed.op()
    assert type(op).__name__ == "SemanticJoinOp"
    assert op.how == "cross"
    assert op.on is None


def test_tagged_roundtrip_join_filter_aggregate():
    """Filter and aggregate after join survive round-trip."""
    import pandas as pd

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    orders = pd.DataFrame({
        "customer_id": [1, 1, 2, 2, 3],
        "amount": [10, 20, 30, 40, 50],
    })
    customers = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
    })
    o_tbl = con.create_table("orders_jfa", orders)
    c_tbl = con.create_table("customers_jfa", customers)

    orders_st = to_semantic_table(o_tbl, "orders_jfa").with_measures(
        total_amount=lambda t: t.amount.sum(),
    )
    customers_st = to_semantic_table(c_tbl, "customers_jfa").with_dimensions(
        id=lambda t: t.id,
        name=lambda t: t.name,
    )

    joined = (
        orders_st.join_many(customers_st, lambda o, c: o.customer_id == c.id)
        .with_dimensions(name=lambda t: t.name)
        .filter(lambda t: t.name != "Carol")
    )

    tagged = to_tagged(joined)
    reconstructed = from_tagged(tagged)

    df = (
        reconstructed
        .group_by("name")
        .aggregate("orders_jfa.total_amount")
        .order_by("name")
        .execute()
    )

    assert len(df) == 2
    got = dict(zip(df.name, df["orders_jfa.total_amount"], strict=False))
    assert got["Alice"] == 30
    assert got["Bob"] == 70


def test_tagged_roundtrip_join_inner():
    """Inner join survives round-trip and excludes non-matching rows."""
    import pandas as pd

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    left = pd.DataFrame({"key": [1, 2, 3], "val": ["a", "b", "c"]})
    right = pd.DataFrame({"key": [2, 3, 4], "label": ["x", "y", "z"]})
    l_tbl = con.create_table("left_inner", left)
    r_tbl = con.create_table("right_inner", right)

    left_st = to_semantic_table(l_tbl, "left_inner").with_dimensions(
        val=lambda t: t.val,
    ).with_measures(row_count=lambda t: t.count())
    right_st = to_semantic_table(r_tbl, "right_inner").with_dimensions(
        key=lambda t: t.key,
        label=lambda t: t.label,
    )

    joined = (
        left_st.join_many(right_st, lambda l, r: l.key == r.key, how="inner")
        .with_dimensions(label=lambda t: t.label)
    )

    tagged = to_tagged(joined)
    reconstructed = from_tagged(tagged)

    df = reconstructed.group_by("label").aggregate("left_inner.row_count").order_by("label").execute()
    assert len(df) == 2  # keys 2 and 3 match
    got = dict(zip(df.label, df["left_inner.row_count"], strict=False))
    assert got["x"] == 1
    assert got["y"] == 1


def test_tagged_roundtrip_join_one_preserves_predicate():
    """join_one round-trip preserves the join predicate (not a cross join).

    Regression test: previously both sides of the join received the full
    joined xorq expression during reconstruction, causing a self-join
    where the predicate became a tautology (cross join).
    """
    import pandas as pd

    from boring_semantic_layer import Dimension, Measure
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    flights = pd.DataFrame({
        "origin": ["SEA", "SEA", "LAX", "LAX", "ORD"],
        "carrier": ["AA", "UA", "AA", "DL", "UA"],
    })
    carriers = pd.DataFrame({
        "code": ["AA", "UA", "DL"],
        "nickname": ["American", "United", "Delta"],
    })
    f_tbl = con.create_table("flights_jo", flights)
    c_tbl = con.create_table("carriers_jo", carriers)

    flights_st = (
        to_semantic_table(f_tbl, name="flights_jo")
        .with_dimensions(
            origin=Dimension(expr=lambda t: t.origin, description="Origin"),
            carrier=Dimension(expr=lambda t: t.carrier, description="Carrier"),
        )
        .with_measures(
            flight_count=Measure(expr=lambda t: t.count(), description="Count"),
        )
    )
    carriers_st = (
        to_semantic_table(c_tbl, name="carriers_jo")
        .with_dimensions(
            code=Dimension(expr=lambda t: t.code, description="Code"),
            nickname=Dimension(expr=lambda t: t.nickname, description="Nickname"),
        )
        .with_measures(
            carrier_count=Measure(expr=lambda t: t.count(), description="Count"),
        )
    )

    joined = flights_st.join_one(carriers_st, on=lambda f, c: f.carrier == c.code)

    # Baseline
    baseline = (
        joined
        .group_by("carriers_jo.nickname")
        .aggregate("flights_jo.flight_count")
        .order_by("carriers_jo.nickname")
        .execute()
    )

    # Round-trip
    tagged = to_tagged(joined)
    reconstructed = from_tagged(tagged)
    result = (
        reconstructed
        .group_by("carriers_jo.nickname")
        .aggregate("flights_jo.flight_count")
        .order_by("carriers_jo.nickname")
        .execute()
    )

    assert len(result) == 3
    got = dict(zip(result["carriers_jo.nickname"], result["flights_jo.flight_count"], strict=False))
    assert got["American"] == 2
    assert got["United"] == 2
    assert got["Delta"] == 1

    # Verify baseline and round-trip match exactly
    assert list(baseline["carriers_jo.nickname"]) == list(result["carriers_jo.nickname"])
    assert list(baseline["flights_jo.flight_count"]) == list(result["flights_jo.flight_count"])


def test_tagged_roundtrip_join_one_left_join():
    """join_one with left join preserves NULLs for non-matching rows."""
    import pandas as pd

    from boring_semantic_layer import Dimension, Measure
    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    orders = pd.DataFrame({
        "order_id": [1, 2, 3, 4],
        "product_id": [10, 20, 30, 99],  # 99 has no matching product
    })
    products = pd.DataFrame({
        "pid": [10, 20, 30],
        "name": ["Widget", "Gadget", "Gizmo"],
    })
    o_tbl = con.create_table("orders_lj", orders)
    p_tbl = con.create_table("products_lj", products)

    orders_st = (
        to_semantic_table(o_tbl, name="orders_lj")
        .with_dimensions(
            order_id=Dimension(expr=lambda t: t.order_id, description="Order ID"),
            product_id=Dimension(expr=lambda t: t.product_id, description="Product ID"),
        )
        .with_measures(
            order_count=Measure(expr=lambda t: t.count(), description="Count"),
        )
    )
    products_st = (
        to_semantic_table(p_tbl, name="products_lj")
        .with_dimensions(
            pid=Dimension(expr=lambda t: t.pid, description="Product ID"),
            name=Dimension(expr=lambda t: t.name, description="Name"),
        )
    )

    joined = orders_st.join_one(products_st, on=lambda o, p: o.product_id == p.pid, how="left")

    tagged = to_tagged(joined)
    reconstructed = from_tagged(tagged)

    result = (
        reconstructed
        .group_by("products_lj.name")
        .aggregate("orders_lj.order_count")
        .order_by("products_lj.name")
        .execute()
    )

    got = dict(zip(result["products_lj.name"], result["orders_lj.order_count"], strict=False))
    assert got["Gadget"] == 1
    assert got["Gizmo"] == 1
    assert got["Widget"] == 1
    # Left join: the unmatched order (product_id=99) shows up with NULL name
    assert len(result) == 4


def test_tagged_roundtrip_join_many_without_with_dimensions():
    """join_many without .with_dimensions() round-trips correctly.

    When there is no .with_dimensions() wrapper, the top-level op is
    SemanticJoinOp and reconstruction must split the join expression.
    """
    import pandas as pd

    from boring_semantic_layer.xorq_convert import from_tagged, to_tagged

    con = ibis.duckdb.connect(":memory:")
    employees = pd.DataFrame({
        "emp_id": [1, 2, 3],
        "dept_id": [10, 10, 20],
        "salary": [50000, 60000, 70000],
    })
    departments = pd.DataFrame({
        "dept_id": [10, 20],
        "dept_name": ["Engineering", "Sales"],
    })
    e_tbl = con.create_table("employees_jm", employees)
    d_tbl = con.create_table("departments_jm", departments)

    emp_st = (
        to_semantic_table(e_tbl, "employees_jm")
        .with_dimensions(dept_id=lambda t: t.dept_id)
        .with_measures(
            headcount=lambda t: t.count(),
            total_salary=lambda t: t.salary.sum(),
        )
    )
    dept_st = (
        to_semantic_table(d_tbl, "departments_jm")
        .with_dimensions(
            dept_id=lambda t: t.dept_id,
            dept_name=lambda t: t.dept_name,
        )
    )

    # join_many without .with_dimensions() → top-level SemanticJoinOp
    joined = emp_st.join_many(dept_st, lambda e, d: e.dept_id == d.dept_id)

    tagged = to_tagged(joined)
    reconstructed = from_tagged(tagged)

    df = (
        reconstructed
        .group_by("departments_jm.dept_name")
        .aggregate("employees_jm.headcount", "employees_jm.total_salary")
        .order_by("departments_jm.dept_name")
        .execute()
    )

    assert len(df) == 2
    got_hc = dict(zip(df["departments_jm.dept_name"], df["employees_jm.headcount"], strict=False))
    got_sal = dict(zip(df["departments_jm.dept_name"], df["employees_jm.total_salary"], strict=False))
    assert got_hc["Engineering"] == 2
    assert got_hc["Sales"] == 1
    assert got_sal["Engineering"] == 110000
    assert got_sal["Sales"] == 70000
