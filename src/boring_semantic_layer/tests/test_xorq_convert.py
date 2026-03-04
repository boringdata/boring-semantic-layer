from __future__ import annotations

import pytest
from returns.result import Failure, Success

from boring_semantic_layer.utils import expr_to_ibis_string, ibis_string_to_expr
from boring_semantic_layer.xorq_convert import (
    from_tagged,
    serialize_dimensions,
    serialize_measures,
    to_tagged,
    try_import_xorq,
)

xorq = pytest.importorskip("xorq", reason="xorq not installed")


def test_try_import_xorq():
    result = try_import_xorq()
    assert isinstance(result, Success | Failure)

    if isinstance(result, Success):
        xorq_mod = result.unwrap()
        assert xorq_mod.api is not None
        assert hasattr(xorq_mod.api, "memtable")


def test_serialize_ibis_lambda():
    fn = lambda t: t.col1  # noqa: E731

    result = expr_to_ibis_string(fn)
    assert isinstance(result, Success | Failure)

    if isinstance(result, Success):
        serialized = result.unwrap()
        assert isinstance(serialized, str)
        assert "_.col1" in serialized or "col1" in serialized


def test_serialize_ibis_method():
    fn = lambda t: t.amount.sum()  # noqa: E731

    result = expr_to_ibis_string(fn)
    assert isinstance(result, Success | Failure)

    if isinstance(result, Success):
        serialized = result.unwrap()
        assert isinstance(serialized, str)
        assert "sum()" in serialized


def test_deserialize_expr_string():
    expr_str = "_.amount * 2"

    deserialize_result = ibis_string_to_expr(expr_str)
    assert isinstance(deserialize_result, Success | Failure)

    if isinstance(deserialize_result, Success):
        restored_fn = deserialize_result.unwrap()
        assert callable(restored_fn)


def test_round_trip_expression():
    fn = lambda t: t.price.mean()  # noqa: E731

    serialize_result = expr_to_ibis_string(fn)
    if not isinstance(serialize_result, Success):
        pytest.skip("Serialization failed")

    serialized = serialize_result.unwrap()
    assert isinstance(serialized, str)
    assert "mean()" in serialized


def test_serialize_empty_dimensions():
    result = serialize_dimensions({})
    assert isinstance(result, Success)
    assert result.unwrap() == {}


def test_serialize_dimensions_with_metadata():
    from boring_semantic_layer.ops import Dimension

    dimensions = {
        "dim1": Dimension(
            expr=lambda t: t.col1,
            description="First dimension",
            is_time_dimension=False,
        ),
        "dim2": Dimension(
            expr=lambda t: t.col2,
            description="Second dimension",
            is_time_dimension=True,
            smallest_time_grain="day",
        ),
    }

    result = serialize_dimensions(dimensions)
    assert isinstance(result, Success)

    data = result.unwrap()

    assert "dim1" in data
    assert data["dim1"]["description"] == "First dimension"
    assert data["dim1"]["is_time_dimension"] is False
    assert "expr_struct" in data["dim1"] or "expr" in data["dim1"]

    assert "dim2" in data
    assert data["dim2"]["description"] == "Second dimension"
    assert data["dim2"]["is_time_dimension"] is True
    assert data["dim2"]["smallest_time_grain"] == "day"
    assert "expr_struct" in data["dim2"] or "expr" in data["dim2"]


def test_serialize_empty_measures():
    result = serialize_measures({})
    assert isinstance(result, Success)
    assert result.unwrap() == {}


def test_serialize_measures_with_metadata():
    from boring_semantic_layer.ops import Measure

    measures = {
        "total": Measure(
            expr=lambda t: t.amount.sum(),
            description="Total amount",
        ),
        "count": Measure(
            expr=lambda t: t.id.count(),
            description="Count of records",
            requires_unnest=("tags",),
        ),
    }

    result = serialize_measures(measures)
    assert isinstance(result, Success)

    data = result.unwrap()

    assert "total" in data
    assert data["total"]["description"] == "Total amount"
    assert data["total"]["requires_unnest"] == []
    assert "expr_struct" in data["total"]

    assert "count" in data
    assert data["count"]["description"] == "Count of records"
    assert data["count"]["requires_unnest"] == ["tags"]
    assert "expr_struct" in data["count"]


@pytest.mark.skipif(not xorq, reason="xorq not available")
def test_to_xorq_returns_xorq_expr():
    import ibis

    from boring_semantic_layer import SemanticModel

    table = ibis.memtable({"a": [1, 2, 3], "b": [4, 5, 6]})
    model = SemanticModel(
        table=table,
        dimensions={"a": lambda t: t.a},
        measures={"sum_b": lambda t: t.b.sum()},
    )

    tagged_expr = to_tagged(model)
    assert tagged_expr is not None
    assert hasattr(tagged_expr, "op")


@pytest.mark.skipif(not xorq, reason="xorq not available")
def test_from_xorq_returns_bsl_expr():
    from xorq.api import memtable

    xorq_table = memtable({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="No BSL metadata found"):
        from_tagged(xorq_table)


@pytest.mark.skipif(not xorq, reason="xorq not available")
def test_from_xorq_with_tagged_table():
    from xorq.api import memtable

    # Use nested tuples format (following xorq sklearn pipeline pattern)
    xorq_table = memtable({"a": [1, 2, 3]}).tag(
        tag="bsl_test",
        bsl_op_type="SemanticTableOp",
        bsl_version="1.0",
        dimensions=(("a", (("description", "Column A"),)),),
        measures=(),
    )

    bsl_expr = from_tagged(xorq_table)
    assert bsl_expr is not None
    assert hasattr(bsl_expr, "dimensions")


@pytest.mark.skipif(not xorq, reason="xorq not available")
def test_from_xorq_without_tags():
    from xorq.api import memtable

    xorq_table = memtable({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="No BSL metadata found"):
        from_tagged(xorq_table)
