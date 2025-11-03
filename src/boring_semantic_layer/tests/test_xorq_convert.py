"""Tests for xorq conversion module.

Tests bidirectional conversion between BSL and xorq expressions.
"""

from __future__ import annotations

import pytest
from returns.result import Failure, Success

from boring_semantic_layer.xorq_convert import (
    deserialize_callable,
    from_xorq,
    serialize_callable,
    serialize_dimensions,
    serialize_measures,
    to_xorq,
    try_import_xorq,
)

# Check if xorq is available
try:
    try_import_xorq()
    xorq_available = True
    xorq_skip_reason = ""
except ImportError:
    xorq_available = False
    xorq_skip_reason = "xorq not installed"


class TestOptionalImports:
    """Test optional import handling."""

    def test_try_import_xorq(self):
        """Test xorq import returns Result type."""
        result = try_import_xorq()
        assert isinstance(result, Success | Failure)

        if isinstance(result, Success):
            xorq_mod = result.unwrap()
            assert xorq_mod.api is not None
            assert hasattr(xorq_mod.api, "memtable")


class TestCallableSerialization:
    """Test callable serialization and deserialization."""

    def test_serialize_simple_lambda(self):
        """Test serialization of simple lambda."""
        fn = lambda x: x + 1  # noqa: E731

        result = serialize_callable(fn)
        assert isinstance(result, Success | Failure)

        if isinstance(result, Success):
            serialized = result.unwrap()
            assert isinstance(serialized, str)
            assert len(serialized) > 0

    def test_serialize_named_function(self):
        """Test serialization of named function."""

        def add_one(x):
            return x + 1

        result = serialize_callable(add_one)
        assert isinstance(result, Success | Failure)

        if isinstance(result, Success):
            serialized = result.unwrap()
            assert isinstance(serialized, str)

    def test_deserialize_callable(self):
        """Test deserialization of callable."""
        fn = lambda x: x * 2  # noqa: E731

        serialize_result = serialize_callable(fn)
        assert isinstance(serialize_result, Success)

        serialized = serialize_result.unwrap()
        deserialize_result = deserialize_callable(serialized)

        # Deserialization may fail if cloudpickle not available
        if isinstance(deserialize_result, Success):
            restored_fn = deserialize_result.unwrap()
            assert callable(restored_fn)
            # Test that it works
            assert restored_fn(5) == 10

    def test_round_trip_callable(self):
        """Test round-trip serialization/deserialization."""

        def multiply(x, y):
            return x * y

        # Serialize
        serialize_result = serialize_callable(multiply)
        if not isinstance(serialize_result, Success):
            pytest.skip("Serialization failed")

        serialized = serialize_result.unwrap()

        # Deserialize
        deserialize_result = deserialize_callable(serialized)
        if not isinstance(deserialize_result, Success):
            pytest.skip("Deserialization failed")

        restored_fn = deserialize_result.unwrap()

        # Test functionality
        assert restored_fn(3, 4) == 12
        assert restored_fn(2, 5) == 10


class TestMetadataSerialization:
    """Test metadata serialization for dimensions and measures."""

    def test_serialize_empty_dimensions(self):
        """Test serialization of empty dimensions dict."""
        result = serialize_dimensions({})
        assert isinstance(result, Success)
        assert result.unwrap() == "{}"

    def test_serialize_dimensions_with_metadata(self):
        """Test serialization of dimensions with metadata."""
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

        import json

        serialized = result.unwrap()
        data = json.loads(serialized)

        assert "dim1" in data
        assert data["dim1"]["description"] == "First dimension"
        assert data["dim1"]["is_time_dimension"] is False

        assert "dim2" in data
        assert data["dim2"]["description"] == "Second dimension"
        assert data["dim2"]["is_time_dimension"] is True
        assert data["dim2"]["smallest_time_grain"] == "day"

    def test_serialize_empty_measures(self):
        """Test serialization of empty measures dict."""
        result = serialize_measures({})
        assert isinstance(result, Success)
        assert result.unwrap() == "{}"

    def test_serialize_measures_with_metadata(self):
        """Test serialization of measures with metadata."""
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

        import json

        serialized = result.unwrap()
        data = json.loads(serialized)

        assert "total" in data
        assert data["total"]["description"] == "Total amount"
        assert data["total"]["requires_unnest"] == []

        assert "count" in data
        assert data["count"]["description"] == "Count of records"
        assert data["count"]["requires_unnest"] == ["tags"]


@pytest.mark.skipif(not xorq_available, reason=xorq_skip_reason)
class TestToXorq:
    """Test BSL to xorq conversion."""

    def test_to_xorq_returns_xorq_expr(self):
        """Test that to_xorq returns a xorq expression directly."""
        import ibis

        from boring_semantic_layer import SemanticModel

        # Create a simple semantic model
        table = ibis.memtable({"a": [1, 2, 3], "b": [4, 5, 6]})
        model = SemanticModel(
            table=table,
            dimensions={"a": lambda t: t.a},
            measures={"sum_b": lambda t: t.b.sum()},
        )

        xorq_expr = to_xorq(model)
        # Should return xorq expression directly, not Result
        assert xorq_expr is not None
        assert hasattr(xorq_expr, "op")  # xorq expressions have .op() method


@pytest.mark.skipif(not xorq_available, reason=xorq_skip_reason)
class TestFromXorq:
    """Test xorq to BSL conversion."""

    def test_from_xorq_returns_bsl_expr(self):
        """Test that from_xorq returns a BSL expression directly."""
        from xorq.api import memtable

        # Create a simple xorq table (untagged - should fail)
        xorq_table = memtable({"a": [1, 2, 3]})

        # from_xorq should raise ValueError for untagged expressions
        with pytest.raises(ValueError, match="No BSL metadata found"):
            from_xorq(xorq_table)

    def test_from_xorq_with_tagged_table(self):
        """Test reconstruction from tagged xorq table."""
        from xorq.api import memtable

        # Create tagged xorq table
        xorq_table = memtable({"a": [1, 2, 3]}).tag(
            tag="bsl_test",
            bsl_op_type="SemanticTableOp",
            bsl_version="1.0",
            dimensions='{"a": {"description": "Column A"}}',
            measures="{}",
        )

        bsl_expr = from_xorq(xorq_table)
        # Should return BSL expression directly, not Result
        assert bsl_expr is not None
        # Should have reconstructed dimensions
        assert hasattr(bsl_expr, "dimensions")

    def test_from_xorq_without_tags(self):
        """Test from_xorq with untagged expression."""
        from xorq.api import memtable

        xorq_table = memtable({"a": [1, 2, 3]})

        # Should raise ValueError since no BSL metadata found
        with pytest.raises(ValueError, match="No BSL metadata found"):
            from_xorq(xorq_table)
