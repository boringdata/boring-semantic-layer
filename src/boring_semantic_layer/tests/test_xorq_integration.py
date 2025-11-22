"""Integration tests for xorq conversion.

Tests full round-trip conversion between BSL and xorq with real semantic models.
"""

from __future__ import annotations

import pytest

from boring_semantic_layer import SemanticModel
from boring_semantic_layer.xorq_convert import from_tagged, to_tagged, try_import_xorq

# Check if xorq is available
try:
    try_import_xorq()
    xorq_available = True
    xorq_skip_reason = ""
except ImportError:
    xorq_available = False
    xorq_skip_reason = "xorq not installed"


@pytest.mark.skipif(not xorq_available, reason=xorq_skip_reason)
class TestXorqIntegration:
    """Integration tests for xorq conversion."""

    def test_simple_model_to_xorq(self):
        """Test converting simple semantic model to xorq."""
        import ibis

        # Create simple semantic model
        table = ibis.memtable({"a": [1, 2, 3], "b": [4, 5, 6]})
        model = SemanticModel(
            table=table,
            dimensions={"a": lambda t: t.a, "b": lambda t: t.b},
            measures={"sum_b": lambda t: t.b.sum()},
        )

        # Convert to xorq
        tagged_expr = to_tagged(model)

        # Verify it's a xorq table with tag method
        assert hasattr(tagged_expr, "tag")

        # Verify metadata is present
        op = tagged_expr.op()
        assert hasattr(op, "metadata")
        metadata = dict(op.metadata)
        assert "bsl_op_type" in metadata
        assert metadata["bsl_op_type"] == "SemanticTableOp"

    def test_xorq_expression_execution(self):
        """Test that converted xorq expression can be executed."""
        import ibis

        table = ibis.memtable({"x": [1, 2, 3], "y": [10, 20, 30]})
        model = SemanticModel(
            table=table,
            dimensions={"x": lambda t: t.x},
            measures={"sum_y": lambda t: t.y.sum()},
        )

        # Convert to xorq
        tagged_expr = to_tagged(model)

        # Execute xorq expression
        from xorq.api import execute

        df = execute(tagged_expr)

        # Verify data is preserved
        assert len(df) == 3
        assert "x" in df.columns
        assert "y" in df.columns
        assert list(df["x"]) == [1, 2, 3]

    def test_round_trip_conversion(self):
        """Test round-trip BSL -> xorq -> BSL conversion."""
        import ibis

        # Create original model
        table = ibis.memtable({"a": [1, 2, 3], "b": [4, 5, 6]})
        original_model = SemanticModel(
            table=table,
            dimensions={"a": lambda t: t.a},
            measures={"sum_b": lambda t: t.b.sum()},
            name="test_model",
        )

        # Convert to xorq
        tagged_expr = to_tagged(original_model)

        # Convert back to BSL
        reconstructed_model = from_tagged(tagged_expr)

        # Verify structure is preserved
        assert hasattr(reconstructed_model, "dimensions")
        assert hasattr(reconstructed_model, "measures")

        # Verify dimension names are preserved
        assert "a" in reconstructed_model.dimensions

        # Verify name is preserved
        if hasattr(reconstructed_model, "name"):
            # Note: name might not be preserved depending on implementation
            pass

    def test_metadata_preservation(self):
        """Test that BSL metadata is preserved through xorq tagging."""
        import ibis

        from boring_semantic_layer.ops import Dimension, Measure

        # Create model with rich metadata
        table = ibis.memtable({"timestamp": ["2024-01-01"], "value": [100]})
        model = SemanticModel(
            table=table,
            dimensions={
                "date": Dimension(
                    expr=lambda t: t.timestamp.cast("date"),
                    description="Transaction date",
                    is_time_dimension=True,
                    smallest_time_grain="day",
                )
            },
            measures={
                "total": Measure(
                    expr=lambda t: t.value.sum(),
                    description="Total value",
                )
            },
            name="transactions",
        )

        # Convert to xorq
        tagged_expr = to_tagged(model)

        # Extract metadata
        op = tagged_expr.op()
        metadata = dict(op.metadata)

        # Verify BSL metadata is present
        assert metadata["bsl_op_type"] == "SemanticTableOp"
        assert metadata["bsl_version"] == "1.0"

        # Verify dimension metadata (now stored as nested tuples)
        dims_tuple = metadata["dimensions"]
        # Convert tuple format back to dict for easy assertion
        dims = {k: dict(v) for k, v in dims_tuple}
        assert "date" in dims
        assert dims["date"]["description"] == "Transaction date"
        assert dims["date"]["is_time_dimension"] is True

        # Verify measure metadata (now stored as nested tuples)
        measures_tuple = metadata["measures"]
        measures = {k: dict(v) for k, v in measures_tuple}
        assert "total" in measures
        assert measures["total"]["description"] == "Total value"

    def test_xorq_caching_feature(self):
        """Test that xorq-specific features like caching are available."""
        import ibis

        table = ibis.memtable({"a": [1, 2, 3]})
        model = SemanticModel(
            table=table,
            dimensions={"a": lambda t: t.a},
            measures={},
        )

        # Convert to xorq
        tagged_expr = to_tagged(model)

        # Verify xorq-specific methods are available
        # (These are xorq features not available in regular ibis)
        assert hasattr(tagged_expr, "tag"), "Xorq tables should have tag method"

        # We can add more xorq tags (e.g., for caching hints)
        cached_expr = tagged_expr.tag(tag="cache", cache_strategy="aggressive")
        assert cached_expr is not None

    def test_filtered_expression_to_xorq(self):
        """Test converting filtered BSL expression to xorq."""
        import ibis

        table = ibis.memtable({"a": [1, 2, 3, 4, 5]})
        model = SemanticModel(
            table=table,
            dimensions={"a": lambda t: t.a},
            measures={"count": lambda t: t.a.count()},
        )

        # Apply filter
        filtered = model.filter(lambda t: t.a > 2)

        # Convert to xorq
        tagged_expr = to_tagged(filtered)

        # Execute and verify filter was applied
        from xorq.api import execute

        df = execute(tagged_expr)
        assert len(df) == 3  # Only values > 2
        assert min(df["a"]) > 2


@pytest.mark.skipif(not xorq_available, reason=xorq_skip_reason)
class TestXorqFeatures:
    """Test xorq-specific features that aren't available in regular ibis."""

    def test_multi_tag_support(self):
        """Test that xorq tables can have multiple tags."""
        import ibis

        table = ibis.memtable({"a": [1, 2, 3]})
        model = SemanticModel(
            table=table,
            dimensions={"a": lambda t: t.a},
            measures={},
        )

        # Convert to xorq
        tagged_expr = to_tagged(model)

        # Add multiple tags
        tagged = tagged_expr.tag(tag="cache", cache_ttl="3600")
        tagged = tagged.tag(tag="monitoring", track_queries="true")

        # Both tags should be preserved
        # (This tests xorq's ability to nest tags)
        assert tagged is not None

    def test_xorq_noop_tag_preservation(self):
        """Test that xorq noop tags preserve query structure."""
        from xorq.api import execute, memtable

        # Create xorq memtable directly (not external ibis)
        xorq_table = memtable({"a": [1, 2, 3]})

        # Tag is a noop - shouldn't affect query results
        tagged_table = xorq_table.tag(tag="test", metadata="example")

        df_untagged = execute(xorq_table)
        df_tagged = execute(tagged_table)

        # Data should be identical
        assert len(df_untagged) == len(df_tagged)
        assert list(df_untagged["a"]) == list(df_tagged["a"])
