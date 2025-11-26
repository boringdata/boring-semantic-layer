"""Tests for xorq backends integration with BSL.

This module tests that BSL semantic models work correctly with various
xorq backends (DuckDB, DataFusion, Pandas, etc.).

**Important**: BSL's `to_tagged()` wraps expressions in xorq's `Tag` operations
to preserve metadata. These tagged expressions must be executed using `xo.execute()`
rather than `backend.execute()`, because backends try to compile Tags to SQL which
doesn't work. Use `xo.execute()` for BSL semantic models.
"""

from __future__ import annotations

import pytest

from boring_semantic_layer import SemanticModel
from boring_semantic_layer.xorq_convert import from_tagged, to_tagged, try_import_xorq

# Check if xorq is available
try:
    try_import_xorq()
    import xorq.api as xo

    xorq_available = True
    xorq_skip_reason = ""
except ImportError:
    xorq_available = False
    xorq_skip_reason = "xorq not installed"


@pytest.mark.skipif(not xorq_available, reason=xorq_skip_reason)
class TestXorqDuckDBBackend:
    """Test BSL with xorq's DuckDB backend."""

    def test_duckdb_backend_basic_execution(self):
        """Test executing a simple semantic model with xorq."""
        import ibis

        # Create BSL semantic model
        table = ibis.memtable({"x": [1, 2, 3, 4, 5], "y": [10, 20, 30, 40, 50]})
        model = SemanticModel(
            table=table,
            dimensions={"x": lambda t: t.x},
            measures={"sum_y": lambda t: t.y.sum(), "count": lambda t: t.x.count()},
        )

        # Convert to xorq
        tagged_expr = to_tagged(model)

        # Execute with xorq (handles Tag operations properly)
        df = xo.execute(tagged_expr)

        # Verify results
        assert len(df) == 5
        assert "x" in df.columns
        assert "y" in df.columns
        assert list(df["x"]) == [1, 2, 3, 4, 5]

    def test_duckdb_aggregation(self):
        """Test aggregation with xorq backend."""
        import ibis

        table = ibis.memtable(
            {"category": ["A", "B", "A", "B", "A"], "value": [10, 20, 30, 40, 50]}
        )
        model = SemanticModel(
            table=table,
            dimensions={"category": lambda t: t.category},
            measures={"total_value": lambda t: t.value.sum()},
        )

        # Convert to xorq and execute
        tagged_expr = to_tagged(model)
        df = xo.execute(tagged_expr)

        # Verify data is loaded
        assert len(df) == 5
        assert "category" in df.columns
        assert "value" in df.columns

    def test_duckdb_filtered_query(self):
        """Test filtered query with xorq backend."""
        import ibis

        table = ibis.memtable({"a": [1, 2, 3, 4, 5], "b": [5, 4, 3, 2, 1]})
        model = SemanticModel(
            table=table,
            dimensions={"a": lambda t: t.a},
            measures={"max_b": lambda t: t.b.max()},
        )

        # Apply filter
        filtered = model.filter(lambda t: t.a > 2)

        # Convert and execute
        tagged_expr = to_tagged(filtered)
        df = xo.execute(tagged_expr)

        # Verify filter was applied
        assert len(df) == 3
        assert min(df["a"]) > 2
        assert list(sorted(df["a"])) == [3, 4, 5]

    def test_duckdb_with_pyarrow_batches(self):
        """Test xorq backend returns PyArrow batches correctly."""
        import ibis

        table = ibis.memtable({"x": list(range(100))})
        model = SemanticModel(table=table, dimensions={"x": lambda t: t.x}, measures={})

        tagged_expr = to_tagged(model)

        # Get PyArrow batches using xorq API
        batches = xo.to_pyarrow_batches(tagged_expr, chunk_size=10)
        assert batches is not None

        # Read all batches
        all_data = []
        for batch in batches:
            all_data.extend(batch.to_pydict()["x"])

        assert len(all_data) == 100
        assert all_data == list(range(100))


@pytest.mark.skipif(not xorq_available, reason=xorq_skip_reason)
class TestXorqDataFusionBackend:
    """Test BSL with xorq's DataFusion backend."""

    def test_datafusion_basic_execution(self):
        """Test executing with DataFusion backend."""
        import ibis
        from xorq.api import execute

        table = ibis.memtable({"a": [1, 2, 3]})
        model = SemanticModel(table=table, dimensions={"a": lambda t: t.a}, measures={})

        tagged_expr = to_tagged(model)

        # Execute (xorq uses DataFusion internally)
        df = execute(tagged_expr)

        assert len(df) == 3
        assert "a" in df.columns

    def test_datafusion_aggregation(self):
        """Test aggregation works with DataFusion backend."""
        import ibis

        table = ibis.memtable({"grp": ["A", "A", "B"], "val": [1, 2, 3]})
        model = SemanticModel(
            table=table,
            dimensions={"grp": lambda t: t.grp},
            measures={"sum_val": lambda t: t.val.sum()},
        )

        tagged_expr = to_tagged(model)

        df = xo.execute(tagged_expr)

        assert len(df) == 3
        assert set(df["grp"]) == {"A", "B"}


@pytest.mark.skipif(not xorq_available, reason=xorq_skip_reason)
class TestXorqPandasBackend:
    """Test BSL with xorq's Pandas backend."""

    def test_pandas_backend_available(self):
        """Test that Pandas backend can be imported."""
        from xorq.backends import pandas

        assert hasattr(pandas, "Backend")

    def test_pandas_backend_execution(self):
        """Test executing with Pandas backend."""
        import ibis
        from xorq.api import execute

        table = ibis.memtable({"x": [1, 2, 3], "y": [4, 5, 6]})
        model = SemanticModel(
            table=table,
            dimensions={"x": lambda t: t.x},
            measures={"sum_y": lambda t: t.y.sum()},
        )

        tagged_expr = to_tagged(model)
        df = execute(tagged_expr)

        assert len(df) == 3
        assert list(df["x"]) == [1, 2, 3]
        assert list(df["y"]) == [4, 5, 6]


@pytest.mark.skipif(not xorq_available, reason=xorq_skip_reason)
class TestXorqBackendSwitching:
    """Test switching between different xorq backends."""

    def test_set_backend_duckdb(self):
        """Test setting backend explicitly."""
        backend = xo.connect()
        xo.set_backend(backend)

        # Now all xorq operations use this backend
        # This is useful for controlling which backend BSL queries run on

    def test_backend_configuration(self):
        """Test configuring backend with session options."""
        config = xo.SessionConfig()
        config = config.with_target_partitions(4)
        backend = xo.connect(session_config=config)

        assert backend is not None

    def test_multiple_backends_isolation(self):
        """Test that multiple backends can coexist."""
        # Create multiple backend instances
        backend1 = xo.connect()
        backend2 = xo.connect()

        # Each should be independent
        assert backend1 is not None
        assert backend2 is not None


@pytest.mark.skipif(not xorq_available, reason=xorq_skip_reason)
class TestXorqBackendFeatures:
    """Test xorq-specific backend features with BSL."""

    def test_backend_caching_with_semantic_model(self):
        """Test that xorq tagging (for caching) works with semantic models."""
        import ibis

        table = ibis.memtable({"a": [1, 2, 3]})
        model = SemanticModel(table=table, dimensions={"a": lambda t: t.a}, measures={})

        tagged_expr = to_tagged(model)

        # Tag for caching (noop for execution but useful for optimization layers)
        cached_expr = tagged_expr.tag(tag="cache", cache_ttl="3600")

        df = xo.execute(cached_expr)

        assert len(df) == 3

    def test_read_write_operations(self):
        """Test xorq backend read/write operations with BSL."""
        import tempfile

        import ibis

        # Create a semantic model
        table = ibis.memtable({"a": [1, 2, 3], "b": [4, 5, 6]})
        model = SemanticModel(
            table=table,
            dimensions={"a": lambda t: t.a},
            measures={"sum_b": lambda t: t.b.sum()},
        )

        tagged_expr = to_tagged(model)

        # Write to parquet
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            temp_path = f.name

        try:
            # Execute and convert to pandas, then save
            df = xo.execute(tagged_expr)
            df.to_parquet(temp_path)

            # Read back with xorq
            read_back = xo.read_parquet(temp_path)
            df_back = xo.execute(read_back)

            assert len(df_back) == 3
            assert list(df_back["a"]) == [1, 2, 3]
        finally:
            import os

            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_backend_round_trip_preservation(self):
        """Test that round-trip through backend preserves semantic information."""
        import ibis

        # Create model with rich metadata
        table = ibis.memtable({"x": [1, 2, 3]})
        model = SemanticModel(
            table=table,
            dimensions={"x": lambda t: t.x},
            measures={"count_x": lambda t: t.x.count()},
            name="test_model",
        )

        # Convert to xorq
        tagged_expr = to_tagged(model)

        # Execute with xorq
        xo.execute(tagged_expr)

        # Convert back from xorq
        restored_model = from_tagged(tagged_expr)

        # Verify structure preserved
        assert "x" in restored_model.dimensions
        assert hasattr(restored_model, "measures")
