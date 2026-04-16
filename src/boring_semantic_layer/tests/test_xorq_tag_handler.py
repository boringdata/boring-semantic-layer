"""Tests for boring_semantic_layer.xorq_integration TagHandler.

Verifies the acceptance criteria of XOR-295:

* ``bsl_tag_handler`` is a valid ``TagHandler`` with ``tag_names=("bsl",)``.
* ``extract_metadata`` returns the sidecar dict by walking nested metadata
  down to the innermost ``SemanticTableOp``.
* ``from_tag_node`` returns the base ``SemanticModel`` (not the full query
  chain) reconstructed from the innermost ``SemanticTableOp`` source.
* The handler is discoverable via the ``xorq.from_tag_node`` entry point.
"""

from __future__ import annotations

import importlib.metadata

import ibis
import pytest

from boring_semantic_layer import SemanticModel
from boring_semantic_layer.serialization import to_tagged
from boring_semantic_layer.serialization.tag_handler import (
    bsl_tag_handler,
    extract_metadata,
    from_tag_node,
)


@pytest.fixture
def simple_model():
    table = ibis.memtable({"a": [1, 2, 3], "b": [4, 5, 6]})
    return SemanticModel(
        table=table,
        dimensions={"a": lambda t: t.a, "b": lambda t: t.b},
        measures={"sum_b": lambda t: t.b.sum(), "avg_b": lambda t: t.b.mean()},
        name="simple",
    )


def _tag_node(tagged_expr):
    """Return the outermost Tag op on a BSL-tagged expression."""
    return tagged_expr.op()


# ---------------------------------------------------------------------------
# TagHandler shape
# ---------------------------------------------------------------------------


def test_bsl_tag_handler_is_valid():
    from xorq.expr.builders import TagHandler

    assert isinstance(bsl_tag_handler, TagHandler)
    assert bsl_tag_handler.tag_names == ("bsl",)
    assert bsl_tag_handler.extract_metadata is extract_metadata
    assert bsl_tag_handler.from_tag_node is from_tag_node


# ---------------------------------------------------------------------------
# Entry-point discovery
# ---------------------------------------------------------------------------


def test_entry_point_registered():
    """BSL registers bsl_tag_handler under xorq.from_tag_node."""
    eps = importlib.metadata.entry_points(group="xorq.from_tag_node")
    names = {ep.name for ep in eps}
    assert "bsl" in names, f"bsl entry point missing; found {names}"

    ep = next(ep for ep in eps if ep.name == "bsl")
    assert ep.load() is bsl_tag_handler


def test_handler_discovered_by_xorq_registry():
    """xorq's registry resolves 'bsl' to our handler (builtin or entry point)."""
    from xorq.expr.builders import _get_from_tag_node_registry

    registry = _get_from_tag_node_registry()
    assert "bsl" in registry
    handler = registry["bsl"]
    # Either our handler directly (via entry point when builtin is removed) or
    # xorq's current builtin that delegates to BSL — both must expose from_tag_node.
    assert handler.from_tag_node is not None


# ---------------------------------------------------------------------------
# extract_metadata
# ---------------------------------------------------------------------------


def test_extract_metadata_on_base_model(simple_model):
    """Direct SemanticTableOp tag → dims/measures read off the tag."""
    tag_node = _tag_node(to_tagged(simple_model))

    meta = extract_metadata(tag_node)

    assert meta["type"] == "semantic_model"
    assert set(meta["dimensions"]) == {"a", "b"}
    assert set(meta["measures"]) == {"sum_b", "avg_b"}
    assert "2 dims" in meta["description"]
    assert "2 measures" in meta["description"]


def test_extract_metadata_walks_source_chain(simple_model):
    """After a query (wraps SemanticTableOp in SemanticAggregateOp etc.), the
    handler still reaches the innermost SemanticTableOp for dim/measure names."""
    query = simple_model.query(dimensions=("a",), measures=("sum_b",))
    tag_node = _tag_node(to_tagged(query))

    meta = extract_metadata(tag_node)

    # Names come from the base model, not just the projected query columns.
    assert set(meta["dimensions"]) == {"a", "b"}
    assert set(meta["measures"]) == {"sum_b", "avg_b"}


# ---------------------------------------------------------------------------
# from_tag_node
# ---------------------------------------------------------------------------


def test_from_tag_node_returns_base_model(simple_model):
    tag_node = _tag_node(to_tagged(simple_model))

    recovered = from_tag_node(tag_node)

    # Behaves like a SemanticModel — has .query, dims, measures.
    assert hasattr(recovered, "query")
    assert set(recovered.dimensions) == {"a", "b"}
    assert set(recovered.measures) == {"sum_b", "avg_b"}


def test_from_tag_node_on_query_recovers_base_model(simple_model):
    """Even when the tagged expression is a query chain, from_tag_node returns
    the base SemanticModel so callers can issue fresh .query() calls."""
    query = simple_model.query(dimensions=("a",), measures=("sum_b",))
    tag_node = _tag_node(to_tagged(query))

    recovered = from_tag_node(tag_node)

    # Base model: all original dims/measures are present, not just the
    # projected subset.
    assert set(recovered.dimensions) == {"a", "b"}
    assert set(recovered.measures) == {"sum_b", "avg_b"}

    # Fresh query works against the recovered model.
    recovered.query(dimensions=("b",), measures=("avg_b",))


# ---------------------------------------------------------------------------
# expr.op.builder integration (xorq dispatch)
# ---------------------------------------------------------------------------


def test_ls_builder_dispatches_to_handler(simple_model):
    """xorq's `expr.ls.builder` walks tags and returns our SemanticModel."""
    tagged_expr = to_tagged(simple_model)

    recovered = tagged_expr.ls.builder

    assert set(recovered.dimensions) == {"a", "b"}
    assert set(recovered.measures) == {"sum_b", "avg_b"}
