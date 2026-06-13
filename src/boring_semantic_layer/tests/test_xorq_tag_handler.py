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

pytest.importorskip("xorq", reason="xorq not installed")

from boring_semantic_layer import SemanticModel, to_semantic_table
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


def test_extract_metadata_walks_join_branches():
    """For joined models the handler must descend into both ``left`` and
    ``right`` branches and union dim/measure names from every leaf
    ``SemanticTableOp``, prefixing them with the leaf's table name to match
    how a joined ``SemanticTable`` exposes its fields."""
    t1 = ibis.memtable({"id": [1, 2], "name": ["a", "b"]})
    t2 = ibis.memtable({"id": [1, 2], "value": [10, 20]})
    t3 = ibis.memtable({"id": [1, 2], "extra": ["x", "y"]})

    st1 = (
        to_semantic_table(t1, name="t1")
        .with_dimensions(id=lambda t: t.id, name=lambda t: t.name)
        .with_measures(count=lambda t: t.count())
    )
    st2 = (
        to_semantic_table(t2, name="t2")
        .with_dimensions(id=lambda t: t.id)
        .with_measures(total=lambda t: t.value.sum())
    )
    st3 = (
        to_semantic_table(t3, name="t3")
        .with_dimensions(id=lambda t: t.id, extra=lambda t: t.extra)
        .with_measures(extra_count=lambda t: t.count())
    )

    # Two-arm join chain: covers nested SemanticJoinOp on the left as well as
    # a query wrapper on top, exercising the same path as the original bug
    # where every leaf was being missed.
    joined = st1.join_one(st2, on=lambda l, r: l.id == r.id).join_one(
        st3, on=lambda l, r: l.id == r.id
    )
    query = joined.query(dimensions=("t1.name",), measures=("t1.count",))
    tag_node = _tag_node(to_tagged(query))

    meta = extract_metadata(tag_node)

    assert set(meta["dimensions"]) == {
        "t1.id",
        "t1.name",
        "t2.id",
        "t3.id",
        "t3.extra",
    }
    assert set(meta["measures"]) == {"t1.count", "t2.total", "t3.extra_count"}
    assert "5 dims" in meta["description"]
    assert "3 measures" in meta["description"]


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


# ---------------------------------------------------------------------------
# Hash contribution — regression for issue #263
# ---------------------------------------------------------------------------
#
# Before the fix, ``to_tagged()`` and ``reemit()`` wrapped expressions in a
# plain ``Tag`` node, which xorq's ``opaque_node_replacer`` strips during
# content-hash computation. ``source`` and ``source.tag("bsl", **metadata)``
# produced identical hashes, so two ``xorq build`` invocations on the same
# source (one bare, one BSL-tagged) silently overwrote each other under
# ``builds/<hash>/``. The fix is to use ``HashingTag``, which is tokenized
# as ``(parent_expr, metadata)``.
#
# Two related hash regressions are covered by pre-existing tests in
# ``test_xorq_convert.py``:
#   - ``test_different_measures_produce_different_hashes``
#   - ``test_same_model_produces_same_hash``
# We avoid duplicating those here.


def test_tagged_op_is_hashing_tag(simple_model):
    """``to_tagged`` wraps the expression in a HashingTag (not a plain Tag).

    HashingTag is a Tag subclass, so existing ``isinstance(op, Tag)`` checks
    in the reconstruct path continue to work; only the concrete class
    matters for the hash contract.
    """
    from xorq.expr.relations import HashingTag, Tag

    tag_node = _tag_node(to_tagged(simple_model))

    assert isinstance(tag_node, Tag)
    assert type(tag_node) is HashingTag


def test_tagged_hash_differs_from_untagged_source(simple_model):
    """A BSL-tagged expression hashes differently from its bare source.

    Without HashingTag both sides hash identically — ``xorq build`` would
    collide BSL artifacts with their underlying source under the same
    ``builds/<hash>/`` directory.
    """
    from xorq.caching.strategy import SnapshotStrategy
    from xorq.common.utils.node_utils import compute_expr_hash

    from boring_semantic_layer.expr import to_untagged

    untagged = to_untagged(simple_model)
    tagged = to_tagged(simple_model)
    strategy = SnapshotStrategy()

    assert compute_expr_hash(untagged, strategy=strategy) != compute_expr_hash(
        tagged, strategy=strategy
    )


def test_reemit_preserves_hashing_tag(simple_model):
    """``reemit`` must re-stamp the rebuilt expression with a HashingTag.

    Sister regression to ``to_tagged``: catalog replay / rebuild paths call
    ``reemit`` to translate the inner source, then re-apply the BSL tag on
    top. If ``reemit`` used the plain ``.tag()`` here, the rebuilt
    expression would lose the hash-contribution guarantee — re-introducing
    issue #263 specifically on rebuilt artifacts.
    """
    from xorq.expr.relations import HashingTag

    from boring_semantic_layer.serialization.tag_handler import reemit

    tag_node = _tag_node(to_tagged(simple_model))
    rebuilt = reemit(tag_node, rebuild_subexpr=lambda e: e)

    assert type(_tag_node(rebuilt)) is HashingTag


def test_reemit_hash_distinguishes_metadata():
    """A reemitted BSL expression hashes by its metadata (#263 across rebuild).

    Two ``to_tagged → reemit`` round-trips on the same underlying ibis table
    but with different BSL metadata must produce different content hashes.
    This pins that ``reemit`` keeps the HashingTag semantics end-to-end.
    """
    from xorq.caching.strategy import SnapshotStrategy
    from xorq.common.utils.node_utils import compute_expr_hash

    from boring_semantic_layer.serialization.tag_handler import reemit

    table = ibis.memtable({"a": [1, 2, 3], "b": [4, 5, 6]})
    model_a = SemanticModel(
        table=table,
        dimensions={"a": lambda t: t.a},
        measures={"sum_b": lambda t: t.b.sum()},
        name="model_a",
    )
    model_b = SemanticModel(
        table=table,
        dimensions={"b": lambda t: t.b},
        measures={"avg_b": lambda t: t.b.mean()},
        name="model_b",
    )

    rebuilt_a = reemit(_tag_node(to_tagged(model_a)), rebuild_subexpr=lambda e: e)
    rebuilt_b = reemit(_tag_node(to_tagged(model_b)), rebuild_subexpr=lambda e: e)
    strategy = SnapshotStrategy()

    assert compute_expr_hash(rebuilt_a, strategy=strategy) != compute_expr_hash(
        rebuilt_b, strategy=strategy
    )
