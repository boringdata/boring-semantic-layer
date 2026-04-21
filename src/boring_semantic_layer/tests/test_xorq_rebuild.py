"""Tests for BSL rebuild support via ``reemit`` on the TagHandler.

Covers:
- ``reemit`` is registered on ``bsl_tag_handler``
- Identity reemit preserves tag metadata (round-trip invariant)
- ``get_rebuild_dispatch`` returns handler-level reemit for BSL tags
- Catalog rebuild round-trip with BSL entries
- Rebuilt BSL entries execute correctly
- Query-chain (aggregate) rebuild
"""

from __future__ import annotations

from pathlib import Path

import ibis
import pytest

from boring_semantic_layer import SemanticModel
from boring_semantic_layer.serialization import to_tagged
from boring_semantic_layer.serialization.tag_handler import (
    bsl_tag_handler,
    reemit,
)

xorq = pytest.importorskip("xorq", reason="xorq not installed")


def _tag_node(tagged_expr):
    return tagged_expr.op()


# ---------------------------------------------------------------------------
# Phase 2: reemit registration
# ---------------------------------------------------------------------------


def test_reemit_registered_on_handler():
    assert bsl_tag_handler.reemit is reemit


def test_reemit_is_callable():
    assert callable(bsl_tag_handler.reemit)


# ---------------------------------------------------------------------------
# Phase 3: identity reemit preserves tag metadata (Invariant B)
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_model():
    table = ibis.memtable({"a": [1, 2, 3], "b": [4, 5, 6]})
    return SemanticModel(
        table=table,
        dimensions={"a": lambda t: t.a, "b": lambda t: t.b},
        measures={"sum_b": lambda t: t.b.sum(), "avg_b": lambda t: t.b.mean()},
        name="simple",
    )


def test_identity_reemit_preserves_metadata(simple_model):
    tagged = to_tagged(simple_model)
    original_meta = dict(_tag_node(tagged).metadata)

    rebuilt = reemit(_tag_node(tagged), rebuild_subexpr=lambda e: e)
    rebuilt_meta = dict(_tag_node(rebuilt).metadata)

    assert original_meta == rebuilt_meta


def test_identity_reemit_on_query_chain(simple_model):
    query = simple_model.query(dimensions=("a",), measures=("sum_b",))
    tagged = to_tagged(query)
    original_meta = dict(_tag_node(tagged).metadata)

    rebuilt = reemit(_tag_node(tagged), rebuild_subexpr=lambda e: e)
    rebuilt_meta = dict(_tag_node(rebuilt).metadata)

    assert original_meta == rebuilt_meta


# ---------------------------------------------------------------------------
# get_rebuild_dispatch returns handler-level reemit for BSL
# ---------------------------------------------------------------------------


def test_get_rebuild_dispatch_returns_callable_for_bsl(simple_model):
    from xorq.expr.builders import get_rebuild_dispatch

    tagged = to_tagged(simple_model)
    dispatch = get_rebuild_dispatch(_tag_node(tagged))
    assert callable(dispatch)


def test_get_rebuild_dispatch_invokes_handler_reemit(simple_model):
    from xorq.expr.builders import get_rebuild_dispatch

    tagged = to_tagged(simple_model)
    dispatch = get_rebuild_dispatch(_tag_node(tagged))
    result = dispatch(lambda e: e)
    assert result is not None
    rebuilt_meta = dict(_tag_node(result).metadata)
    original_meta = dict(_tag_node(tagged).metadata)
    assert original_meta == rebuilt_meta


# ---------------------------------------------------------------------------
# Catalog helpers
# ---------------------------------------------------------------------------


def _make_catalog(tmpdir, name="src"):
    import xorq.api as xo
    from xorq.catalog.backend import GitBackend
    from xorq.catalog.catalog import Catalog

    repo = Catalog.init_repo_path(Path(tmpdir).joinpath(name))
    catalog = Catalog(backend=GitBackend(repo=repo))
    return catalog, xo


def _add_source_with_identity_transform(catalog, xo, data, *, source_alias, transform_alias):
    from xorq.vendor.ibis.expr import operations as ops

    source_expr = xo.memtable(data, name=source_alias)
    source_entry = catalog.add(source_expr, aliases=(source_alias,))

    unbound = ops.UnboundTable(name="p", schema=source_expr.schema()).to_expr()
    identity = unbound.select(*source_expr.columns)
    transform_entry = catalog.add(identity, aliases=(transform_alias,))

    return source_entry, transform_entry


def _replay_rebuild(source_catalog_obj, target_path):
    from xorq.catalog.catalog import Catalog
    from xorq.catalog.replay import Replayer

    target = Catalog.from_repo_path(target_path, init=True)
    Replayer(from_catalog=source_catalog_obj, rebuild=True).replay(target)
    return target


# ---------------------------------------------------------------------------
# Catalog rebuild: query chain (SemanticAggregateOp)
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog_with_bsl_query(tmpdir):
    from xorq.catalog.bind import bind

    catalog, xo = _make_catalog(tmpdir)

    source_entry, transform_entry = _add_source_with_identity_transform(
        catalog,
        xo,
        {"origin": ["JFK", "LAX", "ORD"], "delay": [10.0, -5.0, 3.0]},
        source_alias="flights",
        transform_alias="flights-identity",
    )

    bound = bind(source_entry, transform_entry)
    model = SemanticModel(
        table=bound,
        dimensions={"origin": lambda t: t.origin},
        measures={"avg_delay": lambda t: t.delay.mean()},
        name="flights_model",
    )
    tagged = to_tagged(
        model.query(dimensions=("origin",), measures=("avg_delay",))
    )
    bsl_entry = catalog.add(tagged, aliases=("origin-delays",))

    return catalog, source_entry, bsl_entry


def test_catalog_rebuild_produces_consistent_target(catalog_with_bsl_query, tmpdir):
    catalog, _, _ = catalog_with_bsl_query
    target = _replay_rebuild(catalog, Path(tmpdir).joinpath("tgt"))
    assert len(target.list()) == len(catalog.list())
    assert set(target.list_aliases()) == set(catalog.list_aliases())
    target.assert_consistency()


def test_catalog_rebuild_bsl_entry_exists(catalog_with_bsl_query, tmpdir):
    catalog, _, _ = catalog_with_bsl_query
    target = _replay_rebuild(catalog, Path(tmpdir).joinpath("tgt"))
    entry = target.get_catalog_entry("origin-delays", maybe_alias=True)
    assert entry is not None


def test_catalog_rebuild_bsl_entry_executes(catalog_with_bsl_query, tmpdir):
    catalog, _, _ = catalog_with_bsl_query
    target = _replay_rebuild(catalog, Path(tmpdir).joinpath("tgt"))
    entry = target.get_catalog_entry("origin-delays", maybe_alias=True)
    result = entry.lazy_expr.execute()
    assert len(result) == 3
    assert "origin" in result.columns
    assert "avg_delay" in result.columns


# ---------------------------------------------------------------------------
# Catalog rebuild: base model (SemanticTableOp)
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog_with_base_model(tmpdir):
    from xorq.catalog.bind import bind

    catalog, xo = _make_catalog(tmpdir)

    source_entry, transform_entry = _add_source_with_identity_transform(
        catalog,
        xo,
        {"city": ["NYC", "LA"], "pop": [8_000_000, 4_000_000]},
        source_alias="cities",
        transform_alias="cities-identity",
    )

    bound = bind(source_entry, transform_entry)
    model = SemanticModel(
        table=bound,
        dimensions={"city": lambda t: t.city},
        measures={"total_pop": lambda t: t.pop.sum()},
        name="city_model",
    )
    tagged = to_tagged(model)
    bsl_entry = catalog.add(tagged, aliases=("city-stats",))

    return catalog, source_entry, bsl_entry


def test_catalog_rebuild_base_model(catalog_with_base_model, tmpdir):
    catalog, _, _ = catalog_with_base_model
    target = _replay_rebuild(catalog, Path(tmpdir).joinpath("tgt"))
    assert set(target.list_aliases()) == set(catalog.list_aliases())
    target.assert_consistency()


def test_catalog_rebuild_base_model_executes(catalog_with_base_model, tmpdir):
    catalog, _, _ = catalog_with_base_model
    target = _replay_rebuild(catalog, Path(tmpdir).joinpath("tgt"))
    entry = target.get_catalog_entry("city-stats", maybe_alias=True)
    result = entry.lazy_expr.execute()
    assert len(result) == 2
