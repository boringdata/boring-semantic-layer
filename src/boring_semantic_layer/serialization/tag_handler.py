"""xorq integration: TagHandler for BSL-tagged expressions.

Exposes a ``xorq.expr.builders.TagHandler`` instance that xorq discovers via
the ``xorq.from_tag_node`` entry point group, so xorq no longer needs to
hard-code BSL-specific logic.

- :func:`extract_metadata` — walks nested tag-node metadata to the innermost
  ``SemanticTableOp`` and returns a sidecar dict (dimension/measure names) for
  the catalog.
- :func:`from_tag_node` — reconstructs the base ``SemanticModel`` (the
  ``SemanticTableOp`` at the bottom of the source chain), not the full query
  chain, so callers can issue fresh ``.query()`` calls.
"""

from __future__ import annotations

from typing import Any

from .._xorq import TagHandler

from . import (
    BSLSerializationContext,
    extract_xorq_metadata,
    reconstruct_bsl_operation,
)
from .freeze import thaw


def extract_metadata(tag_node) -> dict[str, Any]:
    """Return sidecar metadata (dimension/measure names) for a BSL-tagged node.

    Walks nested metadata down to every ``SemanticTableOp`` leaf and unions
    their dimension / measure / calc-measure names. ``source`` chains are
    descended through; ``SemanticJoinOp`` nodes branch into ``left`` and
    ``right``. Names from a joined leaf are prefixed with the leaf's table
    name (matching how a joined ``SemanticTable`` exposes its fields, e.g.
    ``flights.flight_count``); a non-joined model returns flat names.
    """

    def as_dict(meta: Any) -> dict[str, Any]:
        return dict(meta) if isinstance(meta, tuple) else meta

    def collect(meta: Any, *, in_join: bool) -> tuple[list[str], list[str], list[str]]:
        meta = as_dict(meta)
        op_type = meta.get("bsl_op_type")

        if (src := meta.get("source")) is not None:
            return collect(src, in_join=in_join)

        if op_type == "SemanticJoinOp":
            ld, lm, lc = collect(meta.get("left", {}), in_join=True)
            rd, rm, rc = collect(meta.get("right", {}), in_join=True)
            return ld + rd, lm + rm, lc + rc

        if op_type == "SemanticTableOp":
            name = meta.get("name")
            prefix = f"{name}." if (in_join and name) else ""
            dims = [prefix + d[0] for d in meta.get("dimensions", ())]
            meas = [prefix + m[0] for m in meta.get("measures", ())]
            calc = [prefix + c[0] for c in meta.get("calc_measures", ())]
            return dims, meas, calc

        return [], [], []

    dims, measures, calc = collect(tag_node.metadata, in_join=False)
    result: dict[str, Any] = {
        "type": "semantic_model",
        "description": f"{len(dims)} dims, {len(measures)} measures",
        "dimensions": tuple(dims),
        "measures": tuple(measures),
    }
    if calc:
        result["calc_measures"] = tuple(calc)
    return result


def from_tag_node(tag_node):
    """Reconstruct the base ``SemanticModel`` from a BSL-tagged node.

    Walks to the innermost ``SemanticTableOp`` source and returns the base
    model (not the surrounding query chain), so callers can issue new
    ``.query()`` calls against it.
    """
    expr = tag_node.to_expr()
    ctx = BSLSerializationContext()
    # extract_xorq_metadata returns a shallow dict whose values are still
    # frozen tuple-of-pairs; thaw each value so we can descend via plain
    # dict lookups.
    metadata = {k: thaw(v) for k, v in extract_xorq_metadata(expr).items()}
    while src := metadata.get("source"):
        metadata = src
    return reconstruct_bsl_operation(metadata, expr, ctx)


def reemit(tag_node, rebuild_subexpr):
    """Re-emit a BSL-tagged subtree with a translated source.

    ``from_tag_node`` returns the base SemanticModel (discarding the query
    chain), so it cannot be used for rebuild — rebuild needs the full tag
    metadata to reproduce the original query.  This function works from the
    tag node directly: it rebuilds the source subtree and re-stamps the
    original tag metadata on top.

    Re-stamping uses ``hashing_tag`` (not ``tag``) so the rebuilt expression
    keeps the same hash-contribution guarantee as ``to_tagged`` — see #263.

    Precondition: ``tag_node`` is a BSL-tagged xorq ``HashingTag`` op (BSL
    only ever emits ``HashingTag`` — see ``to_tagged`` and the re-stamp
    below). xorq's dispatch only routes here when
    ``tag_node.metadata["tag"]`` resolves to this handler, and xorq's op
    definition declares ``parent: Relation`` (non-null) — so by
    construction ``tag_node.parent`` is always a valid relation.
    """
    new_source = rebuild_subexpr(tag_node.parent.to_expr())
    meta = dict(tag_node.metadata)
    tag_name = meta.pop("tag")
    return new_source.hashing_tag(tag=tag_name, **meta)


_handler_kwargs = dict(
    tag_names=("bsl",),
    extract_metadata=extract_metadata,
    from_tag_node=from_tag_node,
)
if "reemit" in {a.name for a in TagHandler.__attrs_attrs__}:
    _handler_kwargs["reemit"] = reemit

bsl_tag_handler = TagHandler(**_handler_kwargs)


__all__ = [
    "bsl_tag_handler",
    "extract_metadata",
    "from_tag_node",
    "reemit",
]
