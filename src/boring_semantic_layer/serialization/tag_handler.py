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

from xorq.expr.builders import TagHandler

from . import (
    BSLSerializationContext,
    extract_xorq_metadata,
    reconstruct_bsl_operation,
)
from .freeze import thaw


def extract_metadata(tag_node) -> dict[str, Any]:
    """Return sidecar metadata (dimension/measure names) for a BSL-tagged node.

    Walks nested metadata (the ``source`` chain) down to the innermost
    ``SemanticTableOp`` and extracts the dimension/measure name tuples.
    """
    table_meta: Any = tag_node.metadata
    while table_meta.get("bsl_op_type") != "SemanticTableOp" and (
        src := table_meta.get("source")
    ):
        table_meta = dict(src) if isinstance(src, tuple) else src
    dims = tuple(d[0] for d in table_meta.get("dimensions", ()))
    measures = tuple(m[0] for m in table_meta.get("measures", ()))
    return {
        "type": "semantic_model",
        "description": f"{len(dims)} dims, {len(measures)} measures",
        "dimensions": dims,
        "measures": measures,
    }


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
    """
    new_source = rebuild_subexpr(tag_node.parent.to_expr())
    meta = dict(tag_node.metadata)
    tag_name = meta.pop("tag")
    return new_source.tag(tag=tag_name, **meta)


bsl_tag_handler = TagHandler(
    tag_names=("bsl",),
    extract_metadata=extract_metadata,
    from_tag_node=from_tag_node,
    reemit=reemit,
)


__all__ = [
    "bsl_tag_handler",
    "extract_metadata",
    "from_tag_node",
    "reemit",
]
