"""Pretty-printing of semantic-layer operation chains.

Used by every ``Semantic*Op.__repr__``. The Op classes themselves are
imported lazily inside each helper to avoid a circular module
dependency with ``_core``.
"""

from __future__ import annotations

from ibis.expr.operations.relations import Relation


def _collect_chain(op: Relation) -> list[Relation]:
    """Walk op.source (or op.left for joins) back to root, return list from root to current."""
    chain = [op]
    current = op
    while True:
        if hasattr(current, "source") and current.source is not None:
            chain.append(current.source)
            current = current.source
        elif hasattr(current, "left") and current.left is not None:
            chain.append(current.left)
            current = current.left
        else:
            break
    chain.reverse()
    return chain


def _format_op_summary(op: Relation) -> str:
    """Return a one-line summary string for a non-root semantic op."""
    from ._core import (
        SemanticAggregateOp,
        SemanticFilterOp,
        SemanticGroupByOp,
        SemanticIndexOp,
        SemanticJoinOp,
        SemanticLimitOp,
        SemanticMutateOp,
        SemanticOrderByOp,
        SemanticProjectOp,
        SemanticTableOp,
        SemanticUnnestOp,
    )
    from ._root_models import _find_all_root_models

    cls = type(op).__name__

    if isinstance(op, SemanticFilterOp):
        predicate = object.__getattribute__(op, "predicate")
        pred_name = "<predicate>"
        if hasattr(predicate, "__name__"):
            pred_name = predicate.__name__
        elif hasattr(predicate, "unwrap"):
            unwrapped = predicate.unwrap
            if hasattr(unwrapped, "__name__"):
                pred_name = unwrapped.__name__
        return f"Filter(λ {pred_name})"

    if isinstance(op, SemanticMutateOp):
        post = object.__getattribute__(op, "post")
        cols = list(post.keys())
        return f"Mutate({', '.join(cols)})"

    if isinstance(op, SemanticGroupByOp):
        keys = object.__getattribute__(op, "keys")
        return f"GroupBy({', '.join(keys)})"

    if isinstance(op, SemanticAggregateOp):
        aggs = object.__getattribute__(op, "aggs")
        agg_names = list(aggs.keys())
        return f"Aggregate({', '.join(agg_names)})"

    if isinstance(op, SemanticOrderByOp):
        keys = object.__getattribute__(op, "keys")
        key_strs = [k if isinstance(k, str) else repr(k) for k in keys]
        return f"OrderBy({', '.join(key_strs)})"

    if isinstance(op, SemanticLimitOp):
        return f"Limit({op.n})"

    if isinstance(op, SemanticProjectOp):
        fields = object.__getattribute__(op, "fields")
        return f"Project({', '.join(fields)})"

    if isinstance(op, SemanticUnnestOp):
        column = object.__getattribute__(op, "column")
        return f"Unnest({column})"

    if isinstance(op, SemanticJoinOp):
        how = object.__getattribute__(op, "how")
        right = object.__getattribute__(op, "right")
        right_name = ""
        if isinstance(right, SemanticTableOp):
            right_name = object.__getattribute__(right, "name") or ""
        if not right_name:
            # Try to find a root name from right side
            roots = _find_all_root_models(right)
            if roots:
                right_name = object.__getattribute__(roots[0], "name") or ""
        if right_name:
            return f"Join({how}, right={right_name})"
        return f"Join({how})"

    if isinstance(op, SemanticIndexOp):
        parts = []
        selector = object.__getattribute__(op, "selector")
        by = object.__getattribute__(op, "by")
        sample = object.__getattribute__(op, "sample")
        if selector is not None:
            if isinstance(selector, tuple):
                parts.append(", ".join(selector))
            else:
                parts.append(str(selector))
        if by is not None:
            parts.append(f"by={by}")
        if sample is not None:
            parts.append(f"sample={sample}")
        return f"Index({', '.join(parts)})"

    # Fallback for unknown op types
    return cls.replace("Semantic", "").replace("Op", "")


def _format_root(root_op) -> str:
    """Format a SemanticTableOp root using the fmt registry from format.py."""
    from boring_semantic_layer.format import fmt

    try:
        return fmt(root_op)
    except Exception:
        # Fallback if format module isn't available
        name = object.__getattribute__(root_op, "name")
        return f"SemanticTable: {name}" if name else "SemanticTable"


def _semantic_repr(op: Relation) -> str:
    from ._core import SemanticTableOp

    chain = _collect_chain(op)

    # Find the root (first element should be a SemanticTableOp)
    root = chain[0]
    if isinstance(root, SemanticTableOp):
        lines = [_format_root(root)]
    else:
        # Fallback: no SemanticTableOp root found
        from ibis.expr.format import pretty

        try:
            return pretty(op)
        except Exception:
            return object.__repr__(op)

    # Append pipeline steps
    for step in chain[1:]:
        if not isinstance(step, SemanticTableOp):
            lines.append(f"-> {_format_op_summary(step)}")

    return "\n".join(lines)
