"""Compile strategy: ``nest=`` aggregate entries.

Extracted verbatim from ``SemanticAggregateOp._to_untagged_with_nest``;
``op`` is the aggregate op being compiled.
"""

from __future__ import annotations

from .._xorq import null_safe_equal
from ..measure_scope import ColumnScope
from ._core import (
    NestAggSpec,
    SemanticAggregateOp,
    _collect_struct,
    _resolve_expr,
    _resolve_nest_order_key,
    _unwrap,
)


def to_untagged_with_nest(op, nest_specs: dict[str, NestAggSpec]):
    """Compile ``nest=`` aggregate entries and join them to the outer result.

    Each nest spec compiles as its own semantic aggregation at
    (outer keys + inner keys) grain — measure and dimension names
    resolve exactly like a top-level query. Its rows are collected
    into one array-of-structs per outer group and attached to the
    outer aggregate with a null-safe left join on the outer keys, so
    outer groups the inner query filtered away keep a NULL array
    instead of disappearing. HAVING predicates run at the inner grain
    before collection; ``order_by``/``limit`` order and truncate each
    group's array.
    """

    plain_aggs = {name: fn for name, fn in op.aggs.items() if name not in nest_specs}
    outer_keys = list(op.keys)
    result = None
    if outer_keys or plain_aggs:
        outer_op = SemanticAggregateOp(
            source=op.source,
            keys=op.keys,
            aggs=plain_aggs,
            nested_columns=tuple(n for n in op.nested_columns if n not in nest_specs),
        )
        result = outer_op.to_untagged()

    for name, spec in nest_specs.items():
        inner_tbl = spec.inner_op.to_untagged()
        for predicate in reversed(spec.having):
            inner_tbl = inner_tbl.filter(
                _resolve_expr(_unwrap(predicate), ColumnScope(_tbl=inner_tbl))
            )
        collect_kwargs = {}
        if spec.order_keys:
            collect_kwargs["order_by"] = [
                _resolve_nest_order_key(key, inner_tbl) for key in spec.order_keys
            ]
        collected = _collect_struct({c: inner_tbl[c] for c in spec.struct_fields}, **collect_kwargs)
        if spec.limit_spec is not None:
            n, offset = spec.limit_spec
            collected = collected[offset : offset + n]
        if outer_keys:
            nest_tbl = inner_tbl.group_by([inner_tbl[k] for k in outer_keys]).aggregate(
                **{name: collected}
            )
            # Temp-rename the join keys so the left join has no name
            # collisions; null-safe equality keeps NULL dimension groups
            # matched to their own nested rows.
            tmp_keys = {f"__bsl_nest_k{i}__": k for i, k in enumerate(outer_keys)}
            nest_tbl = nest_tbl.rename(tmp_keys)
            tmp_for = {old: tmp for tmp, old in tmp_keys.items()}
            preds = [null_safe_equal(result[k], nest_tbl[tmp_for[k]]) for k in outer_keys]
            joined = result.left_join(nest_tbl, preds)
            result = joined.select([*result.columns, name])
        else:
            nest_tbl = inner_tbl.aggregate(**{name: collected})
            result = nest_tbl if result is None else result.cross_join(nest_tbl)

    # Restore the requested column order: keys, then aggregates (nest
    # entries included) in declaration order.
    desired = list(dict.fromkeys([*op.keys, *op.aggs.keys()]))
    cols = list(result.columns)
    ordered = [c for c in desired if c in cols] + [c for c in cols if c not in desired]
    if ordered != cols:
        result = result.select(ordered)
    return result
