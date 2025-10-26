"""Conversion functions for lowering semantic layer operations to Ibis.

This module contains all the converters that register with ibis.expr.sql.convert
to transform semantic layer operations into executable Ibis expressions.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import ibis
from attrs import field, frozen
from ibis.common.collections import FrozenOrderedDict
from ibis.expr.sql import convert

from boring_semantic_layer.ops import (
    SemanticAggregateOp,
    SemanticFilterOp,
    SemanticGroupByOp,
    SemanticJoinOp,
    SemanticLimitOp,
    SemanticMutateOp,
    SemanticOrderByOp,
    SemanticProjectOp,
    SemanticTableOp,
    _find_all_root_models,
)

IbisTableExpr = ibis.expr.api.Table
IbisProject = ibis.expr.operations.relations.Project


@frozen
class _Resolver:
    """Resolver for dimensions in filter/join predicates.

    Provides attribute access to dimensions and raw table columns,
    resolving dimension functions to named expressions.
    """

    _t: Any
    _dims: dict[str, Any] = field(factory=dict)

    def __getattr__(self, name: str):
        return (
            self._dims[name](self._t).name(name)
            if name in self._dims
            else next(
                (
                    dim_func(self._t).name(dim_name)
                    for dim_name, dim_func in self._dims.items()
                    if dim_name.endswith(f".{name}")
                ),
                getattr(self._t, name),
            )
        )

    def __getitem__(self, name: str):
        return getattr(self._t, name)


@frozen
class _AggResolver:
    """Resolver for dimensions and measures in aggregate operations.

    Provides attribute access to both dimensions and measures,
    handling prefixed names from joins (e.g., "table__column").
    """

    _t: Any
    _dims: dict[str, Callable]
    _meas: dict[str, Callable]

    def __getattr__(self, key: str):
        return (
            self._dims[key](self._t)
            if key in self._dims
            else self._meas[key](self._t)
            if key in self._meas
            else next(
                (
                    dim_func(self._t)
                    for dim_name, dim_func in self._dims.items()
                    if dim_name.endswith(f".{key}")
                ),
                None,
            )
            or next(
                (
                    meas_func(self._t)
                    for meas_name, meas_func in self._meas.items()
                    if meas_name.endswith(f".{key}")
                ),
                None,
            )
            or getattr(self._t, key)
        )

    def __getitem__(self, key: str):
        return getattr(self._t, key)


@frozen
class _AggProxy:
    """Proxy for post-aggregation mutations.

    Provides simple attribute/item access to aggregated columns.
    """

    _t: Any

    def __getattr__(self, key: str):
        return self._t[key]

    def __getitem__(self, key: str):
        return self._t[key]


# ============================================================================
# Ibis converters (passthrough for standard Ibis operations)
# ============================================================================


@convert.register(IbisTableExpr)
def _convert_ibis_table(expr, catalog, *args):
    """Convert Ibis table expression to catalog form."""
    return convert(expr.op(), catalog=catalog)


@convert.register(IbisProject)
def _convert_ibis_project(op: IbisProject, catalog, *args):
    """Convert Ibis project operation."""
    tbl = convert(op.parent, catalog=catalog)
    cols = [v.to_expr().name(k) for k, v in op.values.items()]
    return tbl.select(cols)


# ============================================================================
# Semantic layer converters
# ============================================================================


@convert.register(SemanticTableOp)
def _convert_semantic_table(node: SemanticTableOp, catalog, *args):
    """Convert SemanticTableOp to base Ibis table."""
    return convert(node.table, catalog=catalog)


@convert.register(SemanticFilterOp)
def _convert_semantic_filter(node: SemanticFilterOp, catalog, *args):
    """Convert SemanticFilterOp to Ibis filter.

    Resolves dimension references in the filter predicate and applies
    the filter to the base table.
    """
    from boring_semantic_layer.ops import SemanticAggregate, _get_merged_fields

    all_roots = _find_all_root_models(node.source)
    base_tbl = convert(node.source, catalog=catalog)

    dim_map = (
        {}
        if isinstance(node.source, SemanticAggregate)
        else _get_merged_fields(all_roots, "dimensions")
    )
    pred = node.predicate(_Resolver(base_tbl, dim_map))
    return base_tbl.filter(pred)


@convert.register(SemanticProjectOp)
def _convert_semantic_project(node: SemanticProjectOp, catalog, *args):
    """Convert SemanticProjectOp to Ibis select/aggregate.

    Handles projection of:
    - Dimensions (potentially with aggregation if measures are also selected)
    - Measures (triggers aggregation)
    - Raw table columns
    """
    from boring_semantic_layer.ops import _get_merged_fields

    all_roots = _find_all_root_models(node.source)
    tbl = convert(node.source, catalog=catalog)

    if not all_roots:
        return tbl.select([getattr(tbl, f) for f in node.fields])

    merged_dimensions = _get_merged_fields(all_roots, "dimensions")
    merged_measures = _get_merged_fields(all_roots, "measures")

    dims = [f for f in node.fields if f in merged_dimensions]
    meas = [f for f in node.fields if f in merged_measures]
    raw_fields = [f for f in node.fields if f not in merged_dimensions and f not in merged_measures]

    dim_exprs = [merged_dimensions[name](tbl).name(name) for name in dims]
    meas_exprs = [merged_measures[name](tbl).name(name) for name in meas]
    raw_exprs = [getattr(tbl, name) for name in raw_fields if hasattr(tbl, name)]

    return (
        tbl.group_by(dim_exprs).aggregate(meas_exprs)
        if meas_exprs and dim_exprs
        else tbl.aggregate(meas_exprs)
        if meas_exprs
        else tbl.select(dim_exprs + raw_exprs)
        if dim_exprs or raw_exprs
        else tbl
    )


@convert.register(SemanticGroupByOp)
def _convert_semantic_groupby(node: SemanticGroupByOp, catalog, *args):
    """Convert SemanticGroupByOp (passthrough - grouping happens in aggregate)."""
    return convert(node.source, catalog=catalog)


@convert.register(SemanticJoinOp)
def _convert_semantic_join(node: SemanticJoinOp, catalog, *args):
    """Convert SemanticJoinOp to Ibis join.

    Handles both conditional joins (with ON clause) and cross joins.
    """
    left_tbl = convert(node.left, catalog=catalog)
    right_tbl = convert(node.right, catalog=catalog)
    return (
        left_tbl.join(
            right_tbl,
            node.on(_Resolver(left_tbl), _Resolver(right_tbl)),
            how=node.how,
        )
        if node.on is not None
        else left_tbl.join(right_tbl, how=node.how)
    )


@convert.register(SemanticAggregateOp)
def _convert_semantic_aggregate(node: SemanticAggregateOp, catalog, *args):
    """Convert SemanticAggregateOp to Ibis group_by + aggregate.

    Resolves:
    - Group by keys (dimensions or raw columns)
    - Aggregation expressions (measures)

    Returns aggregated table with properly named columns.
    """
    from boring_semantic_layer.ops import _get_merged_fields

    all_roots = _find_all_root_models(node.source)
    tbl = convert(node.source, catalog=catalog)

    merged_dimensions = _get_merged_fields(all_roots, "dimensions")
    merged_measures = _get_merged_fields(all_roots, "measures")

    group_exprs = [
        (merged_dimensions[k](tbl).name(k) if k in merged_dimensions else getattr(tbl, k).name(k))
        for k in node.keys
    ]

    proxy = _AggResolver(tbl, merged_dimensions, merged_measures)
    meas_exprs = [fn(proxy).name(name) for name, fn in node.aggs.items()]
    metrics = FrozenOrderedDict({expr.get_name(): expr for expr in meas_exprs})

    return tbl.group_by(group_exprs).aggregate(metrics) if group_exprs else tbl.aggregate(metrics)


@convert.register(SemanticMutateOp)
def _convert_semantic_mutate(node: SemanticMutateOp, catalog, *args):
    """Convert SemanticMutateOp to Ibis mutate.

    Adds computed columns to the result of an aggregation or other operation.
    """
    agg_tbl = convert(node.source, catalog=catalog)
    proxy = _AggProxy(agg_tbl)
    new_cols = [fn(proxy).name(name) for name, fn in node.post.items()]
    return agg_tbl.mutate(new_cols) if new_cols else agg_tbl


@convert.register(SemanticOrderByOp)
def _convert_semantic_orderby(node: SemanticOrderByOp, catalog, *args):
    """Convert SemanticOrderByOp to Ibis order_by.

    Handles:
    - String keys (column names)
    - Deferred expressions (from lambda functions)
    - Direct column references
    """
    tbl = convert(node.source, catalog=catalog)

    def resolve_key(key):
        return (
            getattr(tbl, key)
            if hasattr(tbl, key)
            else tbl[key]
            if isinstance(key, str) and key in tbl.columns
            else key[1](tbl)
            if isinstance(key, tuple) and len(key) == 2 and key[0] == "__deferred__"
            else key
        )

    return tbl.order_by([resolve_key(key) for key in node.keys])


@convert.register(SemanticLimitOp)
def _convert_semantic_limit(node: SemanticLimitOp, catalog, *args):
    """Convert SemanticLimitOp to Ibis limit.

    Applies row limit with optional offset.
    """
    tbl = convert(node.source, catalog=catalog)
    return tbl.limit(node.n) if node.offset == 0 else tbl.limit(node.n, offset=node.offset)
