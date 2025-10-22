from __future__ import annotations

from typing import Any, Callable

import ibis as ibis_mod
from attrs import frozen, field
from ibis.expr.sql import convert  # noqa: E402
from boring_semantic_layer.semantic_api.ops import (  # noqa: E402
    SemanticAggregate,
    SemanticFilter,
    SemanticGroupBy,
    SemanticJoin,
    SemanticMutate,
    SemanticOrderBy,
    SemanticProject,
    SemanticTable,
    SemanticLimit,
    _find_all_root_models,
    _merge_fields_with_prefixing,
)
from ibis.common.collections import FrozenOrderedDict

IbisTableExpr = ibis_mod.expr.api.Table

IbisProject = ibis_mod.expr.operations.relations.Project


@frozen
class _Resolver:
    _t: Any
    _dims: dict[str, Any] = field(factory=dict)

    def __getattr__(self, name: str):
        # 1. Try exact match first (unprefixed)
        if name in self._dims:
            return self._dims[name](self._t).name(name)
        
        # 2. Try prefixed versions (table__name format)
        for dim_name, dim_func in self._dims.items():
            if dim_name.endswith(f"__{name}"):
                return dim_func(self._t).name(dim_name)
        
        # 3. Fall back to table columns
        return getattr(self._t, name)

    def __getitem__(self, name: str):
        return getattr(self._t, name)


@convert.register(IbisTableExpr)
def _convert_ibis_table(expr, catalog, *args):
    return convert(expr.op(), catalog=catalog)


@convert.register(IbisProject)
def _lower_ibis_project(op: IbisProject, catalog, *args):
    tbl = convert(op.parent, catalog=catalog)
    cols = [v.to_expr().name(k) for k, v in op.values.items()]
    return tbl.select(cols)


@convert.register(SemanticTable)
def _lower_semantic_table(node: SemanticTable, catalog, *args):
    return convert(node.table, catalog=catalog)


@convert.register(SemanticFilter)
def _lower_semantic_filter(node: SemanticFilter, catalog, *args):
    # Handle both single and joined tables
    all_roots = _find_all_root_models(node.source)
    base_tbl = convert(node.source, catalog=catalog)

    # Check if we're filtering after aggregation
    # If the source is a SemanticAggregate, we should only use available columns
    from boring_semantic_layer.semantic_api.ops import SemanticAggregate

    if isinstance(node.source, SemanticAggregate):
        # Post-aggregation filter: only use columns available in the result
        dim_map = {}  # Don't use original dimensions
    else:
        # Pre-aggregation filter: use semantic dimensions
        if len(all_roots) > 1:  # Joined table
            dim_map = _merge_fields_with_prefixing(all_roots, lambda r: r.dimensions)
        else:  # Single table
            dim_map = all_roots[0].dimensions if all_roots else {}

    pred = node.predicate(_Resolver(base_tbl, dim_map))
    return base_tbl.filter(pred)


@convert.register(SemanticProject)
def _lower_semantic_project(node: SemanticProject, catalog, *args):
    # Handle both single and joined tables
    all_roots = _find_all_root_models(node.source)
    if not all_roots:
        tbl = convert(node.source, catalog=catalog)
        cols = [getattr(tbl, f) for f in node.fields]
        return tbl.select(cols)

    tbl = convert(node.source, catalog=catalog)
    
    # Get merged fields with __ separator
    if len(all_roots) > 1:  # Joined table
        merged_dimensions = _merge_fields_with_prefixing(all_roots, lambda r: r.dimensions)
        merged_measures = _merge_fields_with_prefixing(all_roots, lambda r: r.measures)
    else:  # Single table
        merged_dimensions = all_roots[0].dimensions if all_roots else {}
        merged_measures = all_roots[0].measures if all_roots else {}
    
    dims = [f for f in node.fields if f in merged_dimensions]
    meas = [f for f in node.fields if f in merged_measures]
    raw_fields = [f for f in node.fields if f not in merged_dimensions and f not in merged_measures]

    dim_exprs = [merged_dimensions[name](tbl).name(name) for name in dims]
    meas_exprs = [merged_measures[name](tbl).name(name) for name in meas]
    # For raw fields, just try to get them from the table directly
    raw_exprs = [getattr(tbl, name) for name in raw_fields if hasattr(tbl, name)]

    if meas_exprs:
        if dim_exprs:
            return tbl.group_by(dim_exprs).aggregate(meas_exprs)
        else:
            # No dimensions - direct aggregation for measures only
            return tbl.aggregate(meas_exprs)
    else:
        all_exprs = dim_exprs + raw_exprs
        return tbl.select(all_exprs) if all_exprs else tbl  # no-op if nothing selected


@convert.register(SemanticGroupBy)
def _lower_semantic_groupby(node: SemanticGroupBy, catalog, *args):
    return convert(node.source, catalog=catalog)


@convert.register(SemanticJoin)
def _lower_semantic_join(node: SemanticJoin, catalog, *args):
    left_tbl = convert(node.left, catalog=catalog)
    right_tbl = convert(node.right, catalog=catalog)
    if node.on is not None:
        pred = node.on(_Resolver(left_tbl), _Resolver(right_tbl))
        return left_tbl.join(right_tbl, pred, how=node.how)
    else:
        return left_tbl.join(right_tbl, how=node.how)


@convert.register(SemanticAggregate)
def _lower_semantic_aggregate(node: SemanticAggregate, catalog, *args):
    # Handle both single and joined tables
    all_roots = _find_all_root_models(node.source)
    tbl = convert(node.source, catalog=catalog)
    
    # Get merged fields with __ separator
    if len(all_roots) > 1:  # Joined table
        merged_dimensions = _merge_fields_with_prefixing(all_roots, lambda r: r.dimensions)
        merged_measures = _merge_fields_with_prefixing(all_roots, lambda r: r.measures)
    else:  # Single table - use original fields
        merged_dimensions = all_roots[0].dimensions if all_roots else {}
        merged_measures = all_roots[0].measures if all_roots else {}

    group_exprs = []
    for k in node.keys:
        # Try to resolve with merged dimensions (handles prefixed names)
        if k in merged_dimensions:
            group_exprs.append(merged_dimensions[k](tbl).name(k))
        else:
            group_exprs.append(getattr(tbl, k).name(k))

    @frozen
    class _AggResolver:
        _t: Any
        _dims: dict[str, Callable]
        _meas: dict[str, Callable]

        def __getattr__(self, key: str):
            # 1. Try exact match first (unprefixed)
            if key in self._dims:
                return self._dims[key](self._t)
            if key in self._meas:
                return self._meas[key](self._t)
            
            # 2. Try prefixed versions (table__name format)
            for dim_name, dim_func in self._dims.items():
                if dim_name.endswith(f"__{key}"):
                    return dim_func(self._t)
            for meas_name, meas_func in self._meas.items():
                if meas_name.endswith(f"__{key}"):
                    return meas_func(self._t)
            
            # 3. Fall back to table columns
            return getattr(self._t, key)

        def __getitem__(self, key: str):
            return getattr(self._t, key)

    proxy = _AggResolver(tbl, merged_dimensions, merged_measures)
    meas_exprs = [fn(proxy).name(name) for name, fn in node.aggs.items()]

    # Build metrics mapping for correct signature (FrozenOrderedDict[name, expression])
    metrics = FrozenOrderedDict({expr.get_name(): expr for expr in meas_exprs})

    if group_exprs:
        return tbl.group_by(group_exprs).aggregate(metrics)
    else:
        # No grouping - direct aggregation
        return tbl.aggregate(metrics)


@convert.register(SemanticMutate)
def _lower_semantic_mutate(node: SemanticMutate, catalog, *args):
    agg_tbl = convert(node.source, catalog=catalog)

    @frozen
    class _AggProxy:
        _t: Any

        def __getattr__(self, key: str):
            return self._t[key]

        def __getitem__(self, key: str):
            return self._t[key]

    proxy = _AggProxy(agg_tbl)
    new_cols = (
        [fn(proxy).name(name) for name, fn in node.post.items()] if node.post else []
    )
    return agg_tbl.mutate(new_cols)


def _get_table_column(tbl, col_name: str):
    """Helper to get column from table."""
    if hasattr(tbl, col_name):
        return getattr(tbl, col_name)
    elif col_name in tbl.columns:
        return tbl[col_name]
    else:
        raise ValueError(f"Column '{col_name}' not found in table")


@convert.register(SemanticOrderBy)
def _lower_semantic_orderby(node: SemanticOrderBy, catalog, *args):
    tbl = convert(node.source, catalog=catalog)
    order_keys = []

    for key in node.keys:
        if isinstance(key, str):
            # Simple string key - ascending order
            order_keys.append(_get_table_column(tbl, key))
        elif isinstance(key, tuple) and len(key) == 2 and key[0] == "__deferred__":
            # Deferred expression - resolve by calling the stored function
            deferred_fn = key[1]
            order_keys.append(deferred_fn(tbl))
        else:
            # Other type - use as-is
            order_keys.append(key)

    return tbl.order_by(order_keys)


@convert.register(SemanticLimit)
def _lower_semantic_limit(node: SemanticLimit, catalog, *args):
    tbl = convert(node.source, catalog=catalog)
    if node.offset == 0:
        return tbl.limit(node.n)
    else:
        return tbl.limit(node.n, offset=node.offset)
