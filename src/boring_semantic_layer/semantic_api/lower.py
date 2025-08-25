from __future__ import annotations

from typing import Any

from ibis.expr.sql import convert
from boring_semantic_layer.semantic_api.ops import (
    SemanticAggregate,
    SemanticFilter,
    SemanticGroupBy,
    SemanticJoin,
    SemanticMutate,
    SemanticProject,
    SemanticTable,
    _find_root_model,
)
from ibis.expr.types import Table as IbisTableExpr

# Handle vanilla Ibis Project operations (column pruning/projection)
from ibis.expr.operations.relations import Project as IbisProject


# Helper: Proxy to resolve attributes either from semantic dims or from a table's columns
class _Resolver:
    def __init__(self, base_table, dim_map: dict[str, Any] | None = None):
        self._t = base_table
        self._dims = dim_map or {}

    def __getattr__(self, name: str):
        if name in self._dims:
            return self._dims[name](self._t).name(name)
        # Fallback to base columns
        return getattr(self._t, name)

    def __getitem__(self, name: str):
        return self.__getattr__(name)


@convert.register(IbisTableExpr)
def _convert_ibis_table(expr, catalog, *args):
    # unwrap Expr to its ops.Node for further dispatch
    return convert(expr.op(), catalog=catalog)


@convert.register(IbisProject)
def _lower_ibis_project(op: IbisProject, catalog, *args):
    # Project a subset of columns from a table expression
    tbl = convert(op.parent, catalog=catalog)
    cols = [v.to_expr().name(k) for k, v in op.values.items()]
    return tbl.select(cols)


@convert.register(SemanticTable)
def _lower_semantic_table(node: SemanticTable, catalog, *args):
    # Base table: just unwrap to expr()
    return node.table.to_expr()


@convert.register(SemanticFilter)
def _lower_semantic_filter(node: SemanticFilter, catalog, *args):
    root = _find_root_model(node.source)
    base_tbl = convert(node.source, catalog=catalog)
    dim_map = root.dimensions if root else {}
    pred = node.predicate(_Resolver(base_tbl, dim_map))
    return base_tbl.filter(pred)


@convert.register(SemanticProject)
def _lower_semantic_project(node: SemanticProject, catalog, *args):
    root = _find_root_model(node.source)
    if root is None:
        # No model — just select raw columns by name if they exist
        tbl = convert(node.source, catalog=catalog)
        cols = [getattr(tbl, f) for f in node.fields]
        return tbl.select(cols)

    tbl = convert(
        node.source, catalog=catalog
    )  # base table expr, include filters if any
    dims = [f for f in node.fields if f in root.dimensions]
    meas = [f for f in node.fields if f in root.measures]

    dim_exprs = [root.dimensions[name](tbl).name(name) for name in dims]
    meas_exprs = [root.measures[name](tbl).name(name) for name in meas]

    if meas_exprs:
        return tbl.group_by(dim_exprs).aggregate(meas_exprs)
    else:
        return tbl.select(dim_exprs) if dim_exprs else tbl  # no-op if nothing selected


@convert.register(SemanticGroupBy)
def _lower_semantic_groupby(node: SemanticGroupBy, catalog, *args):
    # Marker only — actual grouping is handled by a following SemanticAggregate
    return convert(node.source, catalog=catalog)


@convert.register(SemanticJoin)
def _lower_semantic_join(node: SemanticJoin, catalog, *args):
    # Join two semantic subplans
    left_tbl = convert(node.left, catalog=catalog)
    right_tbl = convert(node.right, catalog=catalog)
    if node.on is not None:
        pred = node.on(_Resolver(left_tbl), _Resolver(right_tbl))
        # Pass predicate as positional argument to match ibis join signature
        return left_tbl.join(right_tbl, pred, how=node.how)
    else:
        return left_tbl.join(right_tbl, how=node.how)


@convert.register(SemanticAggregate)
def _lower_semantic_aggregate(node: SemanticAggregate, catalog, *args):
    root = _find_root_model(node.source)
    tbl = convert(root if root else node.source, catalog=catalog)

    # Resolve grouping keys using dims first, then raw columns
    group_exprs = []
    for k in node.keys:
        if root and k in root.dimensions:
            group_exprs.append(root.dimensions[k](tbl).name(k))
        else:
            group_exprs.append(getattr(tbl, k).name(k))

    # Allow user-defined aggs to reference root dimensions and measures via proxy
    class _AggResolver:
        def __init__(self, table):
            self._t = table
            self._dims = root.dimensions if root else {}
            self._meas = root.measures if root else {}

        def __getattr__(self, key: str):
            if key in self._dims:
                return self._dims[key](self._t)
            if key in self._meas:
                return self._meas[key](self._t)
            return getattr(self._t, key)

        def __getitem__(self, key: str):
            return getattr(self._t, key)

    proxy = _AggResolver(tbl)
    meas_exprs = [fn(proxy).name(name) for name, fn in node.aggs.items()]
    return tbl.group_by(group_exprs).aggregate(meas_exprs)


@convert.register(SemanticMutate)
def _lower_semantic_mutate(node: SemanticMutate, catalog, *args):
    # Lower the upstream plan first (usually a group_by+aggregate)
    agg_tbl = convert(node.source, catalog=catalog)

    # Mutations reference columns on the aggregated table
    class _AggProxy:
        def __init__(self, t):
            self._t = t

        def __getattr__(self, key):
            # Always treat attributes as column lookups to avoid method conflicts
            return self._t[key]

        def __getitem__(self, key):
            return self._t[key]

    proxy = _AggProxy(agg_tbl)
    new_cols = (
        [fn(proxy).name(name) for name, fn in node.post.items()] if node.post else []
    )
    return agg_tbl.mutate(new_cols)
