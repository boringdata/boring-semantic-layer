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
    _find_root_model,
)

IbisTableExpr = ibis_mod.expr.api.Table

IbisProject = ibis_mod.expr.operations.relations.Project


@frozen
class _Resolver:
    _t: Any
    _dims: dict[str, Any] = field(factory=dict)

    def __getattr__(self, name: str):
        if name in self._dims:
            return self._dims[name](self._t).name(name)
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
    return node.table.to_expr()


@convert.register(SemanticFilter)
def _lower_semantic_filter(node: SemanticFilter, catalog, *args):
    root = _find_root_model(node.source)
    base_tbl = convert(node.source, catalog=catalog)

    # Check if we're filtering after aggregation
    # If the source is a SemanticAggregate, we should only use available columns
    from boring_semantic_layer.semantic_api.ops import SemanticAggregate

    if isinstance(node.source, SemanticAggregate):
        # Post-aggregation filter: only use columns available in the result
        dim_map = {}  # Don't use original dimensions
    else:
        # Pre-aggregation filter: use semantic dimensions
        dim_map = root.dimensions if root else {}

    pred = node.predicate(_Resolver(base_tbl, dim_map))
    return base_tbl.filter(pred)


@convert.register(SemanticProject)
def _lower_semantic_project(node: SemanticProject, catalog, *args):
    root = _find_root_model(node.source)
    if root is None:
        tbl = convert(node.source, catalog=catalog)
        cols = [getattr(tbl, f) for f in node.fields]
        return tbl.select(cols)

    tbl = convert(node.source, catalog=catalog)
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
    root = _find_root_model(node.source)
    tbl = convert(node.source, catalog=catalog)

    group_exprs = []
    for k in node.keys:
        if root and k in root.dimensions:
            group_exprs.append(root.dimensions[k](tbl).name(k))
        else:
            group_exprs.append(getattr(tbl, k).name(k))

    @frozen
    class _AggResolver:
        _t: Any
        _dims: dict[str, Callable]
        _meas: dict[str, Callable]

        def __getattr__(self, key: str):
            if key in self._dims:
                return self._dims[key](self._t)
            if key in self._meas:
                return self._meas[key](self._t)
            return getattr(self._t, key)

        def __getitem__(self, key: str):
            return getattr(self._t, key)

    proxy = _AggResolver(
        tbl, root.dimensions if root else {}, root.measures if root else {}
    )
    meas_exprs = [fn(proxy).name(name) for name, fn in node.aggs.items()]
    return tbl.group_by(group_exprs).aggregate(meas_exprs)


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


@convert.register(SemanticOrderBy)
def _lower_semantic_orderby(node: SemanticOrderBy, catalog, *args):
    tbl = convert(node.source, catalog=catalog)
    order_keys = [getattr(tbl, key) for key in node.keys]
    return tbl.order_by(order_keys)


@convert.register(SemanticLimit)
def _lower_semantic_limit(node: SemanticLimit, catalog, *args):
    tbl = convert(node.source, catalog=catalog)
    if node.offset == 0:
        return tbl.limit(node.n)
    else:
        return tbl.limit(node.n, offset=node.offset)
