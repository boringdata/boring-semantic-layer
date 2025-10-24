from __future__ import annotations

from typing import Any, Callable, Mapping, Sequence

import ibis as ibis_mod
from attrs import frozen, field
from ibis.expr.sql import convert  # noqa: E402
from boring_semantic_layer.ops import (  # noqa: E402
    SemanticAggregate,
    SemanticFilterRelation,
    SemanticGroupBy,
    SemanticJoin,
    SemanticMutate,
    SemanticOrderBy,
    SemanticProject,
    SemanticTableRelation,
    SemanticLimit,
    _find_all_root_models,
    _merge_fields_with_prefixing,
)
from ibis.common.collections import FrozenOrderedDict
from ibis.expr import format as fmt

IbisTableExpr = ibis_mod.expr.api.Table

IbisProject = ibis_mod.expr.operations.relations.Project


# Register custom formatter for SemanticTableRelation
@fmt.fmt.register(SemanticTableRelation)
def _format_semantic_table(op: SemanticTableRelation, **kwargs):
    """Format SemanticTableRelation with semantic metadata."""
    # Access the FrozenDicts directly via object.__getattribute__ to bypass custom __getattribute__
    dims_dict = object.__getattribute__(op, 'dimensions')
    base_measures = object.__getattribute__(op, 'measures')
    calc_measures = object.__getattribute__(op, 'calc_measures')

    dims = list(dims_dict.keys())
    measures = list(base_measures.keys()) + list(calc_measures.keys())

    dims_str = ', '.join(dims[:5])
    if len(dims) > 5:
        dims_str += f', ... ({len(dims)} total)'

    meas_str = ', '.join(measures[:5])
    if len(measures) > 5:
        meas_str += f', ... ({len(measures)} total)'

    lines = [f"SemanticTableRelation[{op.name}]"]
    if dims:
        lines.append(f"  dimensions: {dims_str}")
    if measures:
        lines.append(f"  measures: {meas_str}")

    # Show underlying table type
    table_type = type(op.table).__name__
    lines.append(f"  table: {table_type}")

    return '\n'.join(lines)


# Register custom formatter for SemanticFilterRelation
@fmt.fmt.register(SemanticFilterRelation)
def _format_semantic_filter(op: SemanticFilterRelation, **kwargs):
    """Format SemanticFilterRelation showing source and predicate info."""
    source_type = type(op.source).__name__

    lines = ["SemanticFilterRelation"]
    lines.append(f"  source: {source_type}")
    lines.append(f"  predicate: <function>")

    # If source has dimensions/measures, show count
    if hasattr(op.source, 'dimensions'):
        dims_dict = object.__getattribute__(op.source, 'dimensions')
        if dims_dict:
            lines.append(f"  inherited dimensions: {len(dims_dict)}")

    if hasattr(op.source, 'measures'):
        meas_dict = object.__getattribute__(op.source, 'measures')
        calc_dict = object.__getattribute__(op.source, 'calc_measures')
        total_measures = len(meas_dict) + len(calc_dict)
        if total_measures:
            lines.append(f"  inherited measures: {total_measures}")

    return '\n'.join(lines)


@frozen
class _Resolver:
    _t: Any
    _dims: dict[str, Any] = field(factory=dict)

    def __getattr__(self, name: str):
        return (self._dims[name](self._t).name(name) if name in self._dims
                else next((dim_func(self._t).name(dim_name)
                         for dim_name, dim_func in self._dims.items()
                         if dim_name.endswith(f".{name}")), getattr(self._t, name)))

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


@convert.register(SemanticTableRelation)
def _lower_semantic_table(node: SemanticTableRelation, catalog, *args):
    return convert(node.table, catalog=catalog)


@convert.register(SemanticFilterRelation)
def _lower_semantic_filter(node: SemanticFilterRelation, catalog, *args):
    from boring_semantic_layer.ops import SemanticAggregate, _get_merged_fields

    all_roots = _find_all_root_models(node.source)
    base_tbl = convert(node.source, catalog=catalog)

    dim_map = {} if isinstance(node.source, SemanticAggregate) else _get_merged_fields(all_roots, 'dims')
    pred = node.predicate(_Resolver(base_tbl, dim_map))
    return base_tbl.filter(pred)


@convert.register(SemanticProject)
def _lower_semantic_project(node: SemanticProject, catalog, *args):
    from boring_semantic_layer.ops import _get_merged_fields

    all_roots = _find_all_root_models(node.source)
    tbl = convert(node.source, catalog=catalog)

    if not all_roots:
        return tbl.select([getattr(tbl, f) for f in node.fields])

    merged_dimensions = _get_merged_fields(all_roots, 'dims')
    merged_measures = _get_merged_fields(all_roots, 'measures')

    dims = [f for f in node.fields if f in merged_dimensions]
    meas = [f for f in node.fields if f in merged_measures]
    raw_fields = [f for f in node.fields if f not in merged_dimensions and f not in merged_measures]

    dim_exprs = [merged_dimensions[name](tbl).name(name) for name in dims]
    meas_exprs = [merged_measures[name](tbl).name(name) for name in meas]
    raw_exprs = [getattr(tbl, name) for name in raw_fields if hasattr(tbl, name)]

    return (tbl.group_by(dim_exprs).aggregate(meas_exprs) if meas_exprs and dim_exprs
            else tbl.aggregate(meas_exprs) if meas_exprs
            else tbl.select(dim_exprs + raw_exprs) if dim_exprs or raw_exprs
            else tbl)


@convert.register(SemanticGroupBy)
def _lower_semantic_groupby(node: SemanticGroupBy, catalog, *args):
    return convert(node.source, catalog=catalog)


@convert.register(SemanticJoin)
def _lower_semantic_join(node: SemanticJoin, catalog, *args):
    left_tbl = convert(node.left, catalog=catalog)
    right_tbl = convert(node.right, catalog=catalog)
    return (left_tbl.join(right_tbl, node.on(_Resolver(left_tbl), _Resolver(right_tbl)), how=node.how)
            if node.on is not None
            else left_tbl.join(right_tbl, how=node.how))


@convert.register(SemanticAggregate)
def _lower_semantic_aggregate(node: SemanticAggregate, catalog, *args):
    from boring_semantic_layer.ops import _get_merged_fields

    all_roots = _find_all_root_models(node.source)
    tbl = convert(node.source, catalog=catalog)

    merged_dimensions = _get_merged_fields(all_roots, 'dims')
    merged_measures = _get_merged_fields(all_roots, 'measures')

    group_exprs = [(merged_dimensions[k](tbl).name(k) if k in merged_dimensions
                    else getattr(tbl, k).name(k)) for k in node.keys]

    @frozen
    class _AggResolver:
        _t: Any
        _dims: dict[str, Callable]
        _meas: dict[str, Callable]

        def __getattr__(self, key: str):
            return (self._dims[key](self._t) if key in self._dims
                    else self._meas[key](self._t) if key in self._meas
                    else next((dim_func(self._t) for dim_name, dim_func in self._dims.items()
                             if dim_name.endswith(f".{key}")), None) or
                         next((meas_func(self._t) for meas_name, meas_func in self._meas.items()
                              if meas_name.endswith(f".{key}")), None) or
                         getattr(self._t, key))

        def __getitem__(self, key: str):
            return getattr(self._t, key)

    proxy = _AggResolver(tbl, merged_dimensions, merged_measures)
    meas_exprs = [fn(proxy).name(name) for name, fn in node.aggs.items()]
    metrics = FrozenOrderedDict({expr.get_name(): expr for expr in meas_exprs})

    return tbl.group_by(group_exprs).aggregate(metrics) if group_exprs else tbl.aggregate(metrics)


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
    new_cols = [fn(proxy).name(name) for name, fn in node.post.items()]
    return agg_tbl.mutate(new_cols) if new_cols else agg_tbl


@convert.register(SemanticOrderBy)
def _lower_semantic_orderby(node: SemanticOrderBy, catalog, *args):
    tbl = convert(node.source, catalog=catalog)

    def resolve_key(key):
        return (getattr(tbl, key) if hasattr(tbl, key)
                else tbl[key] if isinstance(key, str) and key in tbl.columns
                else key[1](tbl) if isinstance(key, tuple) and len(key) == 2 and key[0] == "__deferred__"
                else key)

    return tbl.order_by([resolve_key(key) for key in node.keys])


@convert.register(SemanticLimit)
def _lower_semantic_limit(node: SemanticLimit, catalog, *args):
    tbl = convert(node.source, catalog=catalog)
    return tbl.limit(node.n) if node.offset == 0 else tbl.limit(node.n, offset=node.offset)
