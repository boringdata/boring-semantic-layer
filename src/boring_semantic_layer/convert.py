"""Conversion functions for lowering semantic layer operations to Ibis.

This module contains all the converters that register with ibis.expr.sql.convert
to transform semantic layer operations into executable Ibis expressions.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, runtime_checkable

import ibis
from attrs import field, frozen
from ibis.common.collections import FrozenOrderedDict
from ibis.expr import types as ir
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
    SemanticUnnestOp,
    _find_all_root_models,
)

IbisTableExpr = ibis.expr.api.Table
IbisProject = ibis.expr.operations.relations.Project


@runtime_checkable
class AnyTable(Protocol):
    """Protocol for table-like objects supporting column access.

    This protocol describes objects that provide table column access
    through attribute and item notation, returning Ibis column expressions.

    Satisfied by:
    - ir.Table: Direct Ibis tables
    - _Resolver: Proxy with dimension resolution
    - _AggResolver: Proxy for aggregation contexts
    """

    def __getattr__(self, name: str) -> ir.Value: ...
    def __getitem__(self, name: str) -> ir.Value: ...


@frozen
class _Resolver:
    """Resolver for dimensions in filter/join predicates.

    Provides attribute access to dimensions and raw table columns,
    resolving dimension functions to named expressions.
    """

    _t: ir.Table
    _dims: dict[str, Callable] = field(factory=dict)

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

    _t: ir.Table
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

    _t: ir.Table

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
# Helper functions for experimental nested access
# ============================================================================


def _process_nested_access_marker(marker, table):
    """Convert NestedAccessMarker to actual Ibis expression with unnesting.

    Args:
        marker: NestedAccessMarker indicating what unnesting is needed
        table: Base Ibis table

    Returns:
        Tuple of (unnested_table, ibis_expression)
    """
    from boring_semantic_layer.nested_access import NestedAccessMarker

    if not isinstance(marker, NestedAccessMarker):
        return (table, marker)

    # Unnest all array columns in the path
    unnested_tbl = table
    for array_col in marker.array_path:
        if array_col in unnested_tbl.columns:
            unnested_tbl = unnested_tbl.unnest(array_col)

    # Build expression accessing nested fields
    if marker.field_path:
        # Access the unnested array column (which is now a struct)
        expr = getattr(unnested_tbl, marker.array_path[0])
        # Navigate through struct fields
        for field in marker.field_path:
            expr = getattr(expr, field)
    else:
        # No field path - operate on the whole unnested table
        expr = unnested_tbl

    # Apply the aggregation operation
    if marker.operation == "count":
        return (unnested_tbl, unnested_tbl.count())
    elif marker.operation == "sum":
        return (unnested_tbl, expr.sum())
    elif marker.operation == "mean":
        return (unnested_tbl, expr.mean())
    elif marker.operation == "min":
        return (unnested_tbl, expr.min())
    elif marker.operation == "max":
        return (unnested_tbl, expr.max())
    elif marker.operation == "nunique":
        return (unnested_tbl, expr.nunique())
    else:
        raise ValueError(f"Unknown nested access operation: {marker.operation}")


def _evaluate_measure_with_nested_access(measure_fn, table):
    """Evaluate a measure function, detecting and handling NestedAccessMarkers.

    Args:
        measure_fn: Measure function (callable)
        table: Base Ibis table

    Returns:
        Tuple of (unnested_table_or_none, ibis_expression)
        If unnested_table is not None, the measure required unnesting
    """
    from boring_semantic_layer.nested_access import NestedAccessMarker

    # Call the measure function
    result = measure_fn(table)

    # Check if it returned a NestedAccessMarker
    if isinstance(result, NestedAccessMarker):
        return _process_nested_access_marker(result, table)
    else:
        return (None, result)


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
    - Experimental: Automatic unnesting for NestedAccessMarker results
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

    # Evaluate dimension expressions
    dim_exprs = [merged_dimensions[name](tbl).name(name) for name in dims]

    # Evaluate measure expressions, checking for NestedAccessMarkers
    meas_exprs = []
    unnested_tbl = tbl  # Track if we need to unnest the table
    needs_unnesting = False

    for name in meas:
        unnested, expr = _evaluate_measure_with_nested_access(merged_measures[name], tbl)
        if unnested is not None:
            # This measure needs unnesting - use the unnested table
            unnested_tbl = unnested
            needs_unnesting = True
        meas_exprs.append(expr.name(name))

    # Use unnested table if any measure needed it
    active_tbl = unnested_tbl if needs_unnesting else tbl

    # Re-evaluate dimensions on unnested table if needed
    if needs_unnesting and dim_exprs:
        dim_exprs = [merged_dimensions[name](active_tbl).name(name) for name in dims]

    raw_exprs = [getattr(active_tbl, name) for name in raw_fields if hasattr(active_tbl, name)]

    return (
        active_tbl.group_by(dim_exprs).aggregate(meas_exprs)
        if meas_exprs and dim_exprs
        else active_tbl.aggregate(meas_exprs)
        if meas_exprs
        else active_tbl.select(dim_exprs + raw_exprs)
        if dim_exprs or raw_exprs
        else active_tbl
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


@convert.register(SemanticUnnestOp)
def _convert_semantic_unnest(node: SemanticUnnestOp, catalog, *args):
    """Convert SemanticUnnestOp to Ibis unnest.

    Expands array column into separate rows, optionally unpacking struct fields.
    """

    def build_struct_fields(col_expr, col_type):
        """Pure function: build dict of struct field selections."""
        return {name: col_expr[name] for name in col_type.names}

    def unpack_struct_if_needed(unnested_tbl, column_name):
        """Conditionally unpack struct fields into top-level columns."""
        if column_name not in unnested_tbl.columns:
            return unnested_tbl

        col_expr = unnested_tbl[column_name]
        col_type = col_expr.type()

        if hasattr(col_type, "fields") and col_type.fields:
            struct_fields = build_struct_fields(col_expr, col_type)
            return unnested_tbl.select(unnested_tbl, **struct_fields)

        return unnested_tbl

    tbl = convert(node.source, catalog=catalog)

    if node.column not in tbl.columns:
        raise ValueError(f"Column '{node.column}' not found in table")

    try:
        unnested = tbl.unnest(node.column)
    except Exception as e:
        raise ValueError(f"Failed to unnest column '{node.column}': {e}") from e

    return unpack_struct_if_needed(unnested, node.column)
