"""Measure-classification, JSON-definition, and column-error helpers.

These analyze user-supplied measure expressions to decide whether they're
"base" (apply an aggregation to a column) or "calc" (reference other
measures), wrap them so the same pipeline can execute both, and produce
the human-readable diagnostic when a dimension lambda references a
column that doesn't exist on the table.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from difflib import get_close_matches
from typing import Any

from ibis.expr import types as ir
from ibis.expr.operations.relations import Field
from returns.maybe import Maybe, Nothing, Some
from returns.result import Success, safe
from toolz import curry

from ..measure_scope import (
    AggregationExpr,
    AllOf,
    BinOp,
    ColumnScope,
    MeasureRef,
    MethodCall,
)
from ._callable import _CallableWrapper, _infer_unnest
from ._values import Measure, _is_deferred


def _extract_measure_metadata(
    fn_or_expr: Any,
) -> tuple[Any, str | None, tuple, Mapping[str, Any]]:
    """Extract metadata from various measure representations."""
    if isinstance(fn_or_expr, dict):
        return (
            fn_or_expr["expr"],
            fn_or_expr.get("description"),
            tuple(fn_or_expr.get("requires_unnest", [])),
            dict(fn_or_expr.get("metadata") or {}),
        )
    elif isinstance(fn_or_expr, Measure):
        return (
            fn_or_expr.expr,
            fn_or_expr.description,
            fn_or_expr.requires_unnest,
            dict(fn_or_expr.metadata),
        )
    else:
        return (fn_or_expr, None, (), {})


_AGG_METHODS = frozenset({"sum", "mean", "avg", "count", "min", "max"})


def _is_calculated_measure(val: Any) -> bool:
    # A MethodCall with an aggregation method on a MeasureRef is a base measure:
    # the column name matched a known measure name in MeasureScope, but the user
    # is really defining a column aggregation (e.g. lambda t: t.flight_count.sum()).
    if (
        isinstance(val, MethodCall)
        and val.method in _AGG_METHODS
        and isinstance(val.receiver, MeasureRef)
    ):
        return False
    return isinstance(val, MeasureRef | AllOf | BinOp | MethodCall | int | float)


def _matches_aggregation_pattern(measure_expr, agg_expr, tbl):
    if not isinstance(agg_expr, AggregationExpr):
        return Success(False)

    @curry
    def evaluate_in_scope(tbl, expr):
        """Evaluate measure expression in a ColumnScope."""
        scope = ColumnScope(_tbl=tbl)
        return (
            expr.resolve(scope) if _is_deferred(expr) else expr(scope) if callable(expr) else expr
        )

    @curry
    def has_matching_operation(agg_expr, result):
        """Check if the operation matches the expected aggregation.

        All our supported aggregations (Sum, Mean, Count, Min, Max) are ibis operations.
        """
        op_name = type(result.op()).__name__.lower()
        expected_op = "avg" if agg_expr.operation.lower() == "mean" else agg_expr.operation.lower()

        return expected_op in op_name

    @curry
    def has_matching_column(agg_expr, result):
        """Check if result's operation references the expected column.

        All supported aggregation operations (Sum, Mean, Count, Min, Max) have:
        - args[0]: Field operation with .name attribute
        - args[1]: Optional where clause (typically None)
        """
        op = result.op()

        if not isinstance(op.args[0], Field):
            return False

        return op.args[0].name == agg_expr.column

    def matches_pattern(result):
        """Check if result matches both operation and column."""
        return has_matching_operation(agg_expr, result) and has_matching_column(agg_expr, result)

    return safe(lambda: evaluate_in_scope(tbl, measure_expr))().map(matches_pattern)


def _find_matching_measure(agg_expr, known_measures: dict, tbl):
    """Find a measure that matches the aggregation expression pattern.

    Returns Maybe[str] using functional patterns.
    """
    if not isinstance(agg_expr, AggregationExpr):
        return Nothing

    @curry
    def matches_pattern(agg_expr, tbl, measure_obj):
        """Check if measure matches the aggregation pattern.

        All measure_obj values are Measure instances with an expr attribute.
        """
        result = _matches_aggregation_pattern(measure_obj.expr, agg_expr, tbl)
        return result.value_or(False)

    for measure_name, measure_obj in known_measures.items():
        if matches_pattern(agg_expr, tbl, measure_obj):
            return Some(measure_name)

    return Nothing


def _make_base_measure(
    expr: Any,
    description: str | None,
    requires_unnest: tuple,
    metadata: Mapping[str, Any] | None = None,
) -> Measure:
    """Create a base measure with proper callable wrapping using functional patterns."""

    @curry
    def apply_aggregation(operation: str, column):
        """Apply aggregation operation to a column using functional dispatch."""
        operations = {
            "sum": lambda c: c.sum(),
            "mean": lambda c: c.mean(),
            "avg": lambda c: c.mean(),
            "count": lambda c: c.count(),
            "min": lambda c: c.min(),
            "max": lambda c: c.max(),
        }

        return (
            Maybe.from_optional(operations.get(operation))
            .map(lambda fn: fn(column))
            .value_or(
                (_ for _ in ()).throw(ValueError(f"Unknown aggregation operation: {operation}"))
            )
        )

    @curry
    def evaluate_expr(expr, scope):
        """Evaluate expression in given scope."""
        return (
            expr.resolve(scope) if _is_deferred(expr) else expr(scope) if callable(expr) else expr
        )

    def convert_aggregation_expr(t, agg_expr: AggregationExpr):
        """Convert AggregationExpr to ibis expression."""
        if agg_expr.operation == "count":
            result = t.count()
        else:
            result = apply_aggregation(agg_expr.operation, t[agg_expr.column])

        for method_name, args, kwargs_tuple in agg_expr.post_ops:
            result = getattr(result, method_name)(*args, **dict(kwargs_tuple))

        return result

    raw_expr = expr._fn if isinstance(expr, _CallableWrapper) else expr

    if isinstance(expr, AggregationExpr):

        def wrapped_expr(t):
            """Convert AggregationExpr to ibis expression."""
            return convert_aggregation_expr(t, expr)

        return Measure(
            expr=wrapped_expr,
            description=description,
            requires_unnest=requires_unnest,
            original_expr=raw_expr,
            metadata=dict(metadata or {}),
        )

    if callable(expr):

        def wrapped_expr(t):
            """Wrapped expression that handles AggregationExpr conversion."""
            scope = ColumnScope(_tbl=t)
            result = evaluate_expr(expr, scope)

            if isinstance(result, AggregationExpr):
                return convert_aggregation_expr(t, result)
            return result

        return Measure(
            expr=wrapped_expr,
            description=description,
            requires_unnest=requires_unnest,
            original_expr=raw_expr,
            metadata=dict(metadata or {}),
        )
    else:
        return Measure(
            expr=lambda t, fn=expr: evaluate_expr(fn, ColumnScope(_tbl=t)),
            description=description,
            requires_unnest=requires_unnest,
            original_expr=raw_expr,
            metadata=dict(metadata or {}),
        )


def _classify_measure(
    fn_or_expr: Any, scope: Any, measure_name: str | None = None
) -> tuple[str, Any]:
    """Classify measure as 'calc' or 'base' with appropriate handling."""
    from ..measure_scope import validate_calc_ast
    from ._core import _resolve_expr

    expr, description, requires_unnest, metadata = _extract_measure_metadata(fn_or_expr)

    resolved = safe(lambda: _resolve_expr(expr, scope))().map(
        lambda val: ("calc", val) if _is_calculated_measure(val) else None
    )

    if isinstance(resolved, Success) and resolved.unwrap() is not None:
        kind, value = resolved.unwrap()
        validate_calc_ast(value, measure_name)
        return (kind, value)

    if not requires_unnest and callable(expr):
        # All scopes (MeasureScope, ColumnScope) have tbl attribute
        table = scope.tbl
        inferred_unnest = _infer_unnest(expr, table)
        requires_unnest = requires_unnest or inferred_unnest

    return ("base", _make_base_measure(expr, description, requires_unnest, metadata))


def _build_json_definition(
    dims_dict: dict,
    meas_dict: dict,
    name: str | None = None,
    description: str | None = None,
) -> dict:
    result = {
        "dimensions": {n: spec.to_json() for n, spec in dims_dict.items()},
        "measures": {n: spec.to_json() for n, spec in meas_dict.items()},
        "entity_dimensions": {n: spec.to_json() for n, spec in dims_dict.items() if spec.is_entity},
        "event_timestamp": {
            n: spec.to_json() for n, spec in dims_dict.items() if spec.is_event_timestamp
        },
        "time_dimensions": {
            n: spec.to_json() for n, spec in dims_dict.items() if spec.is_time_dimension
        },
        "name": name,
    }
    if description is not None:
        result["description"] = description
    return result


def _format_column_error(e: AttributeError, table: ir.Table) -> str:
    """Format a helpful error message for missing column errors."""
    # Extract the column name from the error
    match = re.search(r"has no attribute ['\"]([^'\"]+)['\"]", str(e))
    missing_col = match.group(1) if match else "unknown"

    # Get available columns
    available_cols = list(table.columns) if hasattr(table, "columns") else []

    # Build error message
    parts = [f"Dimension expression references non-existent column '{missing_col}'."]

    if len(available_cols) > 20:
        parts.append(f"Table has {len(available_cols)} columns. First 15: {available_cols[:15]}")
    elif available_cols:
        parts.append(f"Available columns: {available_cols}")
    else:
        parts.append(f"No columns available in {type(table).__name__} object")

    # Suggest similar column names
    suggestions = get_close_matches(missing_col, available_cols, n=3, cutoff=0.6)
    if suggestions:
        parts[-1] += f". Did you mean: {suggestions}?"

    # Add helpful tip
    example = suggestions[0] if suggestions else "column_name"
    parts.append(
        f"\n\nTip: Check that your dimension expression uses the correct column name. "
        f"For example: lambda t: t.{example}"
    )

    return " ".join(parts)
