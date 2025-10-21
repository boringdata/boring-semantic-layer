"""
Query builder for semantic API that bridges the gap between old QueryExpr interface and new SemanticTable.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import ibis as ibis_mod

from ..filters import Filter
from ..time_grain import TimeGrain, TIME_GRAIN_TRANSFORMATIONS


def build_query(
    semantic_table: "SemanticTable",
    dimensions: Optional[List[str]] = [],
    measures: Optional[List[str]] = None,
    filters: Optional[List[Union[Dict[str, Any], str, Callable]]] = None,
    order_by: Optional[List[Tuple[str, str]]] = None,
    limit: Optional[int] = None,
    time_range: Optional[Dict[str, str]] = None,
    time_grain: Optional[Union[str, TimeGrain]] = None,
) -> "SemanticTable":
    """
    Build a SemanticTable from query parameters.

    This function bridges the gap between the old QueryExpr interface (used by MCP)
    and the new SemanticTable method chaining API.

    Args:
        semantic_table: The base SemanticTable to build upon
        dimensions: List of dimension names to include
        measures: List of measure names to include
        filters: List of filters (dict, str, callable, or Filter)
        order_by: List of (field, direction) tuples for ordering
        limit: Maximum number of rows to return
        time_range: Dict with 'start' and 'end' keys for time filtering
        time_grain: The time grain to use for the time dimension

    Returns:
        SemanticTable: A properly configured semantic table
    """
    from .table import SemanticTable

    # semantic_table is always a SemanticTable instance
    expr = semantic_table

    # Find time dimensions using the semantic table's dimensions
    time_dimensions = {}
    if hasattr(expr, '_dims'):
        # Check each dimension to see if it's a time dimension
        # This is a simple heuristic - could be improved with explicit time dimension metadata
        for dim_name in expr._dims.keys():
            if 'date' in dim_name.lower() or 'time' in dim_name.lower() or 'created' in dim_name.lower():
                time_dimensions[dim_name] = expr._dims[dim_name]

    # Step 1: Apply filters first (before grouping/aggregation)
    if filters:
        for filter_spec in filters:
            filter_obj = (
                filter_spec
                if isinstance(filter_spec, Filter)
                else Filter(filter=filter_spec)
            )
            expr = expr.filter(
                lambda t, f=filter_obj: f.to_ibis(t, model=None)
            )

    # Step 2: Apply time range filter if specified
    # Apply to the first time dimension found (or could be extended to specify which one)
    if time_range and time_dimensions:
        start_date = time_range.get("start")
        end_date = time_range.get("end")

        # Use the first time dimension found
        primary_time_dim = next(iter(time_dimensions.keys()))

        # Apply time range filters using the primary time dimension
        if start_date:
            expr = expr.filter(
                lambda t, td=primary_time_dim: getattr(t, td) >= start_date
            )
        if end_date:
            expr = expr.filter(
                lambda t, td=primary_time_dim: getattr(t, td) <= end_date
            )

    # Step 4: Handle time grain transformation by creating a new dimension
    if time_grain and time_dimensions and time_grain in TIME_GRAIN_TRANSFORMATIONS:
        time_transform = TIME_GRAIN_TRANSFORMATIONS[time_grain]

        # Apply time grain transformation to all time dimensions
        for time_dim_name, time_dim in time_dimensions.items():
            # Check if the requested time grain is valid for this dimension
            if (
                hasattr(time_dim, "smallest_time_grain")
                and time_dim.smallest_time_grain is not None
            ):
                # For now, apply the transformation if requested
                # TODO: Add validation that time_grain is compatible with smallest_time_grain
                pass

            time_grain_dim_name = f"{time_dim_name}_by_{time_grain.lower()}"

            # Add the time grain dimension to the semantic table expression
            expr = expr.with_dimensions(
                **{
                    time_grain_dim_name: lambda t,
                    td=time_dim_name,
                    transform=time_transform: transform(getattr(t, td))
                }
            )

            # Add the time grain dimension to the dimensions list
            dimensions.append(time_grain_dim_name)

    # Step 5: Group by dimensions
    if len(dimensions) > 0:
        expr = expr.group_by(*dimensions)

    # Add aggregation if measures are specified
    if measures:
        # Create aggregation dictionary
        measures_dict = {}
        for meas in measures:
            # Create a closure to capture the measure name
            measures_dict[meas] = lambda t, field=meas: getattr(t, field)

        expr = expr.aggregate(**measures_dict)

    # Step 5: Apply ordering
    if order_by:
        order_keys = []
        for field, direction in order_by:
            if direction.lower() == "desc":
                order_keys.append(ibis_mod.desc(field))
            else:
                order_keys.append(field)
        expr = expr.order_by(*order_keys)

    # Step 6: Apply limit
    if limit:
        expr = expr.limit(limit)

    return expr
