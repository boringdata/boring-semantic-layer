"""
Query interface for semantic API with filter and time dimension support.

Provides parameter-based querying as an alternative to method chaining.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from operator import eq, ge, gt, le, lt, ne
from typing import Any, ClassVar, Literal

import ibis
from attrs import frozen
from ibis.common.collections import FrozenDict
from toolz import curry

from .utils import safe_eval

# Time grain type alias
TimeGrain = Literal[
    "TIME_GRAIN_YEAR",
    "TIME_GRAIN_QUARTER",
    "TIME_GRAIN_MONTH",
    "TIME_GRAIN_WEEK",
    "TIME_GRAIN_DAY",
    "TIME_GRAIN_HOUR",
    "TIME_GRAIN_MINUTE",
    "TIME_GRAIN_SECOND",
]

# Mapping of time grain identifiers to ibis truncate units (immutable)
TIME_GRAIN_TRANSFORMATIONS: FrozenDict = {
    "TIME_GRAIN_YEAR": "Y",
    "TIME_GRAIN_QUARTER": "Q",
    "TIME_GRAIN_MONTH": "M",
    "TIME_GRAIN_WEEK": "W",
    "TIME_GRAIN_DAY": "D",
    "TIME_GRAIN_HOUR": "h",
    "TIME_GRAIN_MINUTE": "m",
    "TIME_GRAIN_SECOND": "s",
}

# Order of grains from finest to coarsest (immutable)
TIME_GRAIN_ORDER: tuple[str, ...] = (
    "TIME_GRAIN_SECOND",
    "TIME_GRAIN_MINUTE",
    "TIME_GRAIN_HOUR",
    "TIME_GRAIN_DAY",
    "TIME_GRAIN_WEEK",
    "TIME_GRAIN_MONTH",
    "TIME_GRAIN_QUARTER",
    "TIME_GRAIN_YEAR",
)


# Helper functions using operator module instead of lambdas
def _ibis_isin(x, y):
    return x.isin(y)


def _ibis_not_isin(x, y):
    return ~x.isin(y)


def _ibis_like(x, y):
    return x.like(y)


def _ibis_not_like(x, y):
    return ~x.like(y)


def _ibis_ilike(x, y):
    return x.ilike(y)


def _ibis_not_ilike(x, y):
    return ~x.ilike(y)


def _ibis_isnull(x, _):
    return x.isnull()


def _ibis_notnull(x, _):
    return x.notnull()


def _ibis_and(x, y):
    return x & y


def _ibis_or(x, y):
    return x | y


# Operator mapping using operator module functions where possible
OPERATOR_MAPPING: FrozenDict = {
    "=": eq,
    "eq": eq,
    "equals": eq,
    "!=": ne,
    ">": gt,
    ">=": ge,
    "<": lt,
    "<=": le,
    "in": _ibis_isin,
    "not in": _ibis_not_isin,
    "like": _ibis_like,
    "not like": _ibis_not_like,
    "ilike": _ibis_ilike,
    "not ilike": _ibis_not_ilike,
    "is null": _ibis_isnull,
    "is not null": _ibis_notnull,
    "AND": _ibis_and,
    "OR": _ibis_or,
}


@curry
def _is_time_dimension(dims_dict: dict[str, Any], dim_name: str) -> bool:
    """Check if a dimension is a time dimension (curried for partial application)."""
    return dim_name in dims_dict and dims_dict[dim_name].is_time_dimension


def _find_time_dimension(semantic_table: Any, dimensions: list[str]) -> str | None:
    """
    Find the first time dimension in the query dimensions list.

    Uses functional composition to find matching dimension.
    """
    dims_dict = semantic_table.get_dimensions()
    is_time_dim = _is_time_dimension(dims_dict)
    return next((dim for dim in dimensions if is_time_dim(dim)), None)


@curry
def _make_grain_id(grain: str) -> str:
    """Convert grain name to TIME_GRAIN_ identifier (curried)."""
    return f"TIME_GRAIN_{grain.upper()}"


def _validate_time_grain(
    time_grain: TimeGrain,
    smallest_allowed_grain: str | None,
    dimension_name: str,
) -> None:
    """
    Validate that requested time grain is not finer than smallest allowed grain.

    Raises:
        ValueError: If requested grain is finer than allowed grain.
    """
    if not smallest_allowed_grain:
        return

    smallest_grain = _make_grain_id(smallest_allowed_grain)
    if smallest_grain not in TIME_GRAIN_ORDER:
        return

    requested_idx = TIME_GRAIN_ORDER.index(time_grain)
    smallest_idx = TIME_GRAIN_ORDER.index(smallest_grain)

    if requested_idx < smallest_idx:
        raise ValueError(
            f"Requested time grain '{time_grain}' is finer than the smallest "
            f"allowed grain '{smallest_allowed_grain}' for dimension '{dimension_name}'",
        )


@frozen(kw_only=True, slots=True)
class Filter:
    """
    Unified filter class supporting JSON, string, and callable formats.

    Examples:
        # JSON simple filter
        Filter(filter={"field": "country", "operator": "=", "value": "US"})

        # JSON compound filter
        Filter(filter={
            "operator": "AND",
            "conditions": [
                {"field": "country", "operator": "=", "value": "US"},
                {"field": "tier", "operator": "in", "values": ["gold", "platinum"]}
            ]
        })

        # String expression (evaluated with ibis._)
        Filter(filter="_.carrier == 'AA'")

        # Callable function
        Filter(filter=lambda t: t.amount > 1000)
    """

    filter: FrozenDict | str | Callable

    OPERATORS: ClassVar[set] = set(OPERATOR_MAPPING.keys())
    COMPOUND_OPERATORS: ClassVar[set] = {"AND", "OR"}

    def __attrs_post_init__(self) -> None:
        if not isinstance(self.filter, dict | str) and not callable(self.filter):
            raise ValueError("Filter must be a dict, string, or callable")

    def _get_field_expr(self, field: str) -> Any:
        """Get field expression using ibis._ for unbound reference.

        For prefixed fields (e.g., 'customers.country'), use only the field name
        since joined tables flatten the columns to the top level.
        """
        if "." in field:
            # Extract just the field name, ignoring the table prefix
            # e.g., 'customers.country' -> 'country'
            _table_name, field_name = field.split(".", 1)
            return getattr(ibis._, field_name)
        return getattr(ibis._, field)

    def _parse_json_filter(self, filter_obj: FrozenDict) -> Any:
        """Parse JSON filter object into ibis expression."""
        # Compound filters (AND/OR)
        if filter_obj.get("operator") in self.COMPOUND_OPERATORS:
            conditions = filter_obj.get("conditions")
            if not conditions:
                raise ValueError("Compound filter must have non-empty conditions list")
            expr = self._parse_json_filter(conditions[0])
            for cond in conditions[1:]:
                next_expr = self._parse_json_filter(cond)
                expr = OPERATOR_MAPPING[filter_obj["operator"]](expr, next_expr)
            return expr

        # Simple filter
        field = filter_obj.get("field")
        op = filter_obj.get("operator")
        if field is None or op is None:
            raise KeyError(
                "Missing required keys in filter: 'field' and 'operator' are required",
            )

        field_expr = self._get_field_expr(field)

        if op not in self.OPERATORS:
            raise ValueError(f"Unsupported operator: {op}")

        # List membership operators
        if op in ("in", "not in"):
            values = filter_obj.get("values")
            if values is None:
                raise ValueError(f"Operator '{op}' requires 'values' field")
            return OPERATOR_MAPPING[op](field_expr, values)

        # Null checks
        if op in ("is null", "is not null"):
            if any(k in filter_obj for k in ("value", "values")):
                raise ValueError(
                    f"Operator '{op}' should not have 'value' or 'values' fields",
                )
            return OPERATOR_MAPPING[op](field_expr, None)

        # Single value operators
        value = filter_obj.get("value")
        if value is None:
            raise ValueError(f"Operator '{op}' requires 'value' field")
        return OPERATOR_MAPPING[op](field_expr, value)

    def to_callable(self) -> Callable:
        """Convert filter to callable that can be used with SemanticTable.filter()."""
        if isinstance(self.filter, dict):
            expr = self._parse_json_filter(self.filter)
            return lambda t: expr.resolve(t)
        elif isinstance(self.filter, str):
            expr = safe_eval(
                self.filter,
                context={"_": ibis._, "ibis": ibis},
            ).unwrap()
            return lambda t: expr.resolve(t)
        elif callable(self.filter):
            return self.filter
        raise ValueError("Filter must be a dict, string, or callable")


@curry
def _normalize_filter(
    filter_spec: dict[str, Any] | str | Callable | Filter,
) -> Callable:
    """
    Normalize filter specification to callable (curried for composition).

    Accepts dict, string, callable, or Filter and returns unified callable.
    """
    if isinstance(filter_spec, Filter):
        return filter_spec.to_callable()
    elif isinstance(filter_spec, dict | str):
        return Filter(filter=filter_spec).to_callable()
    elif callable(filter_spec):
        return filter_spec
    else:
        raise ValueError(f"Unsupported filter type: {type(filter_spec)}")


@curry
def _make_time_range_filter(time_dim_name: str, start: str, end: str) -> Callable:
    """Create time range filter predicate (curried)."""
    return lambda t, dim=time_dim_name, s=start, e=end: (
        (t[dim] >= ibis.timestamp(s)) & (t[dim] <= ibis.timestamp(e))
    )


@curry
def _make_order_key(field: str, direction: str):
    """Create order key for sorting (curried)."""
    return ibis.desc(field) if direction.lower() == "desc" else field


def query(
    semantic_table: Any,  # SemanticModel, but avoiding circular import
    dimensions: Sequence[str] | None = None,
    measures: Sequence[str] | None = None,
    filters: Sequence[dict[str, Any] | str | Callable | Filter] | None = None,
    order_by: Sequence[tuple[str, str]] | None = None,
    limit: int | None = None,
    time_grain: TimeGrain | None = None,
    time_range: Mapping[str, str] | None = None,
) -> Any:  # Returns SemanticModel or SemanticAggregate
    """
    Query semantic table using parameter-based interface with time dimension support.

    Args:
        semantic_table: The SemanticTable to query
        dimensions: List of dimension names to group by
        measures: List of measure names to aggregate
        filters: List of filters (dict, str, callable, or Filter objects)
        order_by: List of (field, direction) tuples
        limit: Maximum number of rows to return
        time_grain: Optional time grain to apply to time dimensions (e.g., "TIME_GRAIN_MONTH")
        time_range: Optional time range filter with 'start' and 'end' keys

    Returns:
        SemanticAggregate or SemanticTable ready for execution

    Examples:
        # Basic query
        result = st.query(
            dimensions=["carrier"],
            measures=["flight_count"]
        ).execute()

        # With JSON filter
        result = st.query(
            dimensions=["carrier"],
            measures=["flight_count"],
            filters=[{"field": "distance", "operator": ">", "value": 1000}]
        ).execute()

        # With time grain
        result = st.query(
            dimensions=["order_date"],
            measures=["total_sales"],
            time_grain="TIME_GRAIN_MONTH"
        ).execute()

        # With time range
        result = st.query(
            dimensions=["order_date"],
            measures=["total_sales"],
            time_range={"start": "2024-01-01", "end": "2024-12-31"}
        ).execute()
    """
    from .ops import Dimension

    result = semantic_table
    dimensions = dimensions or []
    filters = list(filters or [])  # Copy to avoid mutating input

    # Step 0: Add time_range as a filter if specified
    if time_range:
        if not isinstance(time_range, dict) or "start" not in time_range or "end" not in time_range:
            raise ValueError("time_range must be a dict with 'start' and 'end' keys")

        time_dim_name = _find_time_dimension(result, dimensions)
        if not time_dim_name:
            raise ValueError(
                "time_range filter requires a time dimension in the query dimensions. "
                f"Available dimensions: {list(dimensions)}. "
                "Mark a dimension as a time dimension using: "
                ".with_dimensions(dim_name={'expr': lambda t: t.column, 'is_time_dimension': True})"
            )
        filters.append(
            _make_time_range_filter(
                time_dim_name,
                time_range["start"],
                time_range["end"],
            ),
        )

    # Step 1: Handle time grain transformations
    if time_grain:
        if time_grain not in TIME_GRAIN_TRANSFORMATIONS:
            raise ValueError(
                f"Invalid time_grain: {time_grain}. Must be one of {list(TIME_GRAIN_TRANSFORMATIONS.keys())}",
            )

        # Find time dimensions and apply grain transformation
        time_dims_to_transform = {}
        dims_dict = result.get_dimensions()
        for dim_name in dimensions:
            if dim_name in dims_dict:
                dim_obj = dims_dict[dim_name]
                if dim_obj.is_time_dimension:
                    # Validate grain
                    _validate_time_grain(
                        time_grain,
                        dim_obj.smallest_time_grain,
                        dim_name,
                    )

                    # Create transformed dimension
                    truncate_unit = TIME_GRAIN_TRANSFORMATIONS[time_grain]
                    orig_expr = dim_obj.expr
                    time_dims_to_transform[dim_name] = Dimension(
                        expr=lambda t, orig=orig_expr, unit=truncate_unit: orig(
                            t,
                        ).truncate(unit),
                        description=dim_obj.description,
                        is_time_dimension=dim_obj.is_time_dimension,
                        smallest_time_grain=dim_obj.smallest_time_grain,
                    )

        # Apply transformations
        if time_dims_to_transform:
            result = result.with_dimensions(**time_dims_to_transform)

    # Step 2: Apply filters using functional composition
    for filter_spec in filters:
        filter_fn = _normalize_filter(filter_spec)
        result = result.filter(filter_fn)

    # Step 3: Group by and aggregate
    if dimensions:
        result = result.group_by(*dimensions)
        if measures:
            result = result.aggregate(*measures)
    elif measures:
        # No dimensions = grand total aggregation
        result = result.group_by().aggregate(*measures)

    # Step 4: Apply ordering using functional composition
    if order_by:
        order_keys = [_make_order_key(field, direction) for field, direction in order_by]
        result = result.order_by(*order_keys)

    # Step 5: Apply limit
    if limit:
        result = result.limit(limit)

    return result
