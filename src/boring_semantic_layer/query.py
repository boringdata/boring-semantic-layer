"""
Query interface for semantic API with filter and time dimension support.

Provides parameter-based querying as an alternative to method chaining.
"""

from __future__ import annotations
from typing import Any, Callable, ClassVar, Dict, List, Literal, Optional, Tuple, Union

from attrs import frozen
import ibis

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

# Mapping of time grain identifiers to ibis truncate units
TIME_GRAIN_TRANSFORMATIONS: Dict[str, str] = {
    "TIME_GRAIN_YEAR": "Y",
    "TIME_GRAIN_QUARTER": "Q",
    "TIME_GRAIN_MONTH": "M",
    "TIME_GRAIN_WEEK": "W",
    "TIME_GRAIN_DAY": "D",
    "TIME_GRAIN_HOUR": "h",
    "TIME_GRAIN_MINUTE": "m",
    "TIME_GRAIN_SECOND": "s",
}

# Order of grains from finest to coarsest
TIME_GRAIN_ORDER = [
    "TIME_GRAIN_SECOND",
    "TIME_GRAIN_MINUTE",
    "TIME_GRAIN_HOUR",
    "TIME_GRAIN_DAY",
    "TIME_GRAIN_WEEK",
    "TIME_GRAIN_MONTH",
    "TIME_GRAIN_QUARTER",
    "TIME_GRAIN_YEAR",
]

# Operator mapping for filter expressions
OPERATOR_MAPPING: Dict[str, Callable] = {
    "=": lambda x, y: x == y,
    "eq": lambda x, y: x == y,
    "equals": lambda x, y: x == y,
    "!=": lambda x, y: x != y,
    ">": lambda x, y: x > y,
    ">=": lambda x, y: x >= y,
    "<": lambda x, y: x < y,
    "<=": lambda x, y: x <= y,
    "in": lambda x, y: x.isin(y),
    "not in": lambda x, y: ~x.isin(y),
    "like": lambda x, y: x.like(y),
    "not like": lambda x, y: ~x.like(y),
    "ilike": lambda x, y: x.ilike(y),
    "not ilike": lambda x, y: ~x.ilike(y),
    "is null": lambda x, _: x.isnull(),
    "is not null": lambda x, _: x.notnull(),
    "AND": lambda x, y: x & y,
    "OR": lambda x, y: x | y,
}


def _find_time_dimension(semantic_table: Any, dimensions: List[str]) -> Optional[str]:
    """Find the first time dimension in the query dimensions list."""
    dims_dict = semantic_table._dims_dict()
    for dim_name in dimensions:
        if dim_name in dims_dict and dims_dict[dim_name].is_time_dimension:
            return dim_name
    return None


def _validate_time_grain(
    time_grain: TimeGrain, smallest_allowed_grain: Optional[str], dimension_name: str
) -> None:
    """Validate that requested time grain is not finer than smallest allowed grain."""
    if not smallest_allowed_grain:
        return

    smallest_grain = f"TIME_GRAIN_{smallest_allowed_grain.upper()}"
    if smallest_grain not in TIME_GRAIN_ORDER:
        return

    requested_idx = TIME_GRAIN_ORDER.index(time_grain)
    smallest_idx = TIME_GRAIN_ORDER.index(smallest_grain)

    if requested_idx < smallest_idx:
        raise ValueError(
            f"Requested time grain '{time_grain}' is finer than the smallest "
            f"allowed grain '{smallest_allowed_grain}' for dimension '{dimension_name}'"
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

    filter: Union[Dict[str, Any], str, Callable]

    OPERATORS: ClassVar[set] = set(OPERATOR_MAPPING.keys())
    COMPOUND_OPERATORS: ClassVar[set] = {"AND", "OR"}

    def __attrs_post_init__(self) -> None:
        if not isinstance(self.filter, (dict, str)) and not callable(self.filter):
            raise ValueError("Filter must be a dict, string, or callable")

    def _get_field_expr(self, field: str) -> Any:
        """Get field expression using ibis._ for unbound reference."""
        if "." in field:
            table_name, field_name = field.split(".", 1)
            return getattr(getattr(ibis._, table_name), field_name)
        return getattr(ibis._, field)

    def _parse_json_filter(self, filter_obj: Dict[str, Any]) -> Any:
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
                "Missing required keys in filter: 'field' and 'operator' are required"
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
                    f"Operator '{op}' should not have 'value' or 'values' fields"
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
            # Evaluate string as ibis expression
            expr = eval(self.filter)
            return lambda t: expr.resolve(t)
        elif callable(self.filter):
            return self.filter
        raise ValueError("Filter must be a dict, string, or callable")


def query(
    semantic_table: Any,
    dimensions: Optional[List[str]] = None,
    measures: Optional[List[str]] = None,
    filters: Optional[List[Union[Dict[str, Any], str, Callable, Filter]]] = None,
    order_by: Optional[List[Tuple[str, str]]] = None,
    limit: Optional[int] = None,
    time_grain: Optional[TimeGrain] = None,
    time_range: Optional[Dict[str, str]] = None,
) -> Any:
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

    if dimensions is None:
        dimensions = []

    if filters is None:
        filters = []
    else:
        filters = list(filters)  # Copy to avoid mutating input

    # Step 0: Add time_range as a filter if specified
    if time_range:
        if (
            not isinstance(time_range, dict)
            or "start" not in time_range
            or "end" not in time_range
        ):
            raise ValueError("time_range must be a dict with 'start' and 'end' keys")

        time_dim_name = _find_time_dimension(result, dimensions)
        if time_dim_name:
            start = time_range["start"]
            end = time_range["end"]
            filters.append(
                lambda t, dim=time_dim_name, s=start, e=end: (t[dim] >= s) & (t[dim] <= e)
            )

    # Step 1: Handle time grain transformations
    if time_grain:
        if time_grain not in TIME_GRAIN_TRANSFORMATIONS:
            raise ValueError(
                f"Invalid time_grain: {time_grain}. Must be one of {list(TIME_GRAIN_TRANSFORMATIONS.keys())}"
            )

        # Find time dimensions and apply grain transformation
        time_dims_to_transform = {}
        dims_dict = result._dims_dict()
        for dim_name in dimensions:
            if dim_name in dims_dict:
                dim_obj = dims_dict[dim_name]
                if dim_obj.is_time_dimension:
                    # Validate grain
                    _validate_time_grain(
                        time_grain, dim_obj.smallest_time_grain, dim_name
                    )

                    # Create transformed dimension
                    truncate_unit = TIME_GRAIN_TRANSFORMATIONS[time_grain]
                    orig_expr = dim_obj.expr
                    time_dims_to_transform[dim_name] = Dimension(
                        expr=lambda t, orig=orig_expr, unit=truncate_unit: orig(t).truncate(unit),
                        description=dim_obj.description,
                        is_time_dimension=dim_obj.is_time_dimension,
                        smallest_time_grain=dim_obj.smallest_time_grain,
                    )

        # Apply transformations
        if time_dims_to_transform:
            result = result.with_dimensions(**time_dims_to_transform)

    # Step 2: Apply filters
    if filters:
        for filter_spec in filters:
            if isinstance(filter_spec, Filter):
                filter_fn = filter_spec.to_callable()
            elif isinstance(filter_spec, dict):
                filter_fn = Filter(filter=filter_spec).to_callable()
            elif isinstance(filter_spec, str):
                filter_fn = Filter(filter=filter_spec).to_callable()
            elif callable(filter_spec):
                filter_fn = filter_spec
            else:
                raise ValueError(f"Unsupported filter type: {type(filter_spec)}")

            result = result.filter(filter_fn)

    # Step 3: Group by and aggregate
    if dimensions:
        result = result.group_by(*dimensions)
        if measures:
            result = result.aggregate(*measures)
    elif measures:
        # No dimensions = grand total aggregation
        result = result.group_by().aggregate(*measures)

    # Step 4: Apply ordering
    if order_by:
        order_keys = []
        for field, direction in order_by:
            if direction.lower() == "desc":
                order_keys.append(ibis.desc(field))
            else:
                order_keys.append(field)
        result = result.order_by(*order_keys)

    # Step 5: Apply limit
    if limit:
        result = result.limit(limit)

    return result
