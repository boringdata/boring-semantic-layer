"""
Query builder for semantic API that provides a simple query interface.

Includes the Filter class for handling JSON-based filters and time grain definitions.
"""

from __future__ import annotations

from attrs import frozen
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    TYPE_CHECKING,
)
import ibis as ibis_mod
from .table import SemanticTable

Expr = ibis_mod.expr.types.core.Expr
_ = ibis_mod._

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

# Mapping of time grain identifiers to ibis truncate functions
TIME_GRAIN_TRANSFORMATIONS: Dict[str, Callable] = {
    "TIME_GRAIN_YEAR": lambda t: t.truncate("Y"),
    "TIME_GRAIN_QUARTER": lambda t: t.truncate("Q"),
    "TIME_GRAIN_MONTH": lambda t: t.truncate("M"),
    "TIME_GRAIN_WEEK": lambda t: t.truncate("W"),
    "TIME_GRAIN_DAY": lambda t: t.truncate("D"),
    "TIME_GRAIN_HOUR": lambda t: t.truncate("h"),
    "TIME_GRAIN_MINUTE": lambda t: t.truncate("m"),
    "TIME_GRAIN_SECOND": lambda t: t.truncate("s"),
}

# Order of grains from finest to coarsest for validation
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

# Mapping of operators to Ibis expressions
OPERATOR_MAPPING: Dict[str, Callable[[Expr, Any], Expr]] = {
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

@frozen(kw_only=True, slots=True)
class Filter:
    """
    Unified filter class that handles all filter types and returns an unbound ibis expression.

    Supports:
    1. JSON filter objects (simple or compound)
    2. String expressions (eval as unbound ibis expressions)
    3. Callable functions that take a table and return a boolean expression

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

        # String expression
        Filter(filter="_.dep_time.year() == 2024")

        # Callable function
        Filter(filter=lambda t: t.amount > 1000)
    """

    filter: Union[Dict[str, Any], str, Callable[[Expr], Expr]]

    OPERATORS: ClassVar[set] = set(OPERATOR_MAPPING.keys())
    COMPOUND_OPERATORS: ClassVar[set] = {"AND", "OR"}

    def __attrs_post_init__(self) -> None:
        if not isinstance(self.filter, (dict, str)) and not callable(self.filter):
            raise ValueError("Filter must be a dict, string, or callable")

    def _get_field_expr(
        self, field: str, table: Optional[Expr], model: Optional["SemanticModel"] = None
    ) -> Expr:
        if "." in field:
            table_name, field_name = field.split(".", 1)
            if model is not None and table is not None:
                if table_name not in model.joins:
                    raise KeyError(f"Unknown join alias: {table_name}")
                join = model.joins[table_name]
                if field_name not in join.model.dimensions:
                    raise KeyError(
                        f"Unknown dimension '{field_name}' in joined model '{table_name}'"
                    )
                return join.model.dimensions[field_name](join.model.table)
            # Unbound expression for table.field reference
            return getattr(getattr(_, table_name), field_name)
        # Simple field reference
        if model is not None and table is not None:
            if field not in model.dimensions:
                raise KeyError(f"Unknown dimension: {field}")
            return model.dimensions[field](table)
        # Unbound expression for field reference
        return getattr(_, field)

    def _parse_json_filter(
        self,
        filter_obj: Dict[str, Any],
        table: Optional[Expr] = None,
        model: Optional["SemanticModel"] = None,
    ) -> Expr:
        # Compound filters (AND/OR)
        if filter_obj.get("operator") in self.COMPOUND_OPERATORS:
            conditions = filter_obj.get("conditions")
            if not conditions:
                raise ValueError("Compound filter must have non-empty conditions list")
            expr = self._parse_json_filter(conditions[0], table, model)
            for cond in conditions[1:]:
                next_expr = self._parse_json_filter(cond, table, model)
                expr = OPERATOR_MAPPING[filter_obj["operator"]](expr, next_expr)
            return expr
        # Simple filter
        field = filter_obj.get("field")
        op = filter_obj.get("operator")
        if field is None or op is None:
            raise KeyError(
                "Missing required keys in filter: 'field' and 'operator' are required"
            )
        field_expr = self._get_field_expr(field, table, model)
        if op not in self.OPERATORS:
            raise ValueError(f"Unsupported operator: {op}")
        # List membership
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

    def to_ibis(self, table: Expr, model: Optional["SemanticModel"] = None) -> Expr:
        if isinstance(self.filter, dict):
            return self._parse_json_filter(self.filter, table, model)
        if isinstance(self.filter, str):
            return eval(self.filter)
        if callable(self.filter):
            return self.filter(table)
        raise ValueError("Filter must be a dict, string, or callable")


def build_query(
    semantic_table: "SemanticTable",
    dimensions: Optional[List[str]] = [],
    measures: Optional[List[str]] = None,
    filters: Optional[List[Union[Dict[str, Any], str, Callable]]] = None,
    order_by: Optional[List[Tuple[str, str]]] = None,
    limit: Optional[int] = None,
) -> "SemanticTable":
    """
    Build a SemanticTable from query parameters.

    This function provides a convenient way to query semantic tables without
    using method chaining.

    Args:
        semantic_table: The base SemanticTable to build upon
        dimensions: List of dimension names to include
        measures: List of measure names to include
        filters: List of filters (dict, str, callable, or Filter)
        order_by: List of (field, direction) tuples for ordering
        limit: Maximum number of rows to return

    Returns:
        SemanticTable: A properly configured semantic table
    """
    from .table import SemanticTable

    # semantic_table is always a SemanticTable instance
    expr = semantic_table

    # Start with the base table
    result = semantic_table

    if dimensions is None:
        dimensions = []

    # Step 1: Apply filters first (before grouping/aggregation)
    if filters:
        for filter_spec in filters:
            if isinstance(filter_spec, Filter):
                # Filter.to_ibis expects raw ibis table, ColumnScope has ._tbl attribute
                result = result.filter(
                    lambda t, f=filter_spec: f.to_ibis(t._tbl, model=None)
                )
            elif isinstance(filter_spec, dict):
                # Convert dict to Filter and apply
                filter_obj = Filter(filter=filter_spec)
                result = result.filter(
                    lambda t, f=filter_obj: f.to_ibis(t._tbl, model=None)
                )
            elif callable(filter_spec):
                # Callable filter - pass through directly
                result = result.filter(filter_spec)
            else:
                raise ValueError(f"Unsupported filter type: {type(filter_spec)}")

    # Step 2: Group by dimensions and aggregate
    if len(dimensions) > 0:
        result = result.group_by(*dimensions)

        # Add aggregation if measures are specified
        if measures:
            result = result.aggregate(*measures)
    elif measures:
        # If no dimensions but measures specified, need to aggregate without grouping
        # This produces a single row with aggregate values
        base_tbl = result._materialize_base_with_dims()

        # Build aggregations directly without grouping
        agg_dict = {}
        for m in measures:
            if m in result._base_measures:
                agg_dict[m] = result._base_measures[m](base_tbl)
            else:
                # Handle calculated measures - need to resolve dependencies
                from .compile_all import _compile_formula

                if m in result._calc_measures:
                    calc_expr = result._calc_measures[m]

                    # First aggregate all base measures needed
                    base_aggs = {
                        name: fn(base_tbl) for name, fn in result._base_measures.items()
                    }
                    base_result = base_tbl.aggregate(**base_aggs)

                    # Then compile the calculated measure
                    agg_dict[m] = _compile_formula(calc_expr, base_result, base_result)

        aggregated = base_tbl.aggregate(**agg_dict)

        # Wrap in SemanticTable
        from .table import SemanticTable

        result = SemanticTable(aggregated, name=result._name)
        result._dims = {}
        result._base_measures = {}
        result._calc_measures = {}

    # Step 5: Apply ordering
    if order_by:
        order_keys = []
        for field, direction in order_by:
            if direction.lower() == "desc":
                order_keys.append(ibis_mod.desc(field))
            else:
                order_keys.append(field)
        result = result.order_by(*order_keys)

    # Step 6: Apply limit
    if limit:
        result = result.limit(limit)

    return result
