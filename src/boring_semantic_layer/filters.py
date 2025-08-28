"""
Filter module for Boring Semantic Layer.
"""

from attrs import frozen
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Optional,
    TYPE_CHECKING,
    Union,
)

try:
    import xorq.vendor.ibis as ibis_mod
except ImportError:
    import ibis as ibis_mod

Expr = ibis_mod.expr.types.core.Expr
_ = ibis_mod._

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

if TYPE_CHECKING:
    from .semantic_model import SemanticModel


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

        # JSON compound filter with table reference
        Filter(filter={
            "operator": "AND",
            "conditions": [
                {"field": "orders.country", "operator": "=", "value": "US"},
                {"field": "customers.tier", "operator": "in", "values": ["gold", "platinum"]}
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
