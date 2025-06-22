"""Lightweight semantic layer for BI-style queries using Xorq backend."""

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Literal, ClassVar
import pandas as pd

try:
    import xorq.vendor.ibis as ibis_mod
except ImportError:
    import ibis as ibis_mod

Expr = ibis_mod.expr.types.core.Expr
_ = ibis_mod._

Dimension = Callable[[Expr], Expr]
Measure = Callable[[Expr], Expr]

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

# Time grain transformation functions
TIME_GRAIN_TRANSFORMATIONS = {
    "TIME_GRAIN_YEAR": lambda t: t.year(),
    "TIME_GRAIN_QUARTER": lambda t: t.quarter(),
    "TIME_GRAIN_MONTH": lambda t: t.month(),
    "TIME_GRAIN_WEEK": lambda t: t.week(),
    "TIME_GRAIN_DAY": lambda t: t.date(),
    "TIME_GRAIN_HOUR": lambda t: t.hour(),
    "TIME_GRAIN_MINUTE": lambda t: t.minute(),
    "TIME_GRAIN_SECOND": lambda t: t.second(),
}

# Time grain ordering for validation
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


@dataclass
class Join:
    """Join definition for semantic model relationships."""

    alias: str
    model: "SemanticModel"
    on: Callable[[Expr, Expr], Expr]
    how: str = "inner"
    # Malloy-style join cardinality: one-to-one, one-to-many, or cross join
    kind: Optional[Literal["one", "many", "cross"]] = None


@dataclass
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

    filter: Union[Dict, str, Callable[[Expr], Expr]]

    # Class level constants
    OPERATOR_MAPPING = {
        "=": lambda x, y: x == y,
        "!=": lambda x, y: x != y,
        ">": lambda x, y: x > y,
        ">=": lambda x, y: x >= y,
        "<": lambda x, y: x < y,
        "<=": lambda x, y: x <= y,
        "in": lambda x, y: x.isin(y),
        "not in": lambda x, y: ~x.isin(y),
        "like": lambda x, y: x.like(y),
        "not like": lambda x, y: ~x.like(y),
        "is null": lambda x, _: x.isnull(),
        "is not null": lambda x, _: x.notnull(),
        "AND": lambda x, y: x & y,
        "OR": lambda x, y: x | y,
    }
    OPERATORS: ClassVar[set] = set(OPERATOR_MAPPING.keys())
    COMPOUND_OPERATORS: ClassVar[set] = {"AND", "OR"}

    def __post_init__(self):
        """Validate filter after initialization."""
        if not isinstance(self.filter, (dict, str, Callable)):
            raise ValueError("Filter must be a dict, string, or callable")

    def _get_field_expr(
        self, field: str, table: Optional[Expr], model: Optional["SemanticModel"] = None
    ) -> Expr:
        """Get field expression with proper error handling and join support."""
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
                return join.model.dimensions[field_name](table)
            else:
                # Unbound expression for table.field reference
                return getattr(getattr(_, table_name), field_name)
        else:
            if model is not None and table is not None:
                if field not in model.dimensions:
                    raise KeyError(f"Unknown dimension: {field}")
                return model.dimensions[field](table)
            else:
                # Unbound expression for field reference
                return getattr(_, field)

    def _parse_json_filter(
        self,
        filter_obj: Dict,
        table: Optional[Expr] = None,
        model: Optional["SemanticModel"] = None,
    ) -> Expr:
        """Convert a JSON filter to an Ibis expression."""
        # Handle compound filters (AND/OR)
        if (
            "operator" in filter_obj
            and filter_obj["operator"] in self.COMPOUND_OPERATORS
        ):
            if "conditions" not in filter_obj or not filter_obj["conditions"]:
                raise ValueError("Compound filter must have non-empty conditions list")

            # Process first condition
            if not filter_obj["conditions"]:
                raise ValueError("Compound filter must have at least one condition")

            result = self._parse_json_filter(filter_obj["conditions"][0], table, model)

            # Then combine with remaining conditions
            for condition in filter_obj["conditions"][1:]:
                next_expr = self._parse_json_filter(condition, table, model)
                result = self.OPERATOR_MAPPING[filter_obj["operator"]](
                    result, next_expr
                )

            return result

        # Handle simple filters
        required_keys = {"field", "operator"}
        missing_keys = required_keys - set(filter_obj.keys())
        if missing_keys:
            raise KeyError(f"Missing required keys in filter: {missing_keys}")

        # Get field expression
        field = filter_obj["field"]
        field_expr = self._get_field_expr(field, table, model)

        # Apply operator
        operator = filter_obj["operator"]
        if operator not in self.OPERATORS:
            raise ValueError(f"Unsupported operator: {operator}")

        # For 'in' and 'not in' operators, use values list
        if operator in ["in", "not in"]:
            if "values" not in filter_obj:
                raise ValueError(f"Operator '{operator}' requires 'values' field")
            return self.OPERATOR_MAPPING[operator](field_expr, filter_obj["values"])

        # For null checks, value is not needed
        elif operator in ["is null", "is not null"]:
            if any(k in filter_obj for k in ["value", "values"]):
                raise ValueError(
                    f"Operator '{operator}' should not have 'value' or 'values' fields"
                )
            return self.OPERATOR_MAPPING[operator](field_expr, None)

        # For all other operators, use the value field
        else:
            if "value" not in filter_obj:
                raise ValueError(f"Operator '{operator}' requires 'value' field")
            return self.OPERATOR_MAPPING[operator](field_expr, filter_obj["value"])

    def to_ibis(self, table: Expr, model: Optional["SemanticModel"] = None) -> Expr:
        """
        Convert the filter to an Ibis expression.

        Args:
            table: The Ibis table expression to filter
            model: Optional SemanticModel for validating field references
        """
        if isinstance(self.filter, dict):
            return self._parse_json_filter(self.filter, table, model)
        elif isinstance(self.filter, str):
            return eval(self.filter)
        elif callable(self.filter):
            return self.filter(table)
        else:
            raise ValueError("Filter must be a dict, string, or callable")


class SemanticModel:
    """
    Define a semantic model over an Ibis table expression with reusable dimensions and measures.

    Attributes:
        table: Base Ibis table expression.
        dimensions: Mapping of dimension names to callables producing column expressions.
        measures: Mapping of measure names to callables producing aggregate expressions.
        timeDimension: Optional name of the time dimension column.
        smallestTimeGrain: Optional smallest time grain for the time dimension.

    Example:
        con = xo.duckdb.connect()
        flights_tbl = con.table('flights')
        flights = SemanticModel(
            table=flights_tbl,
            dimensions={
                'origin': lambda t: t.origin,
                'destination': lambda t: t.destination,
                'carrier': lambda t: t.carrier,
            },
            measures={
                'flight_count': lambda t: t.count(),
                'avg_distance': lambda t: t.distance.mean(),
            },
            timeDimension='date',
            smallestTimeGrain='TIME_GRAIN_DAY'
        )
    """

    def __init__(
        self,
        table: Expr,
        dimensions: Dict[str, Dimension],
        measures: Dict[str, Measure],
        joins: Optional[Dict[str, Join]] = None,
        # Optional primary key name for foreign key joins
        primary_key: Optional[str] = None,
        name: Optional[str] = None,
        timeDimension: Optional[str] = None,
        smallestTimeGrain: Optional[TimeGrain] = None,
    ) -> None:
        self.name = name or table.get_name()
        self.table = table
        self.dimensions = dimensions
        self.measures = measures
        self.timeDimension = timeDimension

        # Validate smallestTimeGrain if provided
        if smallestTimeGrain is not None:
            if smallestTimeGrain not in TIME_GRAIN_TRANSFORMATIONS:
                raise ValueError(
                    f"Invalid smallestTimeGrain. Must be one of: {', '.join(TIME_GRAIN_TRANSFORMATIONS.keys())}"
                )
        self.smallestTimeGrain = smallestTimeGrain

        # Mapping of join alias to Join definitions
        self.joins: Dict[str, Join] = joins or {}
        # Optional primary key for this model (used in foreign key joins)
        self.primary_key: Optional[str] = primary_key

    def _validate_time_grain(self, time_grain: Optional[TimeGrain]) -> None:
        """Validate that the requested time grain is not finer than the smallest allowed grain."""
        if time_grain is None or self.smallestTimeGrain is None:
            return

        requested_idx = TIME_GRAIN_ORDER.index(time_grain)
        smallest_idx = TIME_GRAIN_ORDER.index(self.smallestTimeGrain)

        if requested_idx < smallest_idx:
            raise ValueError(
                f"Requested time grain '{time_grain}' is finer than the smallest allowed grain '{self.smallestTimeGrain}'"
            )

    def _transform_time_dimension(
        self, table: Expr, time_grain: Optional[TimeGrain]
    ) -> Tuple[Expr, Dict[str, Dimension]]:
        """Transform the time dimension based on the specified grain."""
        if not self.timeDimension or not time_grain:
            return table, self.dimensions.copy()

        # Create a copy of dimensions
        dimensions = self.dimensions.copy()

        # Get or create the time dimension function
        if self.timeDimension in dimensions:
            time_dim_func = dimensions[self.timeDimension]
        else:
            # Create a default time dimension function that accesses the column directly
            def time_dim_func(t: Expr) -> Expr:
                return getattr(t, self.timeDimension)

            dimensions[self.timeDimension] = time_dim_func

        # Create the transformed dimension function
        transform_func = TIME_GRAIN_TRANSFORMATIONS[time_grain]
        dimensions[self.timeDimension] = lambda t: transform_func(time_dim_func(t))

        return table, dimensions

    def query(
        self,
        dims: Optional[List[str]] = None,
        measures: Optional[List[str]] = None,
        filters: Optional[
            List[Union[Dict[str, Any], str, Callable[[Expr], Expr]]]
        ] = None,
        order_by: Optional[List[Tuple[str, str]]] = None,
        limit: Optional[int] = None,
        time_range: Optional[Dict[str, str]] = None,
        time_grain: Optional[TimeGrain] = None,
    ) -> Expr:
        """
        Build an Ibis expression that groups by dimensions and aggregates measures.

        Args:
            dims: List of dimension keys to group by.
            measures: List of measure keys to compute.
            filters: List of filters that can be:
                - Dictionary defining a filter in JSON format with the following structure:

                  Simple Filter:
                  {
                      "field": "column_name",     # Can include table references like "table.column"
                      "operator": "=",            # One of: =, !=, >, >=, <, <=, in, not in, like, not like, is null, is not null
                      "value": "value"            # For non-'in' operators
                      # OR
                      "values": ["val1", "val2"]  # For 'in' operator only
                  }

                  Compound Filter (AND/OR):
                  {
                      "operator": "AND",          # or "OR"
                      "conditions": [             # Non-empty list of other filter objects
                          {
                              "field": "country",
                              "operator": "=",
                              "value": "US"
                          },
                          {
                              "field": "tier",
                              "operator": "in",
                              "values": ["gold", "platinum"]
                          }
                      ]
                  }
                - String that can be evaluated to an ibis expression
                - Callable that takes a table expression and returns a boolean expression
            order_by: List of tuples (field, 'asc'|'desc') for ordering.
            limit: Row limit.
            time_range: Optional time range filter for the time dimension, with format:
                {
                    "start": "2008-01-01T00:00:00Z",  # ISO 8601 format
                    "end": "2025-12-31T23:59:59Z"     # ISO 8601 format
                }
            time_grain: Optional time grain to use for the time dimension.
                Must be one of: TIME_GRAIN_YEAR, TIME_GRAIN_QUARTER, TIME_GRAIN_MONTH,
                TIME_GRAIN_WEEK, TIME_GRAIN_DAY, TIME_GRAIN_HOUR, TIME_GRAIN_MINUTE,
                TIME_GRAIN_SECOND. Cannot be finer than smallestTimeGrain.

        Returns:
            Ibis Expr representing the query.
        """
        # Validate time grain if specified
        self._validate_time_grain(time_grain)

        t = self.table

        # Apply defined joins
        for alias, join in self.joins.items():
            right = join.model.table
            # Support cross joins separately
            if join.how == "cross":
                t = t.cross_join(right)
            else:
                cond = join.on(t, right)
                t = t.join(right, cond, how=join.how)

        # Transform time dimension if needed
        t, dimensions = self._transform_time_dimension(t, time_grain)

        # Apply time range filter if specified and time dimension exists
        if time_range and self.timeDimension:
            if (
                not isinstance(time_range, dict)
                or "start" not in time_range
                or "end" not in time_range
            ):
                raise ValueError(
                    "time_range must be a dictionary with 'start' and 'end' keys"
                )

            time_filter = {
                "operator": "AND",
                "conditions": [
                    {
                        "field": self.timeDimension,
                        "operator": ">=",
                        "value": time_range["start"],
                    },
                    {
                        "field": self.timeDimension,
                        "operator": "<=",
                        "value": time_range["end"],
                    },
                ],
            }
            if not filters:
                filters = [time_filter]
            else:
                if not isinstance(filters, list):
                    filters = [filters]
                filters.append(time_filter)

        # Apply filters
        if filters:
            if not isinstance(filters, list):
                filters = [filters]

            for filter_ in filters:
                # Convert filter to Filter object
                filter_obj = Filter(filter=filter_)
                t = t.filter(filter_obj.to_ibis(t, self))

        dims = dims or []

        # If time_grain is specified and timeDimension exists, automatically include it in dimensions
        if time_grain and self.timeDimension and self.timeDimension not in dims:
            dims.append(self.timeDimension)

        measures = measures or []

        # Validate keys (dimensions and measures), including joins
        for d in dims:
            if isinstance(d, str) and "." in d:
                alias, field = d.split(".", 1)
                join = self.joins.get(alias)
                if not join or field not in join.model.dimensions:
                    raise KeyError(f"Unknown dimension: {d}")
            elif d not in dimensions:
                raise KeyError(f"Unknown dimension: {d}")

        for m in measures:
            if isinstance(m, str) and "." in m:
                alias, field = m.split(".", 1)
                join = self.joins.get(alias)
                if not join or field not in join.model.measures:
                    raise KeyError(f"Unknown measure: {m}")
            elif m not in self.measures:
                raise KeyError(f"Unknown measure: {m}")

        # Build aggregate expressions, including join measures
        agg_kwargs: Dict[str, Expr] = {}
        for m in measures:
            if isinstance(m, str) and "." in m:
                alias, field = m.split(".", 1)
                join = self.joins[alias]
                expr = join.model.measures[field](t)
                name = f"{alias}_{field}"
                agg_kwargs[name] = expr.name(name)
            else:
                expr = self.measures[m](t)
                agg_kwargs[m] = expr.name(m)

        # Grouping and aggregation
        if dims:
            # Name and prepare dimension expressions
            dim_exprs = []
            for d in dims:
                if isinstance(d, str) and "." in d:
                    alias, field = d.split(".", 1)
                    join = self.joins[alias]
                    name = f"{alias}_{field}"
                    expr = join.model.dimensions[field](t).name(name)
                else:
                    expr = dimensions[d](t).name(d)
                dim_exprs.append(expr)
            grouped = t.group_by(*dim_exprs)
            result = grouped.aggregate(**agg_kwargs)
        else:
            result = t.aggregate(**agg_kwargs)

        # Ordering
        if order_by:
            order_exprs = []
            for field, direction in order_by:
                if isinstance(field, str) and "." in field:
                    alias, fname = field.split(".", 1)
                    col_name = f"{alias}_{fname}"
                else:
                    col_name = field
                col = result[col_name]
                order_exprs.append(
                    col.desc() if direction.lower().startswith("desc") else col.asc()
                )
            result = result.order_by(order_exprs)

        # Limit
        if limit is not None:
            result = result.limit(limit)

        return result

    def get_time_range(self) -> Dict[str, Any]:
        """Get the available time range for the model's time dimension.

        Returns:
            A dictionary with 'start' and 'end' dates in ISO format, or an error if no time dimension
        """
        if not self.timeDimension:
            return {"error": "Model does not have a time dimension"}

        # Get the original time dimension function
        time_dim_func = self.dimensions[self.timeDimension]

        # Query the min and max dates
        time_range = self.table.aggregate(
            start=time_dim_func(self.table).min(), end=time_dim_func(self.table).max()
        ).execute()

        # Convert to ISO format if not None
        # Access the first (and only) row's values directly
        start_date = (
            pd.Timestamp(time_range["start"].iloc[0]).isoformat()
            if pd.notna(time_range["start"].iloc[0])
            else None
        )
        end_date = (
            pd.Timestamp(time_range["end"].iloc[0]).isoformat()
            if pd.notna(time_range["end"].iloc[0])
            else None
        )

        return {"start": start_date, "end": end_date}

    @property
    def available_dimensions(self) -> List[str]:
        """List available dimension keys, including joined model dimensions."""
        keys = list(self.dimensions.keys())
        # Include time dimension if it exists and is not already in dimensions
        if self.timeDimension and self.timeDimension not in keys:
            keys.append(self.timeDimension)
        for alias, join in self.joins.items():
            keys.extend([f"{alias}.{d}" for d in join.model.dimensions.keys()])
        return keys

    @property
    def available_measures(self) -> List[str]:
        """List available measure keys, including joined model measures."""
        keys = list(self.measures.keys())
        for alias, join in self.joins.items():
            keys.extend([f"{alias}.{m}" for m in join.model.measures.keys()])
        return keys

    @property
    def json_definition(self) -> Dict[str, Any]:
        """Return model metadata including name, dimensions, measures, time dimension, and time grain."""
        definition = {
            "name": self.name,
            "dimensions": self.available_dimensions,
            "measures": self.available_measures,
        }

        # Add time dimension info if present
        if self.timeDimension:
            definition["timeDimension"] = self.timeDimension

        # Add smallest time grain if present
        if self.smallestTimeGrain:
            definition["smallestTimeGrain"] = self.smallestTimeGrain

        return definition


# functions for Malloy-style joins
def join_one(
    alias: str,
    model: SemanticModel,
    with_: Optional[Callable[[Expr], Expr]] = None,
) -> Join:
    if with_ is None:
        raise ValueError("join_one requires a 'with_' callable for foreign key mapping")
    if not callable(with_):
        raise TypeError(
            "'with_' must be a callable mapping the left table to a column expression"
        )
    if not model.primary_key:
        raise ValueError(f"Model does not have 'primary_key' defined for join: {alias}")

    def on_expr(left, right):
        return with_(left) == getattr(right, model.primary_key)

    return Join(alias=alias, model=model, on=on_expr, how="left", kind="one")


def join_many(
    alias: str,
    model: SemanticModel,
    with_: Optional[Callable[[Expr], Expr]] = None,
) -> Join:
    if with_ is None:
        raise ValueError(
            "join_many requires a 'with_' callable for foreign key mapping"
        )
    if not callable(with_):
        raise TypeError(
            "'with_' must be a callable mapping the left table to a column expression"
        )
    if not model.primary_key:
        raise ValueError(f"Model does not have 'primary_key' defined for join: {alias}")

    def on_expr(left, right):
        return with_(left) == getattr(right, model.primary_key)

    return Join(alias=alias, model=model, on=on_expr, how="left", kind="many")


def join_cross(alias: str, model: SemanticModel) -> Join:
    return Join(
        alias=alias, model=model, on=lambda left, right: None, how="cross", kind="cross"
    )
